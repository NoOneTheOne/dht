package dht

import (
	"sync"
	"time"
)

const (
	maxPeerLookups       = 128
	peerLookupAlpha      = 3
	peerLookupBudget     = 24
	peerLookupCandidates = 64
	peerLookupTimeout    = 45 * time.Second
)

type peerLookup struct {
	mu         sync.Mutex
	dht        *DHT
	hash       string
	deadline   time.Time
	candidates []*node
	visited    map[string]bool
	active     int
	attempted  int
}

func (d *DHT) startPeerLookup(hash string, seeds []*node) error {
	d.lookupMu.Lock()
	if d.lookups == nil {
		d.lookups = make(map[string]*peerLookup)
	}
	if existing := d.lookups[hash]; existing != nil {
		d.lookupMu.Unlock()
		return ErrQueryNotQueued
	}
	if len(d.lookups) >= maxPeerLookups {
		d.lookupMu.Unlock()
		return ErrQueryNotQueued
	}
	lookup := &peerLookup{dht: d, hash: hash, deadline: time.Now().Add(peerLookupTimeout), visited: make(map[string]bool)}
	d.lookups[hash] = lookup
	d.lookupMu.Unlock()
	lookup.mu.Lock()
	lookup.addCandidates(seeds)
	queued := lookup.advance()
	lookup.mu.Unlock()
	if !queued {
		return ErrQueryNotQueued
	}
	RecordProtocolEvent("peer_lookup", "started")
	return nil
}

func (l *peerLookup) addCandidates(nodes []*node) {
	for _, n := range nodes {
		if n == nil || n.id == nil || n.addr == nil || n.addr.Port < 1 || n.addr.Port > 65535 ||
			n.id.RawString() == l.dht.node.id.RawString() || n.addr.IP.IsUnspecified() ||
			n.addr.IP.IsMulticast() || n.addr.IP.IsLinkLocalUnicast() ||
			l.dht.blackList.in(n.addr.IP.String(), n.addr.Port) || l.visited[n.addr.String()] {
			continue
		}
		if _, _, ok := verifiedNodeEndpoint(n.addr.String()); !ok {
			continue
		}
		exists := false
		for _, candidate := range l.candidates {
			if candidate.addr.String() == n.addr.String() {
				exists = true
				break
			}
		}
		if !exists {
			l.candidates = append(l.candidates, n)
		}
	}
	l.candidates = getTopKNodes(l.candidates, newBitmapFromString(l.hash), peerLookupCandidates)
}

func (l *peerLookup) advance() bool {
	queued := false
	for len(l.candidates) > 0 && l.active < peerLookupAlpha && l.attempted < peerLookupBudget && time.Now().Before(l.deadline) {
		n := l.candidates[0]
		l.candidates = l.candidates[1:]
		l.visited[n.addr.String()] = true
		if l.dht.transactionManager.queueQuery(n, getPeersType, map[string]interface{}{
			"id": l.dht.id(l.hash), "info_hash": l.hash,
		}, l) {
			l.active++
			l.attempted++
			queued = true
		}
	}
	if l.active == 0 {
		l.dht.lookupMu.Lock()
		if l.dht.lookups[l.hash] == l {
			delete(l.dht.lookups, l.hash)
		}
		l.dht.lookupMu.Unlock()
		RecordProtocolEvent("peer_lookup", "finished")
	}
	return queued
}

func (l *peerLookup) complete(q *query) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active--
	l.addCandidates(q.resultNodes)
	l.advance()
}
