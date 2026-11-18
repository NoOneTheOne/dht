package dht

import (
	"container/list"
	"net"
	"strings"
	"sync"
	"time"
)

// BEP-51 DHT Infohash Indexing: sample_infohashes asks a node for a sample
// of the infohashes it stores peers for. Because the sampled node holds the
// peers, it is also the best first hop for a get_peers lookup of each hash.
// https://www.bittorrent.org/beps/bep_0051.html

const (
	sampleInfohashesType = "sample_infohashes"
	// methodUnknown is the BEP-5 error code for unsupported queries.
	methodUnknown = 204

	maxSampleInterval    = 6 * time.Hour // BEP-51 caps interval at 21600s.
	sampleUnansweredWait = time.Hour     // Unsupported or silent nodes.
	sampleMinWait        = time.Minute
	sampleNodesPerRound  = 4
	maxSampleSchedule    = 1 << 16
	maxLookupHints       = 1 << 14
)

// boundedSchedule maps keys to times, evicting the oldest insertion at capacity.
type boundedSchedule struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List
}

type scheduleEntry struct {
	key   string
	when  time.Time
	value *node
}

func newBoundedSchedule(capacity int) *boundedSchedule {
	return &boundedSchedule{capacity: capacity, items: make(map[string]*list.Element), order: list.New()}
}

func (s *boundedSchedule) set(key string, when time.Time, value *node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.items[key]; e != nil {
		e.Value = scheduleEntry{key: key, when: when, value: value}
		s.order.MoveToBack(e)
		return
	}
	if len(s.items) >= s.capacity {
		oldest := s.order.Front()
		delete(s.items, oldest.Value.(scheduleEntry).key)
		s.order.Remove(oldest)
	}
	s.items[key] = s.order.PushBack(scheduleEntry{key: key, when: when, value: value})
}

func (s *boundedSchedule) get(key string) (scheduleEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.items[key]; e != nil {
		return e.Value.(scheduleEntry), true
	}
	return scheduleEntry{}, false
}

// take removes and returns the entry for key.
func (s *boundedSchedule) take(key string) (scheduleEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.items[key]
	if e == nil {
		return scheduleEntry{}, false
	}
	delete(s.items, key)
	s.order.Remove(e)
	return e.Value.(scheduleEntry), true
}

// sampleDue reports whether addr may be asked for samples at now.
func (dht *DHT) sampleDue(addr string, now time.Time) bool {
	entry, ok := dht.sampleSchedule.get(addr)
	return !ok || !now.Before(entry.when)
}

// sampleRound asks a few random routing-table nodes for infohash samples.
// It yields to metadata lookups whenever the query queue is busy.
func (dht *DHT) sampleRound() {
	tm := dht.transactionManager
	if len(tm.queryChan) > cap(tm.queryChan)/4 {
		RecordProtocolEvent("sample_infohashes", "deferred_busy")
		return
	}
	now := time.Now()
	target := randomString(20)
	sent := 0
	for _, no := range dht.routingTable.GetNeighbors(newBitmapFromString(target), 8*sampleNodesPerRound) {
		if sent == sampleNodesPerRound {
			break
		}
		addr := no.addr.String()
		if !dht.sampleDue(addr, now) {
			continue
		}
		// Assume no answer; a response replaces this with the node's interval.
		dht.sampleSchedule.set(addr, now.Add(sampleUnansweredWait), nil)
		if tm.sendQuery(no, sampleInfohashesType, map[string]interface{}{
			"id": dht.id(target), "target": target,
		}) {
			sent++
			RecordProtocolEvent("sample_infohashes", "sent")
		}
	}
}

// onSampleResponse handles a BEP-51 response from responder at addr. It
// reports false for malformed responses.
func (dht *DHT) onSampleResponse(addr *net.UDPAddr, responder *node, r map[string]interface{}) bool {
	samples, ok := r["samples"].(string)
	if !ok || len(samples)%20 != 0 {
		return false
	}
	interval := sampleMinWait
	if value, ok := r["interval"].(int); ok {
		interval = min(max(time.Duration(value)*time.Second, sampleMinWait), maxSampleInterval)
	}
	dht.sampleSchedule.set(addr.String(), time.Now().Add(interval), nil)

	if nodes, ok := r["nodes"].(string); ok && len(nodes)%26 == 0 {
		for offset := 0; offset < len(nodes); offset += 26 {
			if n, err := newNodeFromCompactInfo(nodes[offset:offset+26], dht.Network); err == nil {
				if _, _, public := verifiedNodeEndpoint(n.addr.String()); public {
					dht.routingTable.InsertMentioned(n)
				}
			}
		}
	}

	hashes := make([]string, 0, len(samples)/20)
	for offset := 0; offset < len(samples); offset += 20 {
		hash := strings.Clone(samples[offset : offset+20])
		hashes = append(hashes, hash)
		// The responder stores peers for every sample: start lookups there.
		dht.lookupHints.set(hash, time.Time{}, responder)
	}
	if len(hashes) == 0 {
		RecordProtocolEvent("sample_infohashes", "empty")
	} else {
		RecordProtocolEvent("sample_infohashes", "samples")
	}
	if dht.OnSampleInfohashes != nil && len(hashes) > 0 {
		dht.OnSampleInfohashes(hashes)
	}
	return true
}

// sampleReply answers a BEP-51 query. A crawler stores no peers, so it
// returns no samples, closest nodes, and the longest allowed interval.
func (dht *DHT) sampleReply(target string) map[string]interface{} {
	return map[string]interface{}{
		"id":       dht.id(target),
		"interval": int(maxSampleInterval / time.Second),
		"num":      0,
		"samples":  "",
		"nodes": strings.Join(dht.routingTable.GetNeighborCompactInfos(
			newBitmapFromString(target), dht.K), ""),
	}
}

// lookupHint returns, and forgets, the node that sampled hash.
func (dht *DHT) lookupHint(hash string) *node {
	if dht.lookupHints == nil {
		return nil
	}
	entry, ok := dht.lookupHints.take(hash)
	if !ok {
		return nil
	}
	return entry.value
}
