package main

import (
	"container/list"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/NoOneTheOne/dht/internal/dht"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

const (
	maxPendingHashes = 8192
	maxCachedHashes  = 4096
	maxHashPeers     = 8
	maxHashAttempts  = 6
	maxHashLookups   = 3
	hashLifetime     = 5 * time.Minute
	successCacheTTL  = 30 * time.Minute
)

type hashDiscovery struct {
	hash   string
	peer   *dht.Request
	source string
}

type pendingHash struct {
	hash        string
	created     time.Time
	nextLookup  time.Time
	nextAttempt time.Time
	lookups     int
	attempts    int
	active      bool
	peers       []dht.Request
	tried       map[string]bool
}

type cachedHash struct {
	hash    string
	expires time.Time
	item    *mysql.Data
}

type hashScheduler struct {
	discoveries chan hashDiscovery
	stop        chan struct{}
	pending     map[string]*list.Element
	queue       *list.List
	cached      map[string]*list.Element
	cacheQueue  *list.List
	lookup      func(string) error
	request     func([]byte, string, int) bool
	inFlight    int
	maxInFlight int
}

func newHashScheduler(workers int, lookup func(string) error, request func([]byte, string, int) bool) *hashScheduler {
	return &hashScheduler{
		discoveries: make(chan hashDiscovery, 4096), stop: make(chan struct{}),
		pending: make(map[string]*list.Element), queue: list.New(),
		cached: make(map[string]*list.Element), cacheQueue: list.New(),
		lookup: lookup, request: request, maxInFlight: workers * 2,
	}
}

func (s *hashScheduler) discover(hash, ip string, port int, source string) {
	if len(hash) != 20 {
		return
	}
	var peer *dht.Request
	// get_peers requests and BEP-51 samples name a hash but no peer.
	if source != "get_peers" && source != "sample" {
		if !dht.IsPublicPeerIP(ip) || port < 1 || port > 65535 {
			return
		}
		peer = &dht.Request{InfoHash: []byte(hash), IP: ip, Port: port}
	}
	dht.RecordTorrentDiscovered()
	dht.RecordProtocolEvent("hash_source", source)
	select {
	case <-s.stop:
		return
	default:
	}
	select {
	case s.discoveries <- hashDiscovery{hash: hash, peer: peer, source: source}:
	default:
		dht.RecordProtocolEvent("hash_queue", "full")
	}
}

func peerKey(peer dht.Request) string { return peer.IP + ":" + strconv.Itoa(peer.Port) }

func (s *hashScheduler) add(discovery hashDiscovery, now time.Time) *mysql.Data {
	if element := s.cached[discovery.hash]; element != nil {
		cached := element.Value.(*cachedHash)
		if now.Before(cached.expires) {
			dht.RecordProtocolEvent("hash_queue", "cached")
			// Lookup and BEP-51 sample responses answer our own queries; they
			// are not new network observations and must not inflate trending.
			if cached.item != nil && discovery.source != "lookup" && discovery.source != "sample" {
				item := *cached.item
				item.Time = now
				return &item
			}
			return nil
		}
		delete(s.cached, cached.hash)
		s.cacheQueue.Remove(element)
	}
	element := s.pending[discovery.hash]
	if element == nil {
		if len(s.pending) >= maxPendingHashes {
			dht.RecordProtocolEvent("hash_queue", "capacity")
			return nil
		}
		entry := &pendingHash{hash: discovery.hash, created: now, tried: make(map[string]bool)}
		element = s.queue.PushBack(entry)
		s.pending[discovery.hash] = element
		dht.RecordProtocolEvent("hash_queue", "new")
	}
	entry := element.Value.(*pendingHash)
	if discovery.peer != nil && len(entry.peers) < maxHashPeers && !entry.tried[peerKey(*discovery.peer)] {
		for _, peer := range entry.peers {
			if peerKey(peer) == peerKey(*discovery.peer) {
				return nil
			}
		}
		entry.peers = append(entry.peers, *discovery.peer)
	}
	return nil
}

func (s *hashScheduler) tick(now time.Time) {
	lookups, downloads := 8, 32
	for scanned, count := 0, min(s.queue.Len(), 256); scanned < count; scanned++ {
		element := s.queue.Front()
		entry := element.Value.(*pendingHash)
		s.queue.MoveToBack(element)
		if entry.active {
			continue
		}
		if now.Sub(entry.created) >= hashLifetime || entry.attempts >= maxHashAttempts ||
			len(entry.peers) == 0 && entry.lookups >= maxHashLookups && !now.Before(entry.nextLookup) {
			s.complete(entry.hash, nil, now)
			s.cached[entry.hash].Value.(*cachedHash).expires = now.Add(time.Minute)
			dht.RecordProtocolEvent("hash_queue", "exhausted")
			continue
		}
		if downloads > 0 && s.inFlight < s.maxInFlight && len(entry.peers) > 0 && !now.Before(entry.nextAttempt) {
			peer := entry.peers[0]
			entry.peers = entry.peers[1:]
			entry.tried[peerKey(peer)] = true
			downloads--
			entry.attempts++
			if s.request(peer.InfoHash, peer.IP, peer.Port) {
				entry.active = true
				s.inFlight++
			}
			continue
		}
		if lookups > 0 && !entry.active && len(entry.peers) == 0 && entry.lookups < maxHashLookups && !now.Before(entry.nextLookup) {
			lookups--
			if err := s.lookup(entry.hash); err != nil {
				entry.nextLookup = now.Add(5 * time.Second)
				reason := "not_ready"
				switch {
				case errors.Is(err, dht.ErrLookupActive):
					// Still running: not a failure, and it must not cost a slot.
					lookups++
					reason = "still_running"
				case errors.Is(err, dht.ErrLookupCapacity):
					// Every further lookup this tick would be refused too.
					lookups = 0
					reason = "capacity"
				case errors.Is(err, dht.ErrQueryNotQueued):
					reason = "not_queued"
				}
				dht.RecordProtocolEvent("hash_lookup", reason)
				continue
			}
			entry.nextLookup = now.Add(10 * time.Second << entry.lookups)
			entry.lookups++
			dht.RecordProtocolEvent("hash_lookup", "requested")
		}
	}
}

func (s *hashScheduler) failed(hash string, now time.Time) {
	if element := s.pending[hash]; element != nil {
		entry := element.Value.(*pendingHash)
		if entry.active {
			entry.active = false
			s.inFlight--
		}
		entry.nextAttempt = now.Add(2 * time.Second)
	}
}

func (s *hashScheduler) complete(hash string, item *mysql.Data, now time.Time) {
	if element := s.pending[hash]; element != nil {
		if element.Value.(*pendingHash).active {
			s.inFlight--
		}
		delete(s.pending, hash)
		s.queue.Remove(element)
	}
	if element := s.cached[hash]; element != nil {
		s.cacheQueue.Remove(element)
		delete(s.cached, hash)
	}
	if len(s.cached) >= maxCachedHashes {
		oldest := s.cacheQueue.Front()
		delete(s.cached, oldest.Value.(*cachedHash).hash)
		s.cacheQueue.Remove(oldest)
	}
	var summary *mysql.Data
	if item != nil {
		// Keep names and sizes, never cache potentially large file lists.
		name := strings.ToValidUTF8(item.Name, "")
		characters := 0
		for offset := range name {
			if characters == 512 {
				name = name[:offset]
				break
			}
			characters++
		}
		summary = &mysql.Data{Name: strings.Clone(name), InfoHash: item.InfoHash, Length: item.Length}
	}
	s.cached[hash] = s.cacheQueue.PushBack(&cachedHash{hash: hash, expires: now.Add(successCacheTTL), item: summary})
}

func (s *hashScheduler) run(responses <-chan dht.Response, failures <-chan dht.MetadataFailure, output chan<- mysql.Data) {
	defer close(output)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	discoveries, stop, ticks := s.discoveries, s.stop, ticker.C
	for responses != nil || failures != nil {
		select {
		case <-stop:
			discoveries, stop, ticks = nil, nil, nil
			ticker.Stop()
		case discovery := <-discoveries:
			if item := s.add(discovery, time.Now()); item != nil {
				output <- *item
				dht.RecordProtocolEvent("hash_observation", "cached")
			}
		case now := <-ticks:
			s.tick(now)
		case failure, ok := <-failures:
			if !ok {
				failures = nil
				continue
			}
			s.failed(string(failure.InfoHash), time.Now())
		case response, ok := <-responses:
			if !ok {
				responses = nil
				continue
			}
			hash := string(response.InfoHash)
			metadata, err := dht.Decode(response.MetadataInfo)
			if err != nil {
				dht.RecordProtocolEvent("metadata_decode", "decode_error")
				s.complete(hash, nil, time.Now())
				continue
			}
			info, ok := metadata.(map[string]interface{})
			if !ok {
				dht.RecordProtocolEvent("metadata_decode", "invalid_info")
				s.complete(hash, nil, time.Now())
				continue
			}
			item, ok := torrentMetadata(info)
			if !ok {
				s.complete(hash, nil, time.Now())
				continue
			}
			item.InfoHash, item.Time = hex.EncodeToString(response.InfoHash), time.Now()
			output <- item
			s.complete(hash, &item, item.Time)
			dht.RecordProtocolEvent("hash_observation", "downloaded")
		}
	}
}
