package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/dht"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

func TestHashDiscoveryDoesNotDialRequester(t *testing.T) {
	s := newHashScheduler(2, func(string) error { return nil }, func([]byte, string, int) bool {
		t.Fatal("get_peers requester must not be used as a resource peer")
		return false
	})
	hash := strings.Repeat("h", 20)
	s.discover(hash, "8.8.8.8", 6881, "get_peers")
	discovery := <-s.discoveries
	if discovery.peer != nil || discovery.hash != hash {
		t.Fatal("requester endpoint entered download queue")
	}
	s.add(discovery, time.Now())
	s.tick(time.Now())
	for _, source := range []string{"announce_peer", "lookup"} {
		for _, ip := range []string{"127.0.0.1", "192.168.1.2", "invalid"} {
			s.discover(hash, ip, 6881, source)
		}
		s.discover(hash, "8.8.8.8", 0, source)
	}
	if len(s.discoveries) != 0 {
		t.Fatal("invalid resource endpoint accepted")
	}
	close(s.stop)
	s.discover(hash, "", 0, "get_peers")
	if len(s.discoveries) != 0 {
		t.Fatal("stopped scheduler accepted discovery")
	}
}

func TestHashSchedulerLookupBackoff(t *testing.T) {
	now := time.Now()
	calls := 0
	ready := false
	s := newHashScheduler(1, func(string) error {
		calls++
		if !ready {
			return dht.ErrNotReady
		}
		return nil
	}, func([]byte, string, int) bool { return true })
	hash := strings.Repeat("h", 20)
	s.add(hashDiscovery{hash: hash}, now)
	s.tick(now)
	entry := s.pending[hash].Value.(*pendingHash)
	if entry.lookups != 0 || calls != 1 {
		t.Fatal("not-ready lookup consumed retry budget")
	}
	ready = true
	s.tick(now.Add(time.Second))
	if calls != 1 {
		t.Fatal("not-ready backoff ignored")
	}
	s.tick(now.Add(5 * time.Second))
	s.tick(now.Add(6 * time.Second))
	if calls != 2 || entry.lookups != 1 {
		t.Fatal("lookup was repeated without backoff")
	}
	s.tick(now.Add(15 * time.Second))
	s.tick(now.Add(35 * time.Second))
	s.tick(now.Add(75 * time.Second))
	if calls != 4 || len(s.pending) != 0 {
		t.Fatalf("lookup retry budget unbounded: calls=%d pending=%d", calls, len(s.pending))
	}
}

func TestHashSchedulerRotatesPeersAndCachesSuccess(t *testing.T) {
	now := time.Now()
	var requests []string
	s := newHashScheduler(1, func(string) error { return nil }, func(_ []byte, ip string, _ int) bool {
		requests = append(requests, ip)
		return true
	})
	hash := strings.Repeat("h", 20)
	for _, ip := range []string{"8.8.8.8", "8.8.8.8", "1.1.1.1"} {
		peer := &dht.Request{InfoHash: []byte(hash), IP: ip, Port: 6881}
		s.add(hashDiscovery{hash: hash, peer: peer, source: "announce_peer"}, now)
	}
	s.tick(now)
	s.tick(now.Add(time.Second))
	if len(requests) != 1 || s.inFlight != 1 {
		t.Fatal("same hash has multiple active downloads")
	}
	s.failed(hash, now)
	s.tick(now.Add(time.Second))
	if len(requests) != 1 {
		t.Fatal("peer retry ignored backoff")
	}
	s.tick(now.Add(2 * time.Second))
	if len(requests) != 2 || requests[1] != "1.1.1.1" {
		t.Fatal("did not rotate to another peer")
	}
	item := mysql.Data{InfoHash: strings.Repeat("a", 40), Name: "resource", Length: 7, Files: []mysql.File{{Path: []string{"file"}, Length: 7}}}
	s.complete(hash, &item, now)
	if s.inFlight != 0 || len(s.pending) != 0 {
		t.Fatal("completion did not release task")
	}
	observation := s.add(hashDiscovery{hash: hash, source: "get_peers"}, now.Add(time.Second))
	if observation == nil || observation.Name != item.Name || observation.InfoHash != item.InfoHash || observation.Time == item.Time || len(observation.Files) != 0 {
		t.Fatalf("incorrect cached observation: %+v", observation)
	}
	if s.add(hashDiscovery{hash: hash, source: "lookup"}, now.Add(time.Second)) != nil {
		t.Fatal("self-generated lookup inflated observations")
	}
	s.tick(now.Add(time.Minute))
	if len(requests) != 2 {
		t.Fatal("success was redownloaded within TTL")
	}
	s.add(hashDiscovery{hash: hash, source: "get_peers"}, now.Add(successCacheTTL))
	if len(s.pending) != 1 || len(s.cached) != 0 {
		t.Fatal("cache expiration did not allow fresh collection")
	}
}

func TestHashSchedulerBounds(t *testing.T) {
	now := time.Now()
	s := newHashScheduler(1, func(string) error { return nil }, func([]byte, string, int) bool { return true })
	for i := 0; i < maxPendingHashes+10; i++ {
		s.add(hashDiscovery{hash: fmt.Sprintf("%020d", i)}, now)
	}
	if len(s.pending) != maxPendingHashes || s.queue.Len() != maxPendingHashes {
		t.Fatal("unbounded pending hashes")
	}
	for i := 0; i < maxCachedHashes+10; i++ {
		s.complete(fmt.Sprintf("%020d", i), nil, now)
	}
	if len(s.cached) != maxCachedHashes || s.cacheQueue.Len() != maxCachedHashes {
		t.Fatal("unbounded success cache")
	}
	s.complete("existing", nil, now)
	for i := 0; i < 10; i++ {
		s.complete("existing", nil, now)
	}
	if len(s.cached) != s.cacheQueue.Len() {
		t.Fatal("cache replacement leaked entries")
	}
	for i := 0; i < maxPendingHashes/256+1; i++ {
		s.tick(now.Add(hashLifetime))
	}
	if len(s.pending) != 0 {
		t.Fatal("expired pending tasks were retained")
	}
}

func TestHashSchedulerDownloadBudget(t *testing.T) {
	now := time.Now()
	requests := 0
	s := newHashScheduler(1, func(string) error { return nil }, func([]byte, string, int) bool { requests++; return true })
	for i := 0; i < 10; i++ {
		hash := fmt.Sprintf("%020d", i)
		s.add(hashDiscovery{hash: hash, peer: &dht.Request{InfoHash: []byte(hash), IP: "8.8.8.8", Port: 6881}}, now)
	}
	s.tick(now)
	if requests != 2 || s.inFlight != 2 {
		t.Fatal("download queue exceeded worker budget")
	}
}

func TestHashSchedulerFairUnderNewDiscoveries(t *testing.T) {
	now := time.Now()
	calls := make(map[string]int)
	s := newHashScheduler(1, func(hash string) error { calls[hash]++; return nil }, func([]byte, string, int) bool { return false })
	hash := strings.Repeat("h", 20)
	s.add(hashDiscovery{hash: hash}, now)
	for second := 0; second < 60; second++ {
		for i := 0; i < 16; i++ {
			s.add(hashDiscovery{hash: fmt.Sprintf("%020d", second*16+i)}, now.Add(time.Duration(second)*time.Second))
		}
		s.tick(now.Add(time.Duration(second) * time.Second))
	}
	if calls[hash] < 2 {
		t.Fatalf("new hashes starved an older hash: lookups=%d", calls[hash])
	}
}

func TestHashSchedulerAttemptLimitAndCooldown(t *testing.T) {
	now := time.Now()
	s := newHashScheduler(1, func(string) error { return nil }, func([]byte, string, int) bool { return false })
	hash := strings.Repeat("h", 20)
	for i := 0; i < 20; i++ {
		s.add(hashDiscovery{hash: hash, peer: &dht.Request{InfoHash: []byte(hash), IP: "8.8.8.8", Port: i + 1}}, now)
	}
	entry := s.pending[hash].Value.(*pendingHash)
	if len(entry.peers) != maxHashPeers {
		t.Fatal("candidate peer list unbounded")
	}
	for i := 0; i < maxHashAttempts+1; i++ {
		s.tick(now)
	}
	if len(s.pending) != 0 {
		t.Fatal("rejected downloads bypassed retry budget")
	}
	s.add(hashDiscovery{hash: hash}, now.Add(30*time.Second))
	if len(s.pending) != 0 {
		t.Fatal("exhausted hash immediately requeued")
	}
	s.add(hashDiscovery{hash: hash}, now.Add(time.Minute))
	if len(s.pending) != 1 {
		t.Fatal("exhausted hash cannot be retried after cooldown")
	}
}

func TestHashSchedulerRunDrainsOnStop(t *testing.T) {
	s := newHashScheduler(1, func(string) error { return errors.New("not ready") }, func([]byte, string, int) bool { return false })
	responses := make(chan dht.Response, 3)
	failures := make(chan dht.MetadataFailure, 1)
	output := make(chan mysql.Data, 4)
	responses <- dht.Response{Request: dht.Request{InfoHash: []byte(strings.Repeat("h", 20))}, MetadataInfo: []byte(dht.Encode(map[string]interface{}{"name": "resource", "length": 7}))}
	responses <- dht.Response{Request: dht.Request{InfoHash: []byte(strings.Repeat("i", 20))}, MetadataInfo: []byte("invalid")}
	responses <- dht.Response{Request: dht.Request{InfoHash: []byte(strings.Repeat("j", 20))}, MetadataInfo: []byte(dht.Encode(map[string]interface{}{"name": "name-only"}))}
	close(s.stop)
	close(responses)
	close(failures)
	done := make(chan struct{})
	go func() { s.run(responses, failures, output); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not drain metadata responses")
	}
	item, ok := <-output
	if !ok || item.Name != "resource" || len(item.Files) != 1 || item.Length != 7 {
		t.Fatal("metadata with a valid filelist lost at stop")
	}
	if _, ok := <-output; ok {
		t.Fatal("invalid metadata or metadata without a filelist was emitted")
	}
	if s.cached[strings.Repeat("j", 20)].Value.(*cachedHash).item != nil {
		t.Fatal("metadata without a filelist was cached as a success")
	}
}
