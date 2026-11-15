package dht

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// These are public addresses used only as data; no test opens a network socket.
func verifiedTestNode(index int) *node {
	id := make([]byte, 20)
	binary.BigEndian.PutUint64(id[12:], uint64(index))
	return &node{id: newBitmapFromBytes(id), addr: &net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 10000 + index}}
}

func verifiedTestWrite(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func verifiedTestRecord(no *node, now time.Time) verifiedNodeRecord {
	id, endpoint, _ := verifiedNodeIdentity(no)
	return verifiedNodeRecord{ID: append([]byte(nil), id...), Endpoint: endpoint, LastSuccess: now, RTT: 20 * time.Millisecond}
}

func TestVerifiedOnlySuccessAndCopies(t *testing.T) {
	s := newVerifiedNodeStore("")
	n := verifiedTestNode(1)
	s.recordFailure(n)
	if len(s.nodes) != 0 || len(s.seeds(time.Now())) != 0 {
		t.Fatal("failure or hearsay created a verified entry")
	}
	s.recordSuccess(n, 100*time.Millisecond)
	s.recordSuccess(n, 200*time.Millisecond)
	r := s.nodes[n.addr.String()]
	if r.RTT != 125*time.Millisecond || r.Failures != 0 || r.LastSuccess.IsZero() {
		t.Fatalf("unexpected success record: %+v", r)
	}
	seeds := s.seeds(time.Now())
	if len(seeds) != 1 || !seeds[0].lastActiveTime.IsZero() {
		t.Fatal("seed missing or restored as an active node")
	}
	originalID := append([]byte(nil), n.id.data...)
	n.id.data[0] = 77
	n.addr.IP[0] = 77
	seeds[0].id.data[1] = 88
	seeds[0].addr.IP[0] = 88
	seeds[0].addr.Port = 1
	again := s.seeds(time.Now())
	if len(again) != 1 || !bytes.Equal(again[0].id.data, originalID) || again[0].addr.String() != r.Endpoint {
		t.Fatal("store retained mutable input or exposed its internal state")
	}
	if err := s.load(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.save(time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestVerifiedFailuresAndIdentity(t *testing.T) {
	s := newVerifiedNodeStore("")
	n := verifiedTestNode(1)
	s.recordSuccess(n, time.Millisecond)
	wrongID := verifiedTestNode(2)
	wrongID.addr.Port = n.addr.Port
	s.recordFailure(wrongID)
	if s.nodes[n.addr.String()].Failures != 0 {
		t.Fatal("different ID changed endpoint quality")
	}
	for i := 0; i < 10; i++ {
		s.recordFailure(n)
	}
	if len(s.seeds(time.Now())) != 0 || s.nodes[n.addr.String()].Failures != 3 {
		t.Fatal("consecutive failures were not filtered/saturated")
	}
	s.recordSuccess(n, 5*time.Millisecond)
	if len(s.seeds(time.Now())) != 1 || s.nodes[n.addr.String()].Failures != 0 {
		t.Fatal("success did not reset failures")
	}
	s.recordSuccess(wrongID, 40*time.Millisecond)
	s.recordFailure(n)
	r := s.nodes[n.addr.String()]
	if !bytes.Equal(r.ID, wrongID.id.data) || r.RTT != 40*time.Millisecond || r.Failures != 0 || len(s.nodes) != 1 {
		t.Fatal("endpoint identity replacement retained old identity's quality")
	}
	s.recordSuccess(wrongID, -time.Second)
	if s.nodes[n.addr.String()].RTT != 40*time.Millisecond {
		t.Fatal("negative latency corrupted RTT")
	}
}

func TestVerifiedCapacityAndOrdering(t *testing.T) {
	s := newVerifiedNodeStore("")
	base := time.Now().Add(-time.Hour)
	for i := 0; i < verifiedNodeCapacity; i++ {
		n := verifiedTestNode(i)
		s.recordSuccess(n, time.Millisecond)
		r := s.nodes[n.addr.String()]
		r.LastSuccess = base.Add(time.Duration(i) * time.Second)
		s.nodes[r.Endpoint] = r
	}
	// Quality outranks recency: evict a failed entry even if recently successful.
	failed := verifiedTestNode(verifiedNodeCapacity - 1)
	s.recordFailure(failed)
	s.recordSuccess(verifiedTestNode(verifiedNodeCapacity), time.Millisecond)
	if len(s.nodes) != verifiedNodeCapacity {
		t.Fatalf("capacity = %d", len(s.nodes))
	}
	if _, ok := s.nodes[failed.addr.String()]; ok {
		t.Fatal("retained failed entry instead of a healthy one")
	}
	s.recordSuccess(verifiedTestNode(verifiedNodeCapacity+1), time.Millisecond)
	if _, ok := s.nodes[verifiedTestNode(0).addr.String()]; ok {
		t.Fatal("oldest healthy success was not evicted")
	}
	seeds := s.seeds(time.Now())
	if len(seeds) != verifiedNodeSeedLimit || seeds[0].addr.Port != 10000+verifiedNodeCapacity+1 {
		t.Fatal("seed limit or most-recent-first order is wrong")
	}
	for i := 1; i < len(seeds); i++ {
		a, b := s.nodes[seeds[i-1].addr.String()], s.nodes[seeds[i].addr.String()]
		if verifiedNodeLess(b, a) {
			t.Fatal("seeds are not quality/recency sorted")
		}
	}
}

func TestVerifiedQualityOrdering(t *testing.T) {
	s := newVerifiedNodeStore("")
	now := time.Now()
	for i := 0; i < 4; i++ {
		r := verifiedTestRecord(verifiedTestNode(i), now)
		r.RTT = time.Duration(4-i) * time.Millisecond
		if i == 3 {
			r.Failures = 1
		}
		s.nodes[r.Endpoint] = r
	}
	seeds := s.seeds(now)
	for i, want := range []int{2, 1, 0, 3} {
		if seeds[i].addr.Port != 10000+want {
			t.Fatalf("quality order at %d: got %v", i, seeds[i].addr)
		}
	}
}

func TestVerifiedFreshness(t *testing.T) {
	now := time.Now().UTC()
	s := newVerifiedNodeStore("")
	for i, stamp := range []time.Time{now, now.Add(-24 * time.Hour), now.Add(time.Nanosecond), {}, now.Add(-23 * time.Hour)} {
		r := verifiedTestRecord(verifiedTestNode(i), stamp)
		s.nodes[r.Endpoint] = r
	}
	seeds := s.seeds(now)
	if len(seeds) != 2 || seeds[0].addr.Port != 10000 || seeds[1].addr.Port != 10004 {
		t.Fatalf("unexpected fresh seeds: %v", seeds)
	}
	if len(s.seeds(now.Add(24*time.Hour))) != 1 {
		// The original future record is now fresh; all others have expired.
		t.Fatal("expiration does not use the supplied time")
	}
}

func TestVerifiedEndpointValidation(t *testing.T) {
	for _, endpoint := range []string{
		"localhost:6881", "router.example:6881", "127.0.0.1:6881", "10.1.2.3:6881",
		"172.16.0.1:6881", "192.168.0.1:6881", "0.1.2.3:6881", "0.0.0.0:6881",
		"100.64.0.1:6881", "169.254.1.1:6881", "192.0.2.1:6881", "198.18.0.1:6881",
		"198.51.100.1:6881", "203.0.113.1:6881", "224.0.0.1:6881", "255.255.255.255:6881",
		"8.8.8.8:0", "8.8.8.8:-1", "8.8.8.8:65536", "8.8.8.8:dht", "8.8.8.8",
		"[::]:6881", "[::1]:6881", "[fc00::1]:6881", "[fe80::1%en0]:6881", "[ff02::1]:6881",
		"[2001:db8::1]:6881", "[2001::1]:6881", "[2002::1]:6881", "[3fff::1]:6881",
		"[::ffff:192.168.1.1]:6881",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if _, _, ok := verifiedNodeEndpoint(endpoint); ok {
				t.Fatalf("accepted non-public/invalid endpoint %q", endpoint)
			}
		})
	}
	for _, endpoint := range []string{"8.8.8.8:1", "1.1.1.1:65535", "[2606:4700:4700::1111]:6881", "[::ffff:8.8.8.8]:6881"} {
		if _, _, ok := verifiedNodeEndpoint(endpoint); !ok {
			t.Fatalf("rejected public endpoint %q", endpoint)
		}
	}
	s := newVerifiedNodeStore("")
	for _, n := range []*node{nil, {}, {id: newBitmap(159)}, {id: newBitmap(160), addr: &net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 6881, Zone: "en0"}}, {id: &bitmap{Size: 160, data: []byte{1}}, addr: verifiedTestNode(1).addr}} {
		s.recordSuccess(n, time.Millisecond)
		s.recordFailure(n)
	}
	if len(s.nodes) != 0 {
		t.Fatal("accepted malformed node")
	}
}

func TestVerifiedSaveLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.json")
	s := newVerifiedNodeStore(path)
	if err := s.load(time.Now()); err != nil {
		t.Fatalf("missing file: %v", err)
	}
	for i := 0; i < 300; i++ {
		s.recordSuccess(verifiedTestNode(i), time.Duration(i+1)*time.Millisecond)
	}
	ipv6 := verifiedTestNode(400)
	ipv6.addr.IP = net.ParseIP("2606:4700:4700::1111")
	s.recordSuccess(ipv6, 12*time.Millisecond)
	s.recordFailure(ipv6)
	failed := verifiedTestNode(299)
	for i := 0; i < 3; i++ {
		s.recordFailure(failed)
	}
	now := time.Now()
	if err := s.save(now); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("file permissions: %v, %v", info, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var records []verifiedNodeRecord
	if err := json.Unmarshal(data, &records); err != nil || len(records) != verifiedNodeDiskLimit {
		t.Fatalf("saved count=%d err=%v", len(records), err)
	}
	restored := newVerifiedNodeStore(path)
	if err := restored.load(now); err != nil {
		t.Fatal(err)
	}
	if len(restored.nodes) != verifiedNodeDiskLimit || len(restored.seeds(now)) != verifiedNodeSeedLimit {
		t.Fatal("restore count or seed bound is wrong")
	}
	for _, r := range records {
		got := restored.nodes[r.Endpoint]
		if !bytes.Equal(got.ID, r.ID) || !got.LastSuccess.Equal(r.LastSuccess) || got.RTT != r.RTT || got.Failures != r.Failures {
			t.Fatalf("restore changed record: %+v -> %+v", r, got)
		}
	}
	if _, ok := restored.nodes[failed.addr.String()]; ok {
		t.Fatal("saved terminally failed node")
	}
	// A fresh in-memory observation must not be rolled back by loading disk.
	restored.recordSuccess(verifiedTestNode(298), time.Millisecond)
	restored.recordFailure(verifiedTestNode(298))
	before := restored.nodes[verifiedTestNode(298).addr.String()]
	if err := restored.load(time.Now()); err != nil {
		t.Fatal(err)
	}
	after := restored.nodes[before.Endpoint]
	if !after.LastSuccess.Equal(before.LastSuccess) || after.Failures != before.Failures || after.RTT != before.RTT {
		t.Fatal("load overwrote a newer live observation")
	}
}

func TestVerifiedIPv6Recovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.json")
	s := newVerifiedNodeStore(path)
	n := verifiedTestNode(1)
	n.addr.IP = net.ParseIP("2606:4700:4700::1111")
	s.recordSuccess(n, 10*time.Millisecond)
	s.recordFailure(n)
	now := time.Now()
	if err := s.save(now); err != nil {
		t.Fatal(err)
	}
	restored := newVerifiedNodeStore(path)
	if err := restored.load(now); err != nil {
		t.Fatal(err)
	}
	seeds := restored.seeds(now)
	if len(seeds) != 1 || !seeds[0].addr.IP.Equal(n.addr.IP) || seeds[0].addr.Port != n.addr.Port || !bytes.Equal(seeds[0].id.data, n.id.data) || restored.nodes[n.addr.String()].Failures != 1 {
		t.Fatal("IPv6 identity/quality was not restored")
	}
}

func TestVerifiedLoadIgnoresInvalidRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.json")
	now := time.Now().UTC()
	valid := verifiedTestRecord(verifiedTestNode(1), now.Add(-time.Hour))
	ipv6 := verifiedTestRecord(verifiedTestNode(2), now)
	ipv6.Endpoint = "[2606:4700:4700::1111]:6881"
	ipv6.Failures = 2
	records := []any{valid, ipv6, map[string]any{"last_success": "not a timestamp"}, 42, nil}
	for i, mutate := range []func(*verifiedNodeRecord){
		func(r *verifiedNodeRecord) { r.LastSuccess = now.Add(-24 * time.Hour) },
		func(r *verifiedNodeRecord) { r.LastSuccess = now.Add(time.Nanosecond) },
		func(r *verifiedNodeRecord) { r.ID = []byte{1} },
		func(r *verifiedNodeRecord) { r.Endpoint = "localhost:6881" },
		func(r *verifiedNodeRecord) { r.Endpoint = "192.168.1.1:6881" },
		func(r *verifiedNodeRecord) { r.RTT = -1 },
		func(r *verifiedNodeRecord) { r.Failures = -1 },
		func(r *verifiedNodeRecord) { r.LastSuccess = time.Time{} },
	} {
		r := verifiedTestRecord(verifiedTestNode(i+10), now)
		mutate(&r)
		records = append(records, r)
	}
	terminal := verifiedTestRecord(verifiedTestNode(50), now)
	terminal.Failures = 3
	records = append(records, terminal)
	verifiedTestWrite(t, path, records)
	s := newVerifiedNodeStore(path)
	if err := s.load(now); err != nil {
		t.Fatal(err)
	}
	if len(s.nodes) != 3 || len(s.seeds(now)) != 2 {
		t.Fatalf("invalid records not ignored: entries=%d seeds=%d", len(s.nodes), len(s.seeds(now)))
	}
	if err := s.save(now); err != nil {
		t.Fatal(err)
	}
	restored := newVerifiedNodeStore(path)
	if err := restored.load(now); err != nil || len(restored.nodes) != 2 {
		t.Fatalf("invalid/failed records persisted: %d %v", len(restored.nodes), err)
	}
}

func TestVerifiedLoadErrorsLeaveMemoryIntact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.json")
	s := newVerifiedNodeStore(path)
	s.recordSuccess(verifiedTestNode(1), time.Millisecond)
	tooMany, err := json.Marshal(make([]verifiedNodeRecord, verifiedNodeDiskLimit+1))
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"", "[", "{}", "null", "[] trailing", strings.Repeat(" ", verifiedNodeReadLimit+1), string(tooMany)} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := s.load(time.Now()); err == nil {
			t.Fatalf("expected error for invalid/oversized input (%d bytes)", len(data))
		}
		if len(s.nodes) != 1 || len(s.seeds(time.Now())) != 1 {
			t.Fatal("failed load changed in-memory observations")
		}
	}
}

func TestVerifiedAtomicSaveAndErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.json")
	s := newVerifiedNodeStore(path)
	s.recordSuccess(verifiedTestNode(1), time.Millisecond)
	if err := s.save(time.Now()); err != nil {
		t.Fatal(err)
	}
	// An open old inode keeps its complete document after atomic replacement.
	old, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.recordSuccess(verifiedTestNode(2), time.Millisecond)
	if err := s.save(time.Now()); err != nil {
		t.Fatal(err)
	}
	oldData := make([]byte, len(before))
	if _, err := old.ReadAt(oldData, 0); err != nil || !bytes.Equal(oldData, before) {
		t.Fatalf("save modified the old inode: %v", err)
	}
	missing := filepath.Join(dir, "missing", "nodes.json")
	if err := newVerifiedNodeStore(missing).save(time.Now()); err == nil {
		t.Fatal("save created a missing parent")
	}
	if _, err := os.Stat(filepath.Dir(missing)); !os.IsNotExist(err) {
		t.Fatal("save created parent directory")
	}
	// Rename over a directory must fail without deleting it or leaking temps.
	blocked := filepath.Join(dir, "blocked")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if err := newVerifiedNodeStore(blocked).save(time.Now()); err == nil {
		t.Fatal("expected rename error")
	}
	if err := newVerifiedNodeStore(blocked).load(time.Now()); err == nil {
		t.Fatal("expected directory read error")
	}
	temps, err := filepath.Glob(filepath.Join(dir, ".verified-nodes-*"))
	if err != nil || len(temps) != 0 {
		t.Fatalf("temporary file leak: %v %v", temps, err)
	}
}

func TestVerifiedConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.json")
	s := newVerifiedNodeStore(path)
	if err := s.save(time.Now()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				n := verifiedTestNode((worker*80 + i) % 600)
				s.recordSuccess(n, time.Duration(i+1)*time.Millisecond)
				s.recordFailure(n)
				seeds := s.seeds(time.Now())
				if len(seeds) > 0 {
					seeds[0].id.data[0]++
					seeds[0].addr.IP[0]++
				}
				if i%20 == 0 {
					if err := s.save(time.Now()); err != nil {
						t.Error(err)
					}
					if err := s.load(time.Now()); err != nil {
						t.Error(err)
					}
				}
			}
		}(worker)
	}
	// Read without taking the store mutex to check on-disk atomic publication.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			data, err := os.ReadFile(path)
			if err != nil || !json.Valid(data) {
				t.Error(fmt.Errorf("incomplete concurrent snapshot: %v", err))
				return
			}
		}
	}()
	wg.Wait()
	if len(s.nodes) > verifiedNodeCapacity || len(s.seeds(time.Now())) > verifiedNodeSeedLimit {
		t.Fatal("concurrent use exceeded bounds")
	}
}
