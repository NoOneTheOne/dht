package dht

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestSampleInfohashesResponseFeedsHashesAndHints(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	addr := remote.LocalAddr().(*net.UDPAddr)
	trans := addKRPCTestTransaction(t, d, addr, sampleInfohashesType)
	var got []string
	d.OnSampleInfohashes = func(hashes []string) { got = append(got, hashes...) }
	a, b := strings.Repeat("a", 20), strings.Repeat("b", 20)
	mentioned, err := newNode(strings.Repeat("m", 20), "udp4", "8.8.4.4:6881")
	if err != nil {
		t.Fatal(err)
	}
	r := map[string]interface{}{
		"id": strings.Repeat("r", 20), "interval": 600, "num": 77,
		"samples": a + b, "nodes": mentioned.CompactNodeInfo(),
	}
	if !handleResponse(d, addr, makeResponse(trans.id, r)) {
		t.Fatal("valid sample_infohashes response rejected")
	}
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("samples delivered = %q", got)
	}
	if hint := d.lookupHint(a); hint == nil || hint.addr.String() != addr.String() {
		t.Fatal("sampled hash did not record its responder as lookup hint")
	}
	if d.lookupHint(a) != nil {
		t.Fatal("lookup hint must be used once")
	}
	if _, ok := d.routingTable.GetNodeByAddress("8.8.4.4:6881"); !ok {
		t.Fatal("nodes from the response were not learned")
	}
	now := time.Now()
	if d.sampleDue(addr.String(), now.Add(9*time.Minute)) || !d.sampleDue(addr.String(), now.Add(11*time.Minute)) {
		t.Fatal("responder's interval was not honoured")
	}
}

func TestSampleInfohashesRejectsMalformedSamples(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	addr := remote.LocalAddr().(*net.UDPAddr)
	trans := addKRPCTestTransaction(t, d, addr, sampleInfohashesType)
	d.OnSampleInfohashes = func([]string) { t.Fatal("malformed samples delivered") }
	r := map[string]interface{}{"id": strings.Repeat("r", 20), "samples": strings.Repeat("x", 21)}
	if handleResponse(d, addr, makeResponse(trans.id, r)) {
		t.Fatal("samples not a multiple of 20 bytes accepted")
	}
}

func TestSampleIntervalIsClamped(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	addr := remote.LocalAddr().(*net.UDPAddr)
	for _, tc := range []struct {
		interval int
		due      time.Duration
	}{{0, sampleMinWait}, {-5, sampleMinWait}, {1 << 30, maxSampleInterval}} {
		trans := addKRPCTestTransaction(t, d, addr, sampleInfohashesType)
		r := map[string]interface{}{"id": strings.Repeat("r", 20), "samples": "", "interval": tc.interval}
		if !handleResponse(d, addr, makeResponse(trans.id, r)) {
			t.Fatal("response rejected")
		}
		d.transactionManager.delete(trans.id)
		now := time.Now()
		if d.sampleDue(addr.String(), now.Add(tc.due-time.Second)) || !d.sampleDue(addr.String(), now.Add(tc.due+time.Second)) {
			t.Fatalf("interval %d not clamped to %s", tc.interval, tc.due)
		}
	}
}

func TestSampleRoundRespectsScheduleAndBusyQueue(t *testing.T) {
	d, _ := newKRPCTestDHT(t)
	// Crawl mode's single unbounded bucket: no splits, candidates or the
	// background refresh pings they trigger.
	d.routingTable = newRoutingTable(1<<30, d)
	for i := 0; i < 20; i++ {
		n, err := newNode(randomString(20), "udp4", net.JoinHostPort(net.IPv4(8, 8, 1, byte(i+1)).String(), "6881"))
		if err != nil {
			t.Fatal(err)
		}
		d.routingTable.Insert(n)
	}
	d.sampleRound()
	if got := len(d.transactionManager.queryChan); got != sampleNodesPerRound {
		t.Fatalf("queued %d sample queries, want %d", got, sampleNodesPerRound)
	}
	sampled := map[string]bool{}
	for len(d.transactionManager.queryChan) > 0 {
		q := <-d.transactionManager.queryChan
		if q.data["q"] != sampleInfohashesType {
			t.Fatalf("unexpected query %v", q.data["q"])
		}
		sampled[q.node.addr.String()] = true
	}
	clear(d.transactionManager.pending)
	// Already-asked nodes stay quiet until their interval passes.
	for round := 0; round < 10; round++ {
		d.sampleRound()
		for len(d.transactionManager.queryChan) > 0 {
			q := <-d.transactionManager.queryChan
			if sampled[q.node.addr.String()] {
				t.Fatal("node re-sampled before its interval")
			}
			sampled[q.node.addr.String()] = true
		}
		clear(d.transactionManager.pending)
	}
	// A busy query queue defers sampling entirely.
	for i := 0; i < cap(d.transactionManager.queryChan)/2; i++ {
		d.transactionManager.queryChan <- &query{node: &node{addr: &net.UDPAddr{}}, data: makeQuery("x", pingType, nil)}
	}
	before := len(d.transactionManager.queryChan)
	d.sampleSchedule = newBoundedSchedule(64)
	d.sampleRound()
	if len(d.transactionManager.queryChan) != before {
		t.Fatal("sampling competed with a busy query queue")
	}
}

func TestSampleInfohashesRequestAndUnknownMethod(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	addr := remote.LocalAddr().(*net.UDPAddr)
	target := strings.Repeat("t", 20)
	if !handleRequest(d, addr, makeQuery("s1", sampleInfohashesType, map[string]interface{}{
		"id": strings.Repeat("r", 20), "target": target,
	})) {
		t.Fatal("sample_infohashes request rejected")
	}
	reply := readKRPCTestMessage(t, remote)
	r, _ := reply["r"].(map[string]interface{})
	if r == nil || r["interval"] != int(maxSampleInterval/time.Second) || r["num"] != 0 || r["samples"] != "" {
		t.Fatalf("unexpected sample reply %v", reply)
	}
	if ipField, _ := reply["ip"].(string); ipField != mustCompact(t, addr) {
		t.Fatal("BEP-42 ip field does not echo the requester's address")
	}

	handleRequest(d, addr, makeQuery("u1", "vote", map[string]interface{}{"id": strings.Repeat("r", 20)}))
	reply = readKRPCTestMessage(t, remote)
	e, _ := reply["e"].([]interface{})
	if reply["y"] != "e" || len(e) != 2 || e[0] != methodUnknown {
		t.Fatalf("unknown method not answered with 204: %v", reply)
	}
}

func mustCompact(t *testing.T, addr *net.UDPAddr) string {
	t.Helper()
	compact, err := encodeCompactIPPortInfo(addr.IP, addr.Port)
	if err != nil {
		t.Fatal(err)
	}
	return compact
}

func TestLookupStopsOnceEnoughPeersFound(t *testing.T) {
	d, _ := newKRPCTestDHT(t)
	d.lookups = map[string]*peerLookup{}
	l := &peerLookup{dht: d, hash: strings.Repeat("h", 20), deadline: time.Now().Add(time.Minute), visited: map[string]bool{}}
	for i := 0; i < 10; i++ {
		n, _ := newNode(randomString(20), "udp4", net.JoinHostPort(net.IPv4(8, 8, 2, byte(i+1)).String(), "6881"))
		l.candidates = append(l.candidates, n)
	}
	for i := 0; i < peerLookupEnough; i++ {
		l.foundPeer()
	}
	l.mu.Lock()
	queued := l.advance()
	l.mu.Unlock()
	if queued || len(d.transactionManager.queryChan) != 0 {
		t.Fatal("lookup kept querying after enough peers were found")
	}
}

func TestGetPeersStartsAtSampleHint(t *testing.T) {
	d, _ := newKRPCTestDHT(t)
	d.ready.Store(true)
	d.OnGetPeersResponse = func(string, *Peer) {}
	far, _ := newNode(strings.Repeat("\xff", 20), "udp4", "8.8.3.1:6881")
	d.routingTable.Insert(far)
	hash := strings.Repeat("\x01", 20)
	hint, _ := newNode(strings.Repeat("\x01", 19)+"\x02", "udp4", "8.8.3.2:6881")
	d.lookupHints.set(hash, time.Time{}, hint)
	if err := d.GetPeers(hash); err != nil {
		t.Fatal(err)
	}
	asked := map[string]bool{}
	for len(d.transactionManager.queryChan) > 0 {
		asked[(<-d.transactionManager.queryChan).node.addr.String()] = true
	}
	if !asked["8.8.3.2:6881"] {
		t.Fatalf("lookup ignored the sampler hint; asked %v", asked)
	}
}

func TestGetPeersAsksFarSampleHintFirst(t *testing.T) {
	d, _ := newKRPCTestDHT(t)
	d.ready.Store(true)
	d.OnGetPeersResponse = func(string, *Peer) {}
	hash := strings.Repeat("\x01", 20)
	// More nodes closer to the hash than the lookup queries at once.
	for i := 0; i < 2*peerLookupAlpha; i++ {
		near, _ := newNode(strings.Repeat("\x01", 19)+string(rune(0x10+i)), "udp4", fmt.Sprintf("8.8.4.%d:6881", i+1))
		d.routingTable.Insert(near)
	}
	hint, _ := newNode(strings.Repeat("\xfe", 20), "udp4", "8.8.3.2:6881") // Far from the hash.
	d.lookupHints.set(hash, time.Time{}, hint)
	if err := d.GetPeers(hash); err != nil {
		t.Fatal(err)
	}
	if first := (<-d.transactionManager.queryChan).node.addr.String(); first != "8.8.3.2:6881" {
		t.Fatalf("first get_peers went to %s, not the sampler hint", first)
	}
}
