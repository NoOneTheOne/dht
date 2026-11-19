package dht

import (
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"
)

func lookupTestDHT() *DHT {
	d := &DHT{InitParam: &InitParam{K: 8, MaxNodes: 1, Try: 1, QueryTimeout: time.Second, OnGetPeersResponse: func(string, *Peer) {}}, node: &node{id: newBitmap(160)}, blackList: newBlackList(100), stop: make(chan struct{})}
	d.routingTable = newRoutingTable(8, d)
	d.transactionManager = newTransactionManager(math.MaxUint32, d)
	d.ready.Store(true)
	return d
}

func TestPeerLookupContinuesWithFullRoutingTable(t *testing.T) {
	d := lookupTestDHT()
	nodes := nearestTestNodes(10)
	for i, n := range nodes {
		n.addr = &net.UDPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 10000 + i}
	}
	d.routingTable.Insert(nodes[0])
	hash := strings.Repeat("h", 20)
	if err := d.GetPeers(hash); err != nil {
		t.Fatal(err)
	}
	q := <-d.transactionManager.queryChan
	trans := d.transactionManager.newTransaction(q.data["t"].(string), q)
	if !d.transactionManager.insert(trans) {
		t.Fatal("insert transaction")
	}
	response := map[string]interface{}{"t": trans.id, "r": map[string]interface{}{"id": q.node.id.RawString(), "token": "test", "nodes": nodes[1].CompactNodeInfo()}}
	if !handleResponse(d, q.node.addr, response) {
		t.Fatal("response rejected")
	}
	d.transactionManager.delete(trans.id)
	d.transactionManager.Lock()
	delete(d.transactionManager.pending, d.transactionManager.genIndexKeyByQuery(q.data, q.node.addr.String()))
	d.transactionManager.Unlock()
	q.lookup.complete(q)
	if d.routingTable.Len() != 1 {
		t.Fatal("routing table bound changed")
	}
	select {
	case next := <-d.transactionManager.queryChan:
		if next.node.addr.String() != nodes[1].addr.String() {
			t.Fatal("lookup did not follow returned candidate")
		}
	default:
		t.Fatal("full routing table prematurely terminated lookup")
	}
}

func TestPeerLookupBudgetAndCandidateBounds(t *testing.T) {
	d := lookupTestDHT()
	nodes := nearestTestNodes(200)
	for i, n := range nodes {
		n.addr = &net.UDPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 10000 + i}
	}
	hash := strings.Repeat("h", 20)
	if err := d.startPeerLookup(hash, nodes, nil); err != nil {
		t.Fatal(err)
	}
	lookup := d.lookups[hash]
	if lookup.active != peerLookupAlpha || len(lookup.candidates) > peerLookupCandidates {
		t.Fatal("lookup bounds exceeded")
	}
	count := 0
	for len(d.transactionManager.queryChan) > 0 {
		q := <-d.transactionManager.queryChan
		d.transactionManager.Lock()
		delete(d.transactionManager.pending, d.transactionManager.genIndexKeyByQuery(q.data, q.node.addr.String()))
		d.transactionManager.Unlock()
		q.resultNodes = nodes
		q.lookup.complete(q)
		count++
		if count > peerLookupBudget {
			t.Fatal("unbounded lookup fanout")
		}
	}
	if count != peerLookupBudget || len(d.lookups) != 0 || lookup.active != 0 {
		t.Fatalf("lookup did not end at budget: %d", count)
	}
}

func TestPeerLookupDeadlineAndCapacity(t *testing.T) {
	d := lookupTestDHT()
	nodes := nearestTestNodes(5)
	for i, n := range nodes {
		n.addr = &net.UDPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 10000 + i}
	}
	hash := strings.Repeat("h", 20)
	if err := d.startPeerLookup(hash, nodes, nil); err != nil {
		t.Fatal(err)
	}
	lookup := d.lookups[hash]
	lookup.deadline = time.Now().Add(-time.Second)
	for len(d.transactionManager.queryChan) > 0 {
		q := <-d.transactionManager.queryChan
		q.lookup.complete(q)
	}
	if len(d.lookups) != 0 {
		t.Fatal("expired lookup retained")
	}
	for i := 0; i < maxPeerLookups; i++ {
		d.lookups[fmt.Sprintf("%020d", i)] = &peerLookup{}
	}
	if err := d.startPeerLookup(hash, nodes, nil); !errors.Is(err, ErrLookupCapacity) || !errors.Is(err, ErrQueryNotQueued) {
		t.Fatalf("active lookup capacity exceeded: %v", err)
	}
}

func TestRefreshRotatesContacts(t *testing.T) {
	d := lookupTestDHT()
	d.MaxNodes = 8
	d.RefreshNodeNum = 2
	nodes := nearestTestNodes(6)
	for i, n := range nodes {
		n.addr = &net.UDPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 10000 + i}
		d.routingTable.Insert(n)
	}
	seen := make(map[string]bool)
	for round := 0; round < 3; round++ {
		d.routingTable.OptimizedRefresh()
		for len(d.transactionManager.queryChan) > 0 {
			q := <-d.transactionManager.queryChan
			seen[q.node.addr.String()] = true
		}
		clear(d.transactionManager.pending)
	}
	if len(seen) != len(nodes) {
		t.Fatalf("refresh repeatedly probed the same contacts: %d of %d", len(seen), len(nodes))
	}
}

func TestPeerLookupUDPTimeoutsReleaseState(t *testing.T) {
	d := lookupTestDHT()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	d.conn = conn
	d.QueryTimeout = time.Millisecond
	d.QueryWorkerLimit = 3
	remote, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	hash := strings.Repeat("h", 20)
	lookup := &peerLookup{dht: d, hash: hash, deadline: time.Now().Add(time.Second), visited: make(map[string]bool), active: 1, attempted: 1}
	d.lookups = map[string]*peerLookup{hash: lookup}
	peer := &node{id: newBitmapFromString(strings.Repeat("p", 20)), addr: remote.LocalAddr().(*net.UDPAddr)}
	if !d.transactionManager.queueQuery(peer, getPeersType, map[string]interface{}{"id": d.id(hash), "info_hash": hash}, lookup) {
		t.Fatal("failed to queue local transport test")
	}
	done := make(chan struct{})
	go func() { d.transactionManager.run(); close(done) }()
	finished := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			d.lookupMu.Lock()
			empty := len(d.lookups) == 0
			d.lookupMu.Unlock()
			if empty {
				close(finished)
				return
			}
			select {
			case <-d.stop:
				return
			default:
			}
		}
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		close(d.stop)
		<-done
		t.Fatal("lookup remained after UDP timeout")
	}
	close(d.stop)
	<-done
	if d.transactionManager.len() != 0 {
		t.Fatal("transactions leaked")
	}
}

func TestPeerLookupRejectsPrivateCandidates(t *testing.T) {
	d := lookupTestDHT()
	nodes := nearestTestNodes(4)
	for i, ip := range []string{"127.0.0.1", "192.168.1.1", "169.254.1.1", "100.64.0.1"} {
		nodes[i].addr = &net.UDPAddr{IP: net.ParseIP(ip), Port: 6881}
	}
	if err := d.startPeerLookup(strings.Repeat("h", 20), nodes, nil); err != ErrQueryNotQueued || len(d.transactionManager.queryChan) != 0 {
		t.Fatal("private lookup candidates were accepted")
	}
}

func TestPeerLookupQueryDeadlineStopsRetries(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	d.stop = make(chan struct{})
	d.QueryTimeout = time.Hour
	lookup := &peerLookup{deadline: time.Now().Add(20 * time.Millisecond)}
	q := &query{node: &node{addr: remote.LocalAddr().(*net.UDPAddr)}, data: makeQuery("deadline", pingType, nil), lookup: lookup}
	done := make(chan struct{})
	go func() { d.transactionManager.query(q, 3); close(done) }()
	readKRPCTestMessage(t, remote)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		close(d.stop)
		<-done
		t.Fatal("lookup deadline did not bound query wait")
	}
	remote.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	if _, _, err := remote.ReadFromUDP(make([]byte, 1024)); err == nil {
		t.Fatal("query retried after deadline")
	}
}

func TestBootstrapResponseIdentityIsVerified(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	q := &query{node: &node{addr: remote.LocalAddr().(*net.UDPAddr)}, data: makeQuery("seed", pingType, nil)}
	trans := d.transactionManager.newTransaction("seed", q)
	d.transactionManager.insert(trans)
	id := strings.Repeat("b", 20)
	if !handleResponse(d, q.node.addr, map[string]interface{}{"t": "seed", "r": map[string]interface{}{"id": id}}) {
		t.Fatal("bootstrap response rejected")
	}
	trans.mu.Lock()
	defer trans.mu.Unlock()
	if trans.verifiedNode == nil || trans.verifiedNode.id.RawString() != id || q.node.id != nil {
		t.Fatal("verified response identity lost or original request mutated")
	}
}

func TestPeerLookupFindsPeersAndNodesTogether(t *testing.T) {
	d := lookupTestDHT()
	nodes := nearestTestNodes(2)
	for i, n := range nodes {
		n.addr = &net.UDPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 10000 + i}
	}
	received := 0
	d.OnGetPeersResponse = func(string, *Peer) { received++ }
	if err := d.startPeerLookup(strings.Repeat("h", 20), nodes[:1], nil); err != nil {
		t.Fatal(err)
	}
	q := <-d.transactionManager.queryChan
	trans := d.transactionManager.newTransaction(q.data["t"].(string), q)
	d.transactionManager.insert(trans)
	peer := newPeer(net.IPv4(1, 1, 1, 1), 6881, "test")
	if !handleResponse(d, q.node.addr, map[string]interface{}{"t": trans.id, "r": map[string]interface{}{"id": q.node.id.RawString(), "token": "test", "values": []interface{}{peer.CompactIPPortInfo()}, "nodes": nodes[1].CompactNodeInfo()}}) {
		t.Fatal("combined response rejected")
	}
	if received != 1 || len(q.resultNodes) != 1 {
		t.Fatal("peer or next-hop candidates lost")
	}
}
