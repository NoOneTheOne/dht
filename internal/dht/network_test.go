package dht

import (
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGetPeersWaitsForRoutingTable(t *testing.T) {
	d := &DHT{InitParam: &InitParam{KBucketSize: 8}}
	if err := d.GetPeers("0123456789012345678901234567890123456789"); err != ErrNotReady {
		t.Fatalf("uninitialized DHT returned %v", err)
	}
	d.routingTable = newRoutingTable(8, d)
	d.ready.Store(true)
	if err := d.GetPeers("0123456789012345678901234567890123456789"); err != ErrNotReady {
		t.Fatalf("empty routing table returned %v", err)
	}
}

func TestGetPeersValidatesHashAndBoundsFanout(t *testing.T) {
	d := &DHT{
		InitParam: &InitParam{K: 8, OnGetPeersResponse: func(string, *Peer) {}},
		node:      &node{id: newBitmap(160)},
		blackList: newBlackList(100),
	}
	d.routingTable = newRoutingTable(8, d)
	d.transactionManager = newTransactionManager(math.MaxUint32, d)
	for i, n := range nearestTestNodes(100) {
		n.addr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10000 + i}
		d.routingTable.cachedNodes.Set(n.addr.String(), n)
	}
	d.ready.Store(true)
	for _, hash := range []string{"", "short", strings.Repeat("x", 40), strings.Repeat("a", 21)} {
		if err := d.GetPeers(hash); err == nil {
			t.Fatalf("invalid hash %q accepted", hash)
		}
	}
	if len(d.transactionManager.queryChan) != 0 {
		t.Fatal("invalid hash queued queries")
	}
	for _, hash := range []string{strings.Repeat("a", 20), strings.Repeat("61", 20)} {
		if err := d.GetPeers(hash); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(d.transactionManager.queryChan); got != d.K {
		t.Fatalf("queued %d queries, want %d", got, d.K)
	}
	target := newBitmapFromString(strings.Repeat("a", 20))
	for _, want := range d.routingTable.GetNeighbors(target, d.K) {
		got := <-d.transactionManager.queryChan
		if got.node != want || got.data["a"].(map[string]interface{})["info_hash"] != target.RawString() {
			t.Fatal("queued query does not match nearest peer or decoded hash")
		}
	}
}

func TestQueryWorkersBoundConcurrency(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	d := &DHT{
		InitParam: &InitParam{Try: 1, QueryWorkerLimit: 2, QueryTimeout: 2 * time.Second},
		conn:      conn,
		blackList: newBlackList(100),
	}
	tm := newTransactionManager(math.MaxUint32, d)
	for i := 0; i < 6; i++ {
		tm.queryChan <- &query{
			node: &node{addr: server.LocalAddr().(*net.UDPAddr)},
			data: makeQuery(fmt.Sprint(i), fmt.Sprintf("query-%d", i), map[string]interface{}{}),
		}
	}
	close(tm.queryChan)
	done := make(chan struct{})
	go func() {
		tm.run()
		close(done)
	}()
	defer func() { <-done }()

	buf := make([]byte, 1024)
	readTransaction := func() *transaction {
		t.Helper()
		server.SetReadDeadline(time.Now().Add(3 * time.Second))
		n, _, err := server.ReadFromUDP(buf)
		if err != nil {
			t.Fatal(err)
		}
		value, err := Decode(buf[:n])
		if err != nil {
			t.Fatal(err)
		}
		id := value.(map[string]interface{})["t"].(string)
		trans := tm.getByTransID(id)
		if trans == nil {
			t.Fatal("missing transaction")
		}
		return trans
	}
	first, second := readTransaction(), readTransaction()
	if got := tm.len(); got != 2 {
		t.Fatalf("active queries = %d, want 2", got)
	}
	server.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if _, _, err := server.ReadFromUDP(buf); err == nil {
		t.Fatal("query exceeded worker limit")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal(err)
	}
	first.response <- struct{}{}
	second.response <- struct{}{}
	for i := 0; i < 4; i++ {
		readTransaction().response <- struct{}{}
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("workers did not drain queued queries")
	}
	if got := tm.len(); got != 0 {
		t.Fatalf("transactions remaining = %d", got)
	}
}

func TestQueryRetriesAndExpires(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	d := &DHT{InitParam: &InitParam{Try: 2, QueryTimeout: 20 * time.Millisecond}, conn: conn, blackList: newBlackList(10)}
	tm := newTransactionManager(math.MaxUint32, d)
	done := make(chan struct{})
	go func() {
		tm.query(&query{node: &node{addr: server.LocalAddr().(*net.UDPAddr)}, data: makeQuery("retry", pingType, nil)}, 2)
		close(done)
	}()
	server.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1024)
	for i := 0; i < 2; i++ {
		if _, _, err := server.ReadFromUDP(buf); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("query did not expire")
	}
	if tm.len() != 0 {
		t.Fatal("expired transaction was not removed")
	}
}

func TestSendErrorDoesNotBlacklistIP(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	d := &DHT{conn: conn, blackList: newBlackList(10)}
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6881}
	if err := send(d, addr, makeQuery("failed", pingType, nil)); err == nil {
		t.Fatal("closed socket unexpectedly accepted packet")
	}
	if d.blackList.in(addr.IP.String(), addr.Port) {
		t.Fatal("local send failure blacklisted remote IP")
	}
}

func TestTransactionIndexRejectsDuplicates(t *testing.T) {
	d := &DHT{InitParam: &InitParam{Try: 1}}
	tm := newTransactionManager(math.MaxUint32, d)
	no := &node{addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6881}}
	first := tm.newTransaction("first", &query{node: no, data: makeQuery("first", pingType, nil)})
	second := tm.newTransaction("second", &query{node: no, data: makeQuery("second", pingType, nil)})
	if !tm.insert(first) || tm.insert(second) {
		t.Fatal("duplicate active query was not rejected")
	}
	tm.delete(first.id)
	if !tm.insert(second) {
		t.Fatal("query could not be retried after completion")
	}
}

func TestWireDeduplicatesActivePeer(t *testing.T) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	wire := NewWire(10, 10, 2)
	port := listener.Addr().(*net.TCPAddr).Port
	wire.Request(make([]byte, 20), "127.0.0.1", port)
	wire.Request(make([]byte, 20), "127.0.0.1", port)
	done := make(chan struct{})
	go func() {
		wire.Run()
		close(done)
	}()
	listener.SetDeadline(time.Now().Add(time.Second))
	conn, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	listener.SetDeadline(time.Now().Add(50 * time.Millisecond))
	if duplicate, err := listener.AcceptTCP(); err == nil {
		duplicate.Close()
		t.Fatal("duplicate TCP metadata connection")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal(err)
	}
	close(wire.requests)
	<-done
	conn.Close()
	deadline := time.Now().Add(time.Second)
	for wire.queue.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("active peer was not removed after disconnect")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestQueuedQueriesDeduplicateAndRollback(t *testing.T) {
	d := &DHT{InitParam: &InitParam{Try: 1}, blackList: newBlackList(10)}
	tm := newTransactionManager(math.MaxUint32, d)
	tm.queryChan = make(chan *query, 2)
	first := &node{addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6881}}
	second := &node{addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6882}}
	third := &node{addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6883}}
	var workers sync.WaitGroup
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			tm.sendQuery(first, pingType, nil)
		}()
	}
	workers.Wait()
	tm.sendQuery(second, pingType, nil)
	if len(tm.queryChan) != 2 || len(tm.pending) != 2 {
		t.Fatal("duplicate queries consumed queue capacity")
	}
	tm.sendQuery(third, pingType, nil)
	if len(tm.pending) != 2 {
		t.Fatal("full queue retained pending reservation")
	}
	<-tm.queryChan
	tm.sendQuery(third, pingType, nil)
	if len(tm.queryChan) != 2 || len(tm.pending) != 3 {
		t.Fatal("queue overflow prevented later retry")
	}
}

func TestWireQueuedRequestsDeduplicateAndRollback(t *testing.T) {
	wire := NewWire(10, 2, 1)
	var workers sync.WaitGroup
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			wire.Request(make([]byte, 20), "127.0.0.1", 6881)
		}()
	}
	workers.Wait()
	wire.Request(make([]byte, 20), "127.0.0.1", 6882)
	wire.Request(make([]byte, 20), "127.0.0.1", 6883)
	if len(wire.requests) != 2 || wire.queue.Len() != 2 {
		t.Fatal("duplicate or rejected requests retained queue capacity")
	}
	first := <-wire.requests
	wire.Request(make([]byte, 20), "127.0.0.1", 6881)
	if len(wire.requests) != 1 {
		t.Fatal("active reservation did not block duplicate")
	}
	wire.Request(make([]byte, 20), "127.0.0.1", 6883)
	if len(wire.requests) != 2 {
		t.Fatal("overflow reservation was not released")
	}
	wire.queue.Delete(string(first.InfoHash) + ":" + genAddress(first.IP, first.Port))
	<-wire.requests
	wire.Request(first.InfoHash, first.IP, first.Port)
	if len(wire.requests) != 2 {
		t.Fatal("completed request could not be queued again")
	}
}

func TestWireFullQueueDoesNotBlock(t *testing.T) {
	wire := NewWire(10, 1, 1)
	wire.Request(make([]byte, 20), "127.0.0.1", 6881)
	done := make(chan struct{})
	go func() {
		wire.Request(make([]byte, 20), "127.0.0.1", 6882)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full metadata queue blocked DHT packet handler")
	}
	if len(wire.requests) != 1 {
		t.Fatal("queue capacity was not respected")
	}
}
