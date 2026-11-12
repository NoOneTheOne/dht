package dht

import (
	"net"
	"strings"
	"testing"
	"time"
)

// Construct only the state these handlers need: no default config, New, init,
// bootstrap workers, or external address discovery.
func newKRPCTestDHT(t *testing.T) (*DHT, *net.UDPConn) {
	t.Helper()
	listen := func() *net.UDPConn {
		conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	conn, remote := listen(), listen()
	d := &DHT{
		InitParam: &InitParam{
			K: 8, KBucketSize: 8, MaxNodes: 64, Network: "udp4",
			Mode: "StandardMode", Try: 1,
		},
		node:      &node{id: newBitmapFromString(strings.Repeat("s", 20))},
		conn:      conn,
		blackList: newBlackList(64),
	}
	d.routingTable = newRoutingTable(d.KBucketSize, d)
	d.peersManager = newPeersManager(d)
	d.tokenManager = newTokenManager(time.Minute, d)
	d.transactionManager = newTransactionManager(65536, d)
	return d, remote
}

func readKRPCTestMessage(t *testing.T, conn *net.UDPConn) map[string]interface{} {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	message, err := parseMessage(decoded)
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestKRPCAnnounceRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value interface{}
	}{
		{"implied-string", "implied_port", "1"},
		{"implied-list", "implied_port", []interface{}{1}},
		{"implied-map", "implied_port", map[string]interface{}{}},
		{"hash-int", "info_hash", 1},
		{"hash-empty", "info_hash", ""},
		{"hash-short", "info_hash", strings.Repeat("h", 19)},
		{"hash-long", "info_hash", strings.Repeat("h", 21)},
		{"port-string", "port", "6881"},
		{"port-negative", "port", -1},
		{"port-zero", "port", 0},
		{"port-overflow", "port", 65536},
	}
	for _, mode := range []string{"StandardMode", "CrawlMode"} {
		for _, tt := range tests {
			t.Run(mode+"/"+tt.name, func(t *testing.T) {
				d, remote := newKRPCTestDHT(t)
				d.Mode = mode
				addr := remote.LocalAddr().(*net.UDPAddr)
				token := d.tokenManager.token(addr)
				if token == "" || !d.tokenManager.Has(addr.IP.String()) {
					t.Fatal("missing local token")
				}
				called := false
				d.OnAnnouncePeer = func(string, string, int) { called = true }
				a := map[string]interface{}{
					"id": strings.Repeat("r", 20), "info_hash": strings.Repeat("h", 20),
					"port": 6881, "token": token,
				}
				a[tt.key] = tt.value
				if handleRequest(d, addr, makeQuery("bad", announcePeerType, a)) {
					t.Fatal("invalid announce accepted")
				}
				message := readKRPCTestMessage(t, remote)
				if message["t"] != "bad" || message["y"] != "e" {
					t.Fatalf("expected protocol error, got %v", message)
				}
				if e := message["e"].([]interface{}); e[0] != protocolError {
					t.Fatalf("error code = %v, want %d", e[0], protocolError)
				}
				if called || d.peersManager.table.Len() != 0 || d.routingTable.Len() != 0 {
					t.Fatal("invalid announce affected callback, peers, or routing table")
				}
			})
		}
	}
}

func TestKRPCAnnounceEffectivePort(t *testing.T) {
	tests := []struct {
		name    string
		port    int
		implied interface{}
	}{
		{"minimum", 1, nil},
		{"maximum", 65535, nil},
		{"explicit", 6881, 0},
		{"implied", 0, 1},
		{"implied-nonzero", 65536, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, remote := newKRPCTestDHT(t)
			addr := remote.LocalAddr().(*net.UDPAddr)
			token := d.tokenManager.token(addr)
			hash := strings.Repeat("h", 20)
			a := map[string]interface{}{
				"id": strings.Repeat("r", 20), "info_hash": hash,
				"port": tt.port, "token": token,
			}
			wantPort := tt.port
			if tt.implied != nil {
				a["implied_port"] = tt.implied
				if tt.implied.(int) != 0 {
					wantPort = addr.Port
				}
			}
			calls := 0
			d.OnAnnouncePeer = func(gotHash, ip string, port int) {
				calls++
				if gotHash != hash || ip != addr.IP.String() || port != wantPort {
					t.Errorf("unexpected announce callback: %q %s:%d", gotHash, ip, port)
				}
			}
			if !handleRequest(d, addr, makeQuery("good", announcePeerType, a)) {
				t.Fatal("valid announce rejected")
			}
			if message := readKRPCTestMessage(t, remote); message["y"] != "r" {
				t.Fatalf("expected response, got %v", message)
			}
			peers := d.peersManager.GetPeers(hash, 8)
			if calls != 1 || len(peers) != 1 || peers[0].Port != wantPort || d.routingTable.Len() != 1 {
				t.Fatal("valid announce did not update callback, peer, and routing table")
			}
		})
	}
}

func addKRPCTestTransaction(t *testing.T, d *DHT, addr *net.UDPAddr, queryType string) *transaction {
	t.Helper()
	q := &query{
		node: &node{id: newBitmapFromString(strings.Repeat("r", 20)), addr: addr},
		data: makeQuery("tx", queryType, map[string]interface{}{
			"info_hash": strings.Repeat("h", 20), "target": strings.Repeat("h", 20),
		}),
	}
	trans := d.transactionManager.newTransaction("tx", q)
	if !d.transactionManager.insert(trans) {
		t.Fatal("could not insert test transaction")
	}
	return trans
}

func TestKRPCGetPeersMixedValues(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	addr := remote.LocalAddr().(*net.UDPAddr)
	trans := addKRPCTestTransaction(t, d, addr, getPeersType)
	valid := newPeer(net.IPv4(127, 0, 0, 2), 6881, "token").CompactIPPortInfo()
	zeroPort := newPeer(net.IPv4(127, 0, 0, 3), 0, "token").CompactIPPortInfo()
	calls := 0
	d.OnGetPeersResponse = func(hash string, p *Peer) {
		calls++
		if hash != strings.Repeat("h", 20) || p.Port != 6881 || p.Token != "token" {
			t.Errorf("unexpected peer callback: %q %+v", hash, p)
		}
	}
	r := map[string]interface{}{
		"id": strings.Repeat("r", 20), "token": "token",
		"values": []interface{}{1, []interface{}{}, map[string]interface{}{}, "short", zeroPort, valid, 2},
	}
	if !handleResponse(d, addr, makeResponse(trans.id, r)) {
		t.Fatal("mixed values response rejected instead of skipping invalid peers")
	}
	peers := d.peersManager.GetPeers(strings.Repeat("h", 20), 8)
	if calls != 1 || len(peers) != 1 || peers[0].CompactIPPortInfo() != valid {
		t.Fatal("invalid values inserted or valid peer skipped")
	}
	if len(trans.response) != 1 {
		t.Fatal("transaction not notified")
	}
}

func TestKRPCFindOnRejectsInvalidCompactNodes(t *testing.T) {
	compact := strings.Repeat("n", 20) + "\x7f\x00\x00\x01\x1a\xe1"
	tests := []struct {
		name    string
		network string
		nodes   interface{}
	}{
		{"type", "udp4", 1},
		{"length", "udp4", compact[:25]},
		// A correctly sized IPv4 entry still fails newNode when its network
		// cannot be resolved. The parser error must not become a nil dereference.
		{"constructor-error", "invalid-network", compact},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, remote := newKRPCTestDHT(t)
			d.Network = tt.network
			r := map[string]interface{}{"nodes": tt.nodes}
			if err := findOn(d, r, newBitmapFromString(strings.Repeat("h", 20)), findNodeType); err == nil {
				t.Fatal("invalid compact nodes accepted")
			}
			addr := remote.LocalAddr().(*net.UDPAddr)
			trans := addKRPCTestTransaction(t, d, addr, findNodeType)
			r["id"] = strings.Repeat("r", 20)
			if handleResponse(d, addr, makeResponse(trans.id, r)) {
				t.Fatal("response with invalid compact nodes accepted")
			}
			if d.routingTable.Len() != 0 || len(trans.response) != 0 || len(d.transactionManager.queryChan) != 0 {
				t.Fatal("failed parsing inserted nodes, notified transaction, or queued queries")
			}
		})
	}
}

func TestKRPCRequestNodeConstructionError(t *testing.T) {
	d, _ := newKRPCTestDHT(t)
	// Crawl mode does not send a find_node reply, so this isolates the final
	// newNode call without trying to send to an invalid UDP destination.
	d.Mode = "CrawlMode"
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65536}
	if handleRequest(d, addr, makeQuery("bad-address", findNodeType, map[string]interface{}{
		"id": strings.Repeat("r", 20),
	})) {
		t.Fatal("request with unresolvable node address accepted")
	}
	if d.routingTable.Len() != 0 {
		t.Fatal("failed node construction modified routing table")
	}
}

func TestKRPCDuplicateNotificationsDoNotBlock(t *testing.T) {
	for _, kind := range []string{"response", "error", "mixed"} {
		t.Run(kind, func(t *testing.T) {
			d, remote := newKRPCTestDHT(t)
			addr := remote.LocalAddr().(*net.UDPAddr)
			trans := addKRPCTestTransaction(t, d, addr, pingType)
			// Keep the transaction registered and do not consume notifications.
			// Start full, then deliver enough duplicates to exceed retry capacity.
			for i := 0; i < cap(trans.response); i++ {
				trans.response <- struct{}{}
			}
			for i := 0; i < cap(trans.response)+3; i++ {
				done := make(chan bool, 1)
				go func(isError bool) {
					if isError {
						done <- handleError(d, addr, makeError(trans.id, protocolError, "bad request"))
					} else {
						done <- handleResponse(d, addr, makeResponse(trans.id, map[string]interface{}{
							"id": strings.Repeat("r", 20),
						}))
					}
				}(kind == "error" || kind == "mixed" && i%2 == 1)
				select {
				case success := <-done:
					if !success {
						t.Fatal("duplicate was rejected")
					}
				case <-time.After(time.Second):
					// Unblock a regressed blocking send before failing the test.
					<-trans.response
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Fatal("handler did not finish after notification was drained")
					}
					t.Fatal("duplicate notification blocked on a full channel")
				}
			}
			if len(trans.response) != cap(trans.response) {
				t.Fatal("duplicate notifications changed full channel state")
			}
		})
	}
}

func TestKRPCResponseExistingValidation(t *testing.T) {
	for _, r := range []interface{}{
		"not-a-map",
		map[string]interface{}{},
		map[string]interface{}{"id": 1},
		map[string]interface{}{"id": strings.Repeat("r", 19)},
		map[string]interface{}{"id": strings.Repeat("r", 21)},
	} {
		d, remote := newKRPCTestDHT(t)
		addr := remote.LocalAddr().(*net.UDPAddr)
		trans := addKRPCTestTransaction(t, d, addr, pingType)
		trans.node.id = nil // Exercise newNode's ID length validation, not mismatch handling.
		response := makeResponse(trans.id, nil)
		response["r"] = r
		if handleResponse(d, addr, response) {
			t.Fatalf("invalid response accepted: %v", r)
		}
		if d.routingTable.Len() != 0 || len(trans.response) != 0 {
			t.Fatal("invalid response modified routing table or notified transaction")
		}
	}
}
