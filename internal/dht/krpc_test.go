package dht

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
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

func TestTokenReuseExpiryAndInvalidAttempt(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	addr := remote.LocalAddr().(*net.UDPAddr)
	tm := d.tokenManager
	now := time.Now()
	tm.now = func() time.Time { return now }
	tm.rotatedAt = now
	tokenString := tm.token(addr)
	if tm.check(addr, "wrong") {
		t.Fatal("invalid token accepted")
	}
	other := &net.UDPAddr{IP: net.IPv4(8, 8, 8, 8), Port: addr.Port}
	if tm.check(other, tokenString) {
		t.Fatal("token accepted for another IP")
	}
	for i := 0; i < 3; i++ {
		if !tm.check(addr, tokenString) {
			t.Fatal("valid token consumed by another announce")
		}
	}
	// Issued just before a rotation, the token must survive the full window.
	now = now.Add(tm.expiredAfter - time.Second)
	issued := tm.token(addr)
	now = now.Add(tm.expiredAfter - time.Second)
	if !tm.check(addr, issued) {
		t.Fatal("token rejected before expiredAfter elapsed")
	}
	now = now.Add(2 * tm.expiredAfter)
	if tm.check(addr, issued) || tm.check(addr, tokenString) {
		t.Fatal("expired token accepted")
	}
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
				if token == "" || !d.tokenManager.check(addr, token) {
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
	for _, mode := range []string{"StandardMode", "CrawlMode"} {
		t.Run(mode, func(t *testing.T) {
			d, remote := newKRPCTestDHT(t)
			d.Mode = mode
			addr := remote.LocalAddr().(*net.UDPAddr)
			trans := addKRPCTestTransaction(t, d, addr, getPeersType)
			valid := newPeer(net.IPv4(127, 0, 0, 2), 6881, "token").CompactIPPortInfo()
			zeroPort := newPeer(net.IPv4(127, 0, 0, 3), 0, "token").CompactIPPortInfo()
			calls := 0
			d.OnGetPeersResponse = func(hash string, p *Peer) {
				calls++
				if hash != strings.Repeat("h", 20) || p.Port != 6881 || p.Token != "token" || p.CompactIPPortInfo() != valid {
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
			if calls != 1 {
				t.Fatalf("peer callback count = %d, want 1", calls)
			}
			if mode == "CrawlMode" {
				if len(peers) != 0 || d.peersManager.table.Len() != 0 {
					t.Fatal("crawl mode cached response peers")
				}
			} else if len(peers) != 1 || peers[0].CompactIPPortInfo() != valid {
				t.Fatal("invalid values inserted or valid peer skipped")
			}
			if len(trans.response) != 1 {
				t.Fatal("transaction not notified")
			}
		})
	}
}

func TestKRPCFindOnFiltersRecommendedEndpoints(t *testing.T) {
	for _, mode := range []string{"StandardMode", "CrawlMode"} {
		for _, queryType := range []string{findNodeType, getPeersType} {
			for _, endpoint := range []string{
				"10.0.0.1:6881", "172.16.0.1:6881", "192.168.1.1:6881",
				"127.0.0.1:6881", "169.254.1.1:6881", "100.64.0.1:6881",
				"192.0.2.1:6881", "0.0.0.0:6881", "224.0.0.1:6881", "8.8.8.8:0",
				"8.8.8.8:6881",
			} {
				for _, withNeighbor := range []bool{false, true} {
					name := mode + "/" + queryType + "/" + endpoint + "/target-only"
					if withNeighbor {
						name = mode + "/" + queryType + "/" + endpoint + "/with-neighbor"
					}
					t.Run(name, func(t *testing.T) {
						d, remote := newKRPCTestDHT(t)
						d.Mode = mode
						addr := remote.LocalAddr().(*net.UDPAddr)
						trans := addKRPCTestTransaction(t, d, addr, queryType)
						target, err := newNode(strings.Repeat("h", 20), "udp4", endpoint)
						if err != nil {
							t.Fatal(err)
						}
						neighbor, err := newNode(strings.Repeat("n", 20), "udp4", "1.1.1.1:6881")
						if err != nil {
							t.Fatal(err)
						}
						compact := target.CompactNodeInfo()
						if withNeighbor {
							compact += neighbor.CompactNodeInfo()
						}
						r := map[string]interface{}{
							"id": strings.Repeat("r", 20), "token": "token", "nodes": compact,
						}
						if !handleResponse(d, addr, makeResponse(trans.id, r)) {
							t.Fatal("valid compact response rejected")
						}
						publicTarget := endpoint == "8.8.8.8:6881"
						gotTarget, _ := d.routingTable.GetNodeKBucketByID(target.id)
						if (gotTarget != nil) != publicTarget {
							t.Fatal("recommended target endpoint filtering failed")
						}
						gotNeighbor, _ := d.routingTable.GetNodeKBucketByID(neighbor.id)
						if (gotNeighbor != nil) != withNeighbor {
							t.Fatal("public neighbor not retained")
						}
						// Directly contacted loopback responders must remain usable.
						responder, _ := d.routingTable.GetNodeKBucketByID(trans.node.id)
						if responder == nil || responder.addr.String() != addr.String() {
							t.Fatal("direct local responder was filtered")
						}
						wantNodes := 1
						if publicTarget {
							wantNodes++
						}
						if withNeighbor {
							wantNodes++
						}
						if d.routingTable.Len() != wantNodes {
							t.Fatalf("routing table size = %d, want %d", d.routingTable.Len(), wantNodes)
						}
						wantQueries := 0
						if withNeighbor && !publicTarget {
							wantQueries = 1
						}
						if len(d.transactionManager.queryChan) != wantQueries {
							t.Fatalf("queued queries = %d, want %d", len(d.transactionManager.queryChan), wantQueries)
						}
						if wantQueries != 0 {
							q := <-d.transactionManager.queryChan
							if q.node.addr.String() != neighbor.addr.String() || q.data["q"] != queryType {
								t.Fatal("query did not continue to the public neighbor")
							}
							key := "target"
							if queryType == getPeersType {
								key = "info_hash"
							}
							if q.data["a"].(map[string]interface{})[key] != target.id.RawString() {
								t.Fatal("continued query changed its target")
							}
						}
					})
				}
			}
		}
	}
}

func TestKRPCFindOnRejectsInvalidCompactNodes(t *testing.T) {
	compact := strings.Repeat("n", 20) + "\x08\x08\x08\x08\x1a\xe1"
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
					if success != (i == 0) {
						t.Fatal("transaction must accept only its first valid reply")
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
			if len(trans.response) != 1 {
				t.Fatal("transaction received more than one completion")
			}
		})
	}
}

func TestKRPCConcurrentResponseCompletesOnce(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	addr := remote.LocalAddr().(*net.UDPAddr)
	trans := addKRPCTestTransaction(t, d, addr, getPeersType)
	var calls atomic.Int32
	d.OnGetPeersResponse = func(string, *Peer) { calls.Add(1) }
	r := makeResponse(trans.id, map[string]interface{}{
		"id": strings.Repeat("r", 20), "token": "token",
		"values": []interface{}{newPeer(net.IPv4(127, 0, 0, 2), 6881, "").CompactIPPortInfo()},
	})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Go(func() { handleResponse(d, addr, r) })
	}
	wg.Wait()
	if calls.Load() != 1 || len(trans.response) != 1 {
		t.Fatal("duplicate response repeated callback or completion")
	}
}

func TestKRPCErrorValidationAndAssociation(t *testing.T) {
	for _, value := range []interface{}{
		"bad", []interface{}{}, []interface{}{201}, []interface{}{201, 1}, []interface{}{"201", "bad"},
	} {
		d, remote := newKRPCTestDHT(t)
		addr := remote.LocalAddr().(*net.UDPAddr)
		trans := addKRPCTestTransaction(t, d, addr, pingType)
		message := makeError(trans.id, 201, "error")
		message["e"] = value
		if handleError(d, addr, message) || len(trans.response) != 0 {
			t.Fatalf("accepted malformed error: %v", value)
		}
		if handleError(d, addr, makeError("unknown", 201, "error")) {
			t.Fatal("accepted unsolicited error")
		}
		wrong := &net.UDPAddr{IP: addr.IP, Port: addr.Port + 1}
		if handleError(d, wrong, makeError(trans.id, 201, "error")) || len(trans.response) != 0 {
			t.Fatal("accepted error from wrong endpoint")
		}
		if !handleError(d, addr, makeError(trans.id, 201, "error")) || len(trans.response) != 1 {
			t.Fatal("rejected associated valid error")
		}
	}
}

func TestKRPCGetPeersRejectsMalformedValues(t *testing.T) {
	d, remote := newKRPCTestDHT(t)
	addr := remote.LocalAddr().(*net.UDPAddr)
	trans := addKRPCTestTransaction(t, d, addr, getPeersType)
	r := map[string]interface{}{"id": strings.Repeat("r", 20), "token": "token", "values": "bad", "nodes": ""}
	if handleResponse(d, addr, makeResponse(trans.id, r)) || len(trans.response) != 0 || d.routingTable.Len() != 0 {
		t.Fatal("malformed values bypassed validation through nodes fallback")
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

func TestCrawlModeAnswersWithNodes(t *testing.T) {
	for _, queryType := range []string{findNodeType, getPeersType} {
		t.Run(queryType, func(t *testing.T) {
			d, remote := newKRPCTestDHT(t)
			d.Mode = "CrawlMode"
			known, err := newNode(strings.Repeat("k", 20), "udp4", "8.8.8.8:6881")
			if err != nil || !d.routingTable.Insert(known) {
				t.Fatal("could not seed routing table")
			}
			a := map[string]interface{}{"id": strings.Repeat("r", 20)}
			if queryType == findNodeType {
				a["target"] = strings.Repeat("t", 20)
			} else {
				a["info_hash"] = strings.Repeat("h", 20)
			}
			addr := remote.LocalAddr().(*net.UDPAddr)
			if !handleRequest(d, addr, makeQuery("cq", queryType, a)) {
				t.Fatal("crawl-mode request rejected")
			}
			message := readKRPCTestMessage(t, remote)
			r, ok := message["r"].(map[string]interface{})
			if message["t"] != "cq" || !ok {
				t.Fatalf("expected response, got %v", message)
			}
			if nodes, _ := r["nodes"].(string); nodes != known.CompactNodeInfo() {
				t.Fatalf("nodes = %q, want the known contact", nodes)
			}
			target, _ := a["target"].(string)
			if queryType == getPeersType {
				target = a["info_hash"].(string)
			}
			// Crawl mode impersonates a neighbor of the requested target.
			if id, _ := r["id"].(string); len(id) != 20 || id[:15] != target[:15] {
				t.Fatalf("responder id %q is not a neighbor of %q", id, target)
			}

		})
	}
}
