package dht

import (
	"crypto/hmac"
	"crypto/sha1"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	pingType         = "ping"
	findNodeType     = "find_node"
	getPeersType     = "get_peers"
	announcePeerType = "announce_peer"
)

const (
	generalError = 201 + iota
	serverError
	protocolError
	unknownError
)

// packet represents the information receive from udp.
type packet struct {
	data        []byte
	RealAddress *net.UDPAddr
}

// tokenManager issues stateless announce tokens: a truncated HMAC of the
// requester's IP. The secret rotates every expiredAfter and the previous
// secret is still accepted, so a token stays valid for at least expiredAfter
// (BEP-5 asks for ten minutes) and memory does not grow with requesters.
type tokenManager struct {
	mu           sync.Mutex
	current      []byte
	previous     []byte
	rotatedAt    time.Time
	expiredAfter time.Duration
	now          func() time.Time
}

// newTokenManager returns a new tokenManager.
func newTokenManager(expiredAfter time.Duration, _ *DHT) *tokenManager {
	tm := &tokenManager{expiredAfter: expiredAfter, now: time.Now}
	tm.current = []byte(randomString(20))
	tm.previous = []byte(randomString(20))
	tm.rotatedAt = tm.now()
	return tm
}

// secrets returns the current and previous secrets, rotating as needed.
func (tm *tokenManager) secrets() (current, previous []byte) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	elapsed := tm.now().Sub(tm.rotatedAt)
	if elapsed >= tm.expiredAfter {
		if elapsed >= 2*tm.expiredAfter {
			tm.current = []byte(randomString(20)) // Both secrets are stale.
		}
		tm.previous, tm.current = tm.current, []byte(randomString(20))
		tm.rotatedAt = tm.now()
	}
	return tm.current, tm.previous
}

func tokenFor(secret []byte, addr *net.UDPAddr) string {
	mac := hmac.New(sha1.New, secret)
	mac.Write(addr.IP.To16())
	return string(mac.Sum(nil)[:8])
}

// token returns the token for addr's IP.
func (tm *tokenManager) token(addr *net.UDPAddr) string {
	current, _ := tm.secrets()
	return tokenFor(current, addr)
}

// check returns whether the token is valid for addr's IP.
func (tm *tokenManager) check(addr *net.UDPAddr, tokenString string) bool {
	current, previous := tm.secrets()
	return hmac.Equal([]byte(tokenString), []byte(tokenFor(current, addr))) ||
		hmac.Equal([]byte(tokenString), []byte(tokenFor(previous, addr)))
}

// makeQuery returns a query-formed data.
func makeQuery(t, q string, a map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"t": t,
		"y": "q",
		"q": q,
		"a": a,
	}
}

// makeResponse returns a response-formed data.
func makeResponse(t string, r map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"t": t,
		"y": "r",
		"r": r,
	}
}

// makeError returns a err-formed data.
func makeError(t string, errCode int, errMsg string) map[string]interface{} {
	return map[string]interface{}{
		"t": t,
		"y": "e",
		"e": []interface{}{errCode, errMsg},
	}
}

// send data to the udp.
func send(dht *DHT, addr *net.UDPAddr, data map[string]interface{}) error {
	if dht.reachability != nil {
		dht.reachability.recordSend(addr)
	}
	dht.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := dht.conn.WriteToUDP([]byte(Encode(data)), addr)
	if err == nil {
		RecordPacketSent()
	}
	return err
}

// query represents the query data included queried node and query-formed data.
type query struct {
	node        *node
	data        map[string]interface{}
	lookup      *peerLookup
	resultNodes []*node
}

// transaction implements transaction.
type transaction struct {
	*query
	id           string
	response     chan struct{}
	mu           sync.Mutex
	completed    bool
	remoteError  bool
	verifiedNode *node
}

// transactionManager represents the manager of transactions.
type transactionManager struct {
	*sync.RWMutex
	transactions *syncedMap
	index        *syncedMap
	cursor       uint64
	maxCursor    uint64
	queryChan    chan *query
	pending      map[string]struct{}
	dht          *DHT
}

// newTransactionManager returns new transactionManager pointer.
func newTransactionManager(maxCursor uint64, dht *DHT) *transactionManager {
	if maxCursor == 0 {
		maxCursor = 1
	}
	return &transactionManager{
		RWMutex:      &sync.RWMutex{},
		transactions: newSyncedMap(),
		index:        newSyncedMap(),
		maxCursor:    maxCursor,
		queryChan:    make(chan *query, 1024),
		pending:      make(map[string]struct{}),
		dht:          dht,
	}
}

// genTransID generates a transaction id and returns it.
func (tm *transactionManager) genTransID() string {
	tm.Lock()
	defer tm.Unlock()

	tm.cursor = (tm.cursor + 1) % tm.maxCursor
	return string(int2bytes(tm.cursor))
}

// newTransaction creates a new transaction.
func (tm *transactionManager) newTransaction(id string, q *query) *transaction {
	return &transaction{
		id:       id,
		query:    q,
		response: make(chan struct{}, tm.dht.Try+1),
	}
}

// genIndexKey generates an indexed key which consists of queryType and
// address.
func (tm *transactionManager) genIndexKey(queryType, address string) string {
	return queryType + ":" + address
}

// genIndexKeyByTrans generates an indexed key by a transaction.
func (tm *transactionManager) genIndexKeyByTrans(trans *transaction) string {
	return tm.genIndexKey(trans.data["q"].(string), trans.node.addr.String())
}

// insert adds a transaction to transactionManager.
func (tm *transactionManager) insert(trans *transaction) bool {
	tm.Lock()
	defer tm.Unlock()

	key := tm.genIndexKeyByTrans(trans)
	if tm.index.Has(key) || tm.transactions.Has(trans.id) {
		return false
	}
	tm.transactions.Set(trans.id, trans)
	tm.index.Set(key, trans)
	return true
}

// delete removes a transaction from transactionManager.
func (tm *transactionManager) delete(transID string) {
	tm.Lock()
	defer tm.Unlock()

	v, ok := tm.transactions.Get(transID)
	if !ok {
		return
	}

	trans := v.(*transaction)
	tm.transactions.Delete(trans.id)
	tm.index.Delete(tm.genIndexKeyByTrans(trans))
}

// len returns how many transactions are requesting now.
func (tm *transactionManager) len() int {
	return tm.transactions.Len()
}

// transaction returns a transaction. keyType should be one of 0, 1 which
// represents transId and index each.
func (tm *transactionManager) transaction(
	key string, keyType int) *transaction {

	sm := tm.transactions
	if keyType == 1 {
		sm = tm.index
	}

	v, ok := sm.Get(key)
	if !ok {
		return nil
	}

	return v.(*transaction)
}

// getByTransID returns a transaction by transID.
func (tm *transactionManager) getByTransID(transID string) *transaction {
	return tm.transaction(transID, 0)
}

// getByIndex returns a transaction by indexed key.
func (tm *transactionManager) getByIndex(index string) *transaction {
	return tm.transaction(index, 1)
}

// transaction gets the proper transaction with whose id is transId and
// address is addr.
func (tm *transactionManager) filterOne(
	transID string, addr *net.UDPAddr) *transaction {

	trans := tm.getByTransID(transID)
	if trans == nil || trans.node.addr.String() != addr.String() {
		return nil
	}

	return trans
}

// query sends the query-formed data to udp and wait for the response.
// When timeout, it will retry `try - 1` times, which means it will query
// `try` times totally.
func (tm *transactionManager) query(q *query, try int) {
	if q.lookup != nil && !time.Now().Before(q.lookup.deadline) {
		return
	}
	started := time.Now()
	transID := q.data["t"].(string)
	trans := tm.newTransaction(transID, q)

	if !tm.insert(trans) {
		return
	}
	outcome := "stopped"
	defer func() {
		trans.mu.Lock()
		if outcome == "reply" {
			outcome = "success"
			if trans.remoteError {
				outcome = "remote_error"
			}
		}
		trans.completed = true
		verified := trans.verifiedNode
		trans.mu.Unlock()
		tm.delete(trans.id)
		if tm.dht.verifiedNodes != nil {
			if outcome == "success" && verified != nil {
				tm.dht.verifiedNodes.recordSuccess(verified, time.Since(started))
			} else if outcome != "stopped" {
				tm.dht.verifiedNodes.recordFailure(q.node)
			}
		}
		RecordProtocolEvent("query", outcome)
	}()

	for i := 0; i < try; i++ {
		select {
		case <-tm.dht.stop:
			return
		default:
		}
		if q.lookup != nil && !time.Now().Before(q.lookup.deadline) {
			outcome = "timeout"
			return
		}
		if err := send(tm.dht, q.node.addr, q.data); err != nil {
			outcome = "send_error"
			return
		}
		wait := tm.dht.QueryTimeout
		if q.lookup != nil {
			wait = min(wait, max(time.Duration(0), time.Until(q.lookup.deadline)))
		}
		timer := time.NewTimer(wait)
		select {
		case <-trans.response:
			outcome = "reply"
			timer.Stop()
			return
		case <-tm.dht.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	trans.mu.Lock()
	defer trans.mu.Unlock()
	outcome = "timeout"
	if trans.completed {
		outcome = "reply"
	}
	if !trans.completed && q.node.id != nil {
		tm.dht.routingTable.removeContact(q.node, false)
	}
	trans.completed = true
}

// run starts to listen and consume the query chan.
func (tm *transactionManager) run() {
	var workers sync.WaitGroup
	for i := 0; i < tm.dht.QueryWorkerLimit; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-tm.dht.stop:
					return
				default:
				}
				var q *query
				select {
				case <-tm.dht.stop:
					return
				case value, ok := <-tm.queryChan:
					if !ok {
						return
					}
					q = value
				}
				if !tm.dht.blackList.in(q.node.addr.IP.String(), q.node.addr.Port) {
					tm.query(q, tm.dht.Try)
				}
				tm.Lock()
				delete(tm.pending, tm.genIndexKey(q.data["q"].(string), q.node.addr.String()))
				tm.Unlock()
				if q.lookup != nil {
					q.lookup.complete(q)
				}
			}
		}()
	}
	workers.Wait()
	tm.Lock()
	defer tm.Unlock()
	clear(tm.pending)
	for {
		select {
		case _, ok := <-tm.queryChan:
			if !ok {
				return
			}
			RecordProtocolEvent("query_queue", "stopped")
		default:
			return
		}
	}
}

// sendQuery send query-formed data to the chan.
func (tm *transactionManager) sendQuery(
	no *node, queryType string, a map[string]interface{}) bool {
	return tm.queueQuery(no, queryType, a, nil)
}

func (tm *transactionManager) queueQuery(no *node, queryType string, a map[string]interface{}, lookup *peerLookup) bool {

	// If the target is self, then stop.
	if no.id != nil && no.id.RawString() == tm.dht.node.id.RawString() ||
		tm.getByIndex(tm.genIndexKey(queryType, no.addr.String())) != nil ||
		tm.dht.blackList.in(no.addr.IP.String(), no.addr.Port) {
		return false
	}

	data := makeQuery(tm.genTransID(), queryType, a)
	q := &query{
		node:   no,
		data:   data,
		lookup: lookup,
	}

	key := tm.genIndexKey(queryType, no.addr.String())
	tm.Lock()
	defer tm.Unlock()
	select {
	case <-tm.dht.stop:
		RecordProtocolEvent("query_queue", "stopped")
		return false
	default:
	}
	if _, exists := tm.pending[key]; exists {
		return false
	}
	tm.pending[key] = struct{}{}
	select {
	case tm.queryChan <- q:
		return true
	default:
		delete(tm.pending, key)
		RecordProtocolEvent("query_queue", "full")
		return false
	}
}

// ping sends ping query to the chan.
func (tm *transactionManager) ping(no *node) {
	tm.sendQuery(no, pingType, map[string]interface{}{
		"id": tm.dht.id(no.id.RawString()),
	})
}

// findNode sends find_node query to the chan.
func (tm *transactionManager) findNode(no *node, target string) {
	tm.sendQuery(no, findNodeType, map[string]interface{}{
		"id":     tm.dht.id(target),
		"target": target,
	})
}

// getPeers sends get_peers query to the chan.
func (tm *transactionManager) getPeers(no *node, infoHash string) bool {
	return tm.sendQuery(no, getPeersType, map[string]interface{}{
		"id":        tm.dht.id(infoHash),
		"info_hash": infoHash,
	})
}

// ParseKey parses the key in dict data. `t` is type of the keyed value.
// It's one of "int", "string", "map", "list".
func ParseKey(data map[string]interface{}, key string, t string) error {
	val, ok := data[key]
	if !ok {
		return errors.New("lack of key")
	}

	switch t {
	case "string":
		_, ok = val.(string)
	case "int":
		_, ok = val.(int)
	case "map":
		_, ok = val.(map[string]interface{})
	case "list":
		_, ok = val.([]interface{})
	default:
		panic("invalid type")
	}

	if !ok {
		return errors.New("invalid key type")
	}

	return nil
}

// ParseKeys parses keys. It just wraps ParseKey.
func ParseKeys(data map[string]interface{}, pairs [][]string) error {
	for _, args := range pairs {
		key, t := args[0], args[1]
		if err := ParseKey(data, key, t); err != nil {
			return err
		}
	}
	return nil
}

// parseMessage parses the basic data received from udp.
// It returns a map value.
func parseMessage(data interface{}) (map[string]interface{}, error) {
	response, ok := data.(map[string]interface{})
	if !ok {
		return nil, errors.New("response is not dict")
	}

	if err := ParseKeys(
		response, [][]string{{"t", "string"}, {"y", "string"}}); err != nil {
		return nil, err
	}

	return response, nil
}

// handleRequest handles the requests received from udp.
func handleRequest(dht *DHT, addr *net.UDPAddr,
	response map[string]interface{}) (success bool) {

	t := response["t"].(string)

	if err := ParseKeys(
		response, [][]string{{"q", "string"}, {"a", "map"}}); err != nil {

		send(dht, addr, makeError(t, protocolError, err.Error()))
		return
	}

	q := response["q"].(string)
	a := response["a"].(map[string]interface{})
	unsolicited := dht.reachability != nil && dht.reachability.unsolicited(addr)
	defer func() {
		if success && unsolicited {
			dht.reachability.recordInbound(addr)
		}
	}()

	if err := ParseKey(a, "id", "string"); err != nil {
		send(dht, addr, makeError(t, protocolError, err.Error()))
		return
	}

	id := a["id"].(string)

	if id == dht.node.id.RawString() {
		return
	}

	if len(id) != 20 {
		send(dht, addr, makeError(t, protocolError, "invalid id"))
		return
	}

	// The endpoint now presents another ID: it restarted, or our entry came
	// from an unverified third-party mention. Drop the stale contact rather
	// than blacklisting the real sender, whose requests carry infohashes.
	if no, ok := dht.routingTable.GetNodeByAddress(addr.String()); ok &&
		no.id.RawString() != id {
		dht.routingTable.removeContact(no, false)
	}

	switch q {
	case pingType:
		send(dht, addr, makeResponse(t, map[string]interface{}{
			"id": dht.id(id),
		}))
	case findNodeType:
		if err := ParseKey(a, "target", "string"); err != nil {
			send(dht, addr, makeError(t, protocolError, err.Error()))
			return
		}
		target := a["target"].(string)
		if len(target) != 20 {
			send(dht, addr, makeError(t, protocolError, "invalid target"))
			return
		}
		// Crawl mode answers too: nodes that reply stay in remote routing
		// tables, which is what attracts get_peers/announce_peer traffic.
		var nodes string
		targetID := newBitmapFromString(target)

		no, _ := dht.routingTable.GetNodeKBucketByID(targetID)
		if no != nil {
			nodes = no.CompactNodeInfo()
		} else {
			nodes = strings.Join(
				dht.routingTable.GetNeighborCompactInfos(targetID, dht.K),
				"",
			)
		}

		send(dht, addr, makeResponse(t, map[string]interface{}{
			"id":    dht.id(target),
			"nodes": nodes,
		}))
	case getPeersType:
		if err := ParseKey(a, "info_hash", "string"); err != nil {
			send(dht, addr, makeError(t, protocolError, err.Error()))
			return
		}

		infoHash := a["info_hash"].(string)

		if len(infoHash) != 20 {
			send(dht, addr, makeError(t, protocolError, "invalid info_hash"))
			return
		}

		// Crawl mode never caches peers, so it always takes the nodes branch.
		if peers := dht.peersManager.GetPeers(
			infoHash, dht.K); len(peers) > 0 {

			values := make([]interface{}, len(peers))
			for i, p := range peers {
				values[i] = p.CompactIPPortInfo()
			}

			send(dht, addr, makeResponse(t, map[string]interface{}{
				"id":     dht.id(infoHash),
				"values": values,
				"token":  dht.tokenManager.token(addr),
			}))
		} else {
			send(dht, addr, makeResponse(t, map[string]interface{}{
				"id":    dht.id(infoHash),
				"token": dht.tokenManager.token(addr),
				"nodes": strings.Join(dht.routingTable.GetNeighborCompactInfos(
					newBitmapFromString(infoHash), dht.K), ""),
			}))
		}

		if dht.OnGetPeers != nil {
			dht.OnGetPeers(infoHash, addr.IP.String(), addr.Port)
		}
	case announcePeerType:
		if err := ParseKeys(a, [][]string{
			{"info_hash", "string"},
			{"port", "int"},
			{"token", "string"}}); err != nil {

			send(dht, addr, makeError(t, protocolError, err.Error()))
			return
		}

		infoHash := a["info_hash"].(string)
		port := a["port"].(int)
		token := a["token"].(string)

		if !dht.tokenManager.check(addr, token) {
			//			send(dht, addr, makeError(t, protocolError, "invalid token"))
			return
		}

		if len(infoHash) != 20 {
			send(dht, addr, makeError(t, protocolError, "invalid info_hash"))
			return
		}

		if _, ok := a["implied_port"]; ok {
			if err := ParseKey(a, "implied_port", "int"); err != nil {
				send(dht, addr, makeError(t, protocolError, err.Error()))
				return
			}
			if a["implied_port"].(int) != 0 {
				port = addr.Port
			}
		}

		if port < 1 || port > 65535 {
			send(dht, addr, makeError(t, protocolError, "invalid port"))
			return
		}

		// Peers are cached only in standard mode, but both modes must reply:
		// an unanswered announce counts as a timeout and gets us evicted.
		dht.peersManager.Insert(infoHash, newPeer(addr.IP, port, token))
		// Reply under the same ID our get_peers answer used for this hash.
		send(dht, addr, makeResponse(t, map[string]interface{}{
			"id": dht.id(infoHash),
		}))

		if dht.OnAnnouncePeer != nil {
			dht.OnAnnouncePeer(infoHash, addr.IP.String(), port)
		}
	default:
		//		send(dht, addr, makeError(t, protocolError, "invalid q"))
		return
	}

	no, err := newNode(id, addr.Network(), addr.String())
	if err != nil {
		return
	}
	dht.routingTable.Insert(no)
	return true
}

// findOn puts nodes in the response to the routingTable, then if target is in
// the nodes or all nodes are in the routingTable, it stops. Otherwise,
// it continues to findNode or getPeers.
func findOn(dht *DHT, r map[string]interface{}, target *bitmap,
	queryType string) error {

	if err := ParseKey(r, "nodes", "string"); err != nil {
		return err
	}

	nodes := r["nodes"].(string)
	if len(nodes)%26 != 0 {
		return errors.New("the length of nodes should can be divided by 26")
	}

	hasNew, found := false, false
	for i := 0; i < len(nodes)/26; i++ {
		no, err := newNodeFromCompactInfo(nodes[i*26:(i+1)*26], dht.Network)
		if err != nil {
			return err
		}
		// Third-party recommendations must not route queries to non-public endpoints.
		if _, _, public := verifiedNodeEndpoint(no.addr.String()); !public {
			continue
		}

		if no.id.RawString() == target.RawString() {
			found = true
		}

		if dht.routingTable.InsertMentioned(no) {
			hasNew = true
		}
	}

	if found || !hasNew {
		return nil
	}

	targetID := target.RawString()
	for _, no := range dht.routingTable.GetNeighbors(target, dht.K) {
		switch queryType {
		case findNodeType:
			dht.transactionManager.findNode(no, targetID)
		case getPeersType:
			dht.transactionManager.getPeers(no, targetID)
		default:
			panic("invalid find type")
		}
	}
	return nil
}

// handleResponse handles responses received from udp.
func handleResponse(dht *DHT, addr *net.UDPAddr,
	response map[string]interface{}) (success bool) {

	t := response["t"].(string)

	trans := dht.transactionManager.filterOne(t, addr)
	if trans == nil {
		return
	}
	trans.mu.Lock()
	defer trans.mu.Unlock()
	if trans.completed {
		return
	}

	if err := ParseKey(response, "r", "map"); err != nil {
		return
	}

	q := trans.data["q"].(string)
	a := trans.data["a"].(map[string]interface{})
	r := response["r"].(map[string]interface{})

	if q == findNodeType {
		if err := ParseKey(a, "target", "string"); err != nil || len(a["target"].(string)) != 20 {
			return
		}
	}

	if err := ParseKey(r, "id", "string"); err != nil {
		return
	}

	id := r["id"].(string)
	if len(id) != 20 || id == dht.node.id.RawString() {
		return
	}

	// The queried endpoint answered our transaction under a new ID (e.g. a
	// restart). Forget the old identity; the reply itself is still valid.
	// removeContact checks pointer identity, so a contact refreshed by
	// another query is left alone.
	if trans.node.id != nil && trans.node.id.RawString() != id {
		dht.routingTable.removeContact(trans.node, false)
	}

	node, err := newNode(id, addr.Network(), addr.String())
	if err != nil {
		return
	}

	switch q {
	case pingType:
	case findNodeType:
		if trans.data["q"].(string) != findNodeType {
			return
		}

		target := trans.data["a"].(map[string]interface{})["target"].(string)
		if findOn(dht, r, newBitmapFromString(target), findNodeType) != nil {
			return
		}
	case getPeersType:
		if err := ParseKey(r, "token", "string"); err != nil {
			return
		}

		if err := ParseKey(a, "info_hash", "string"); err != nil || len(a["info_hash"].(string)) != 20 {
			return
		}
		token := r["token"].(string)
		infoHash := a["info_hash"].(string)
		// Validate every field before mutating any state.
		if value, exists := r["values"]; exists {
			if _, ok := value.([]interface{}); !ok {
				return
			}
		}

		if trans.lookup != nil {
			if value, exists := r["nodes"]; exists {
				compact, ok := value.(string)
				if !ok || len(compact)%26 != 0 {
					return
				}
				for offset := 0; offset < len(compact) && len(trans.resultNodes) < peerLookupCandidates; offset += 26 {
					n, err := newNodeFromCompactInfo(compact[offset:offset+26], dht.Network)
					if err == nil {
						if _, _, public := verifiedNodeEndpoint(n.addr.String()); !public {
							continue
						}
						trans.resultNodes = append(trans.resultNodes, n)
						dht.routingTable.InsertMentioned(n)
					}
				}
			}
		}
		if _, exists := r["values"]; exists {
			values := r["values"].([]interface{})
			for _, v := range values {
				compactInfo, ok := v.(string)
				if !ok {
					continue
				}
				p, err := newPeerFromCompactIPPortInfo(compactInfo, token)
				if err != nil || p.Port == 0 {
					continue
				}
				dht.peersManager.Insert(infoHash, p)
				if dht.OnGetPeersResponse != nil {
					dht.OnGetPeersResponse(infoHash, p)
				}
			}
		} else if trans.lookup == nil && findOn(
			dht, r, newBitmapFromString(infoHash), getPeersType) != nil {
			return
		}
	case announcePeerType:
	default:
		return
	}

	trans.verifiedNode = node
	trans.completed = true
	select {
	case trans.response <- struct{}{}:
	default:
	}

	if dht.reachability != nil {
		dht.reachability.recordResponse()
	}
	dht.blackList.delete(addr.IP.String(), addr.Port)
	dht.routingTable.Insert(node)

	return true
}

// handleError handles errors received from udp.
func handleError(dht *DHT, addr *net.UDPAddr,
	response map[string]interface{}) (success bool) {

	if err := ParseKey(response, "e", "list"); err != nil {
		return
	}

	e := response["e"].([]interface{})
	if len(e) != 2 {
		return
	}
	if _, ok := e[0].(int); !ok {
		return
	}
	if _, ok := e[1].(string); !ok {
		return
	}

	trans := dht.transactionManager.filterOne(response["t"].(string), addr)
	if trans == nil {
		return
	}
	trans.mu.Lock()
	defer trans.mu.Unlock()
	if trans.completed {
		return
	}
	trans.remoteError = true
	trans.completed = true
	select {
	case trans.response <- struct{}{}:
	default:
	}
	return true
}

var handlers = map[string]func(*DHT, *net.UDPAddr, map[string]interface{}) bool{
	"q": handleRequest,
	"r": handleResponse,
	"e": handleError,
}

// packetWorker consumes the packet queue until the DHT stops. A fixed pool
// lets the queue absorb bursts instead of dropping packets whenever every
// worker is momentarily busy, and avoids a goroutine per packet.
func (dht *DHT) packetWorker() {
	for {
		select {
		case <-dht.stop:
			return
		case pkt := <-dht.packets:
			dht.activePacketWorkers.Add(1)
			handle(dht, pkt)
			dht.activePacketWorkers.Add(-1)
		}
	}
}

// handle handles a packet received from udp on the calling goroutine.
func handle(dht *DHT, pkt packet) {
	if dht.blackList.in(pkt.RealAddress.IP.String(), pkt.RealAddress.Port) {
		return
	}

	data, err := Decode(pkt.data)
	if err != nil {
		RecordProtocolEvent("packet", "decode_error")
		return
	}

	response, err := parseMessage(data)
	if err != nil {
		RecordProtocolEvent("packet", "invalid_envelope")
		return
	}

	if f, ok := handlers[response["y"].(string)]; ok {
		if !f(dht, pkt.RealAddress, response) {
			RecordProtocolEvent("packet", "rejected")
		}
	} else {
		RecordProtocolEvent("packet", "unknown_type")
	}
}
