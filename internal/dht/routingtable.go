package dht

import (
	"cmp"
	"container/heap"
	"container/list"
	"encoding/binary"
	"errors"
	"net"
	"slices"
	"sync"
	"time"
)

// maxPrefixLength is the length of DHT node.
const maxPrefixLength = 160

// node represents a DHT node.
type node struct {
	id             *bitmap
	addr           *net.UDPAddr
	lastActiveTime time.Time
}

// newNode returns a node pointer.
func newNode(id, network, address string) (*node, error) {
	if len(id) != 20 {
		return nil, errors.New("node id should be a 20-length string")
	}

	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	return &node{newBitmapFromString(id), addr, time.Now()}, nil
}

// newNodeFromCompactInfo parses compactNodeInfo and returns a node pointer.
func newNodeFromCompactInfo(
	compactNodeInfo string, network string) (*node, error) {

	if len(compactNodeInfo) != 26 {
		return nil, errors.New("compactNodeInfo should be a 26-length string")
	}

	id := compactNodeInfo[:20]
	ip, port, _ := decodeCompactIPPortInfo(compactNodeInfo[20:])

	return newNode(id, network, genAddress(ip.String(), port))
}

// CompactIPPortInfo returns "Compact IP-address/port info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (node *node) CompactIPPortInfo() string {
	info, _ := encodeCompactIPPortInfo(node.addr.IP, node.addr.Port)
	return info
}

// CompactNodeInfo returns "Compact node info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (node *node) CompactNodeInfo() string {
	return node.id.RawString() + node.CompactIPPortInfo()
}

// Peer represents a peer contact.
type Peer struct {
	IP    net.IP
	Port  int
	Token string
}

// newPeer returns a new peer pointer.
func newPeer(ip net.IP, port int, token string) *Peer {
	return &Peer{
		IP:    ip,
		Port:  port,
		Token: token,
	}
}

// newPeerFromCompactIPPortInfo create a peer pointer by compact ip/port info.
func newPeerFromCompactIPPortInfo(compactInfo, token string) (*Peer, error) {
	ip, port, err := decodeCompactIPPortInfo(compactInfo)
	if err != nil {
		return nil, err
	}

	return newPeer(ip, port, token), nil
}

// CompactIPPortInfo returns "Compact node info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (p *Peer) CompactIPPortInfo() string {
	info, _ := encodeCompactIPPortInfo(p.IP, p.Port)
	return info
}

// KBucket represents a k-size bucket.
type KBucket struct {
	sync.RWMutex
	nodes, candidates *keyedDeque
	lastChanged       time.Time
	prefix            *bitmap
}

// newKBucket returns a new KBucket pointer.
func newKBucket(prefix *bitmap) *KBucket {
	bucket := &KBucket{
		nodes:       newKeyedDeque(),
		candidates:  newKeyedDeque(),
		lastChanged: time.Now(),
		prefix:      prefix,
	}
	return bucket
}

// LastChanged return the last time when it changes.
func (bucket *KBucket) LastChanged() time.Time {
	bucket.RLock()
	defer bucket.RUnlock()

	return bucket.lastChanged
}

// RandomChildID returns a random id that has the same prefix with bucket.
func (bucket *KBucket) RandomChildID() string {
	id := []byte(randomString(20))
	whole, bits := bucket.prefix.Size/8, bucket.prefix.Size%8
	copy(id[:whole], bucket.prefix.data[:whole])
	if bits != 0 {
		mask := byte(0xff << (8 - bits))
		id[whole] = id[whole]&^mask | bucket.prefix.data[whole]&mask
	}
	return string(id)
}

// UpdateTimestamp update bucket's last changed time.
func (bucket *KBucket) UpdateTimestamp() {
	bucket.Lock()
	defer bucket.Unlock()

	bucket.lastChanged = time.Now()
}

// Insert inserts node to the bucket. It returns whether the node is new in
// the bucket.
func (bucket *KBucket) Insert(no *node) bool {
	isNew := !bucket.nodes.HasKey(no.id.RawString())

	bucket.nodes.Push(no.id.RawString(), no)
	bucket.UpdateTimestamp()

	return isNew
}

// Replace removes node, then put bucket.candidates.Back() to the right
// place of bucket.nodes.
func (bucket *KBucket) Replace(no *node) *node {
	if bucket.nodes.Delete(no.id.RawString()) == nil {
		return nil
	}
	bucket.UpdateTimestamp()
	if bucket.candidates.Len() == 0 {
		return nil
	}
	promoted := bucket.candidates.Remove(bucket.candidates.Back()).(*node)
	// Snapshot before keyed reordering invalidates the list elements.
	var entries []*list.Element
	for e := range bucket.nodes.Iter() {
		entries = append(entries, e)
	}
	bucket.nodes.Push(promoted.id.RawString(), promoted)
	// Reappend newer entries to preserve timestamp order using keyed operations.
	for _, e := range entries {
		n := e.Value.(*node)
		if n.lastActiveTime.After(promoted.lastActiveTime) {
			bucket.nodes.Push(n.id.RawString(), n)
		}
	}
	return promoted
}

// Fresh pings the expired nodes in the bucket.
func (bucket *KBucket) Fresh(dht *DHT) {
	for e := range bucket.nodes.Iter() {
		no := e.Value.(*node)
		if time.Since(no.lastActiveTime) > dht.NodeExpireAfter {
			dht.transactionManager.ping(no)
		}
	}
}

// routingTableNode represents routing table tree node.
type routingTableNode struct {
	sync.RWMutex
	children []*routingTableNode
	bucket   *KBucket
}

// newRoutingTableNode returns a new routingTableNode pointer.
func newRoutingTableNode(prefix *bitmap) *routingTableNode {
	return &routingTableNode{
		children: make([]*routingTableNode, 2),
		bucket:   newKBucket(prefix),
	}
}

// Child returns routingTableNode's left or right child.
func (tableNode *routingTableNode) Child(index int) *routingTableNode {
	if index >= 2 {
		return nil
	}

	tableNode.RLock()
	defer tableNode.RUnlock()

	return tableNode.children[index]
}

// SetChild sets routingTableNode's left or right child. When index is 0, it's
// the left child, if 1, it's the right child.
func (tableNode *routingTableNode) SetChild(index int, c *routingTableNode) {
	tableNode.Lock()
	defer tableNode.Unlock()

	tableNode.children[index] = c
}

// KBucket returns the bucket routingTableNode holds.
func (tableNode *routingTableNode) KBucket() *KBucket {
	tableNode.RLock()
	defer tableNode.RUnlock()

	return tableNode.bucket
}

// SetKBucket sets the bucket.
func (tableNode *routingTableNode) SetKBucket(bucket *KBucket) {
	tableNode.Lock()
	defer tableNode.Unlock()

	tableNode.bucket = bucket
}

// Split splits current routingTableNode and sets it's two children.
func (tableNode *routingTableNode) Split() {
	prefixLen := tableNode.KBucket().prefix.Size

	if prefixLen == maxPrefixLength {
		return
	}

	for i := 0; i < 2; i++ {
		tableNode.SetChild(i, newRoutingTableNode(newBitmapFrom(
			tableNode.KBucket().prefix, prefixLen+1)))
	}

	tableNode.Lock()
	tableNode.children[1].bucket.prefix.Set(prefixLen)
	tableNode.Unlock()

	for e := range tableNode.KBucket().nodes.Iter() {
		nd := e.Value.(*node)
		tableNode.Child(nd.id.Bit(prefixLen)).KBucket().nodes.Push(nd.id.RawString(), nd)
	}

	for e := range tableNode.KBucket().candidates.Iter() {
		nd := e.Value.(*node)
		tableNode.Child(nd.id.Bit(prefixLen)).KBucket().candidates.Push(nd.id.RawString(), nd)
	}

	for i := 0; i < 2; i++ {
		tableNode.Child(i).KBucket().UpdateTimestamp()
	}
}

// routingTable implements the routing table in DHT protocol.
type routingTable struct {
	*sync.RWMutex
	k              int
	root           *routingTableNode
	cachedNodes    *syncedMap
	cachedKBuckets *keyedDeque
	dht            *DHT
	refreshOffset  int
}

// newRoutingTable returns a new routingTable pointer.
func newRoutingTable(k int, dht *DHT) *routingTable {
	root := newRoutingTableNode(newBitmap(0))

	rt := &routingTable{
		RWMutex:        &sync.RWMutex{},
		k:              k,
		root:           root,
		cachedNodes:    newSyncedMap(),
		cachedKBuckets: newKeyedDeque(),
		dht:            dht,
	}

	rt.cachedKBuckets.Push(root.bucket.prefix.String(), root.bucket)
	return rt
}

// Insert adds a node to routing table. It returns whether the node is new
// in the routing table.
func (rt *routingTable) Insert(nd *node) bool {
	rt.Lock()
	defer rt.Unlock()

	if nd.id.RawString() == rt.dht.node.id.RawString() || nd.addr.Port < 1 || nd.addr.Port > 65535 ||
		nd.addr.IP.IsUnspecified() || nd.addr.IP.IsMulticast() || nd.addr.IP.IsLinkLocalUnicast() ||
		rt.dht.blackList.in(nd.addr.IP.String(), nd.addr.Port) {
		return false
	}

	previous, previousBucket := rt.getNodeKBucketByIDLocked(nd.id)
	byAddress, addressExists := rt.cachedNodes.Get(nd.addr.String())
	if previous == nil && !addressExists && rt.cachedNodes.Len() >= rt.dht.MaxNodes {
		return false
	}
	// Reserve the endpoint's slot for its replacement rather than a candidate.
	if addressExists && byAddress.(*node).id.RawString() != nd.id.RawString() {
		old := byAddress.(*node)
		if _, b := rt.getNodeKBucketByIDLocked(old.id); b != nil {
			b.nodes.Delete(old.id.RawString())
			b.UpdateTimestamp()
		}
		rt.cachedNodes.Delete(nd.addr.String())
	}
	if previous != nil {
		rt.cachedNodes.Delete(previous.addr.String())
		previousBucket.Insert(nd)
		rt.cachedNodes.Set(nd.addr.String(), nd)
	}
	// Remove candidate aliases before they can be promoted back into the table.
	rt.cachedKBuckets.syncedList.RLock()
	for e := rt.cachedKBuckets.queue.Front(); e != nil; e = e.Next() {
		b := e.Value.(*KBucket)
		var stale []string
		b.candidates.RLock()
		for _, c := range b.candidates.index {
			n := c.Value.(*node)
			if n.id.RawString() == nd.id.RawString() || n.addr.String() == nd.addr.String() {
				stale = append(stale, n.id.RawString())
			}
		}
		b.candidates.RUnlock()
		for _, id := range stale {
			b.candidates.Delete(id)
		}
	}
	rt.cachedKBuckets.syncedList.RUnlock()
	if previous != nil {
		rt.cachedKBuckets.Push(previousBucket.prefix.String(), previousBucket)
		return false
	}

	var (
		next   *routingTableNode
		bucket *KBucket
	)
	root := rt.root

	for prefixLen := 1; prefixLen <= maxPrefixLength; prefixLen++ {
		next = root.Child(nd.id.Bit(prefixLen - 1))

		if next != nil {
			// If next is not the leaf.
			root = next
		} else if root.KBucket().nodes.Len() < rt.k ||
			root.KBucket().nodes.HasKey(nd.id.RawString()) {

			bucket = root.KBucket()
			isNew := bucket.Insert(nd)

			rt.cachedNodes.Set(nd.addr.String(), nd)
			rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)

			return isNew
		} else if root.KBucket().prefix.Size < maxPrefixLength &&
			root.KBucket().prefix.Compare(rt.dht.node.id, prefixLen-1) == 0 {
			// If node has the same prefix with bucket, split it.

			root.Split()

			rt.cachedKBuckets.Delete(root.KBucket().prefix.String())
			root.SetKBucket(nil)

			for i := 0; i < 2; i++ {
				bucket = root.Child(i).KBucket()
				rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)
			}

			root = root.Child(nd.id.Bit(prefixLen - 1))
		} else {
			// Finally, store node as a candidate and fresh the bucket.
			root.KBucket().candidates.Push(nd.id.RawString(), nd)
			if root.KBucket().candidates.Len() > rt.k {
				root.KBucket().candidates.Remove(
					root.KBucket().candidates.Front())
			}

			refreshBucket := root.KBucket()
			rt.dht.background.Go(func() { refreshBucket.Fresh(rt.dht) })
			return false
		}
	}
	return false
}

// neighborSnapshots recycles the routing-table snapshot GetNeighbors needs on
// every inbound find_node/get_peers.
var neighborSnapshots = sync.Pool{New: func() interface{} { return new([]*node) }}

// InsertMentioned adds a contact learned from another node's reply. Unlike
// Insert it never touches a known ID or endpoint: an unverified mention must
// neither keep a dead contact fresh nor move a known ID to a new address.
func (rt *routingTable) InsertMentioned(nd *node) bool {
	rt.RLock()
	_, known := rt.cachedNodes.Get(nd.addr.String())
	if !known {
		existing, _ := rt.getNodeKBucketByIDLocked(nd.id)
		known = existing != nil
	}
	rt.RUnlock()
	if known {
		return false
	}
	return rt.Insert(nd)
}

// GetNeighbors returns the size-length nodes closest to id.
func (rt *routingTable) GetNeighbors(id *bitmap, size int) []*node {
	if size <= 0 {
		return nil
	}
	buf := neighborSnapshots.Get().(*[]*node)
	nodes := (*buf)[:0]
	rt.cachedNodes.RLock()
	for _, value := range rt.cachedNodes.data {
		nodes = append(nodes, value.(*node))
	}
	rt.cachedNodes.RUnlock()

	result := getTopKNodes(nodes, id, size)
	clear(nodes) // Do not pin evicted nodes through the pool.
	*buf = nodes[:0]
	neighborSnapshots.Put(buf)
	return result
}

// GetNeighborCompactInfos Get Neighbor Ids return the size-length compact node info closest to id.
func (rt *routingTable) GetNeighborCompactInfos(id *bitmap, size int) []string {
	neighbors := rt.GetNeighbors(id, size)
	infos := make([]string, len(neighbors))

	for i, no := range neighbors {
		infos[i] = no.CompactNodeInfo()
	}

	return infos
}

// GetNodeKBucketByID returns node whose id is `id` and the bucket it
// belongs to.
func (rt *routingTable) GetNodeKBucketByID(id *bitmap) (
	nd *node, bucket *KBucket) {

	rt.RLock()
	defer rt.RUnlock()

	return rt.getNodeKBucketByIDLocked(id)
}

// GetNodeByAddress finds node by address.
func (rt *routingTable) GetNodeByAddress(address string) (no *node, ok bool) {
	rt.RLock()
	defer rt.RUnlock()

	v, ok := rt.cachedNodes.Get(address)
	if ok {
		no = v.(*node)
	}
	return
}

// Remove deletes the node whose id is `id`.
func (rt *routingTable) Remove(id *bitmap) {
	rt.Lock()
	defer rt.Unlock()
	rt.removeLocked(id)
}

// removeLocked updates both indexes while the table write lock is held.
func (rt *routingTable) removeLocked(id *bitmap) {
	if nd, bucket := rt.getNodeKBucketByIDLocked(id); nd != nil {
		for e := range bucket.candidates.Iter() {
			candidate := e.Value.(*node)
			if rt.dht.blackList.in(candidate.addr.IP.String(), candidate.addr.Port) {
				bucket.candidates.Delete(candidate.id.RawString())
			}
		}
		promoted := bucket.Replace(nd)
		rt.cachedNodes.Delete(nd.addr.String())
		if promoted != nil {
			rt.cachedNodes.Set(promoted.addr.String(), promoted)
		}
		rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)
	}
}

func (rt *routingTable) removeContact(expected *node) {
	rt.Lock()
	defer rt.Unlock()
	current, ok := rt.cachedNodes.Get(expected.addr.String())
	if !ok || current != expected {
		return
	}
	rt.removeLocked(expected.id)
}

// RemoveByAddr Remove the node whose address is `ip:port`.
func (rt *routingTable) RemoveByAddr(address string) {
	rt.Lock()
	defer rt.Unlock()
	v, ok := rt.cachedNodes.Get(address)
	if ok {
		rt.removeLocked(v.(*node).id)
	}
}

// Len returns the number of nodes in table.
func (rt *routingTable) Len() int {
	rt.RLock()
	defer rt.RUnlock()

	return rt.cachedNodes.Len()
}

// xorDistance is a 160-bit XOR distance split into machine words, so most
// comparisons are decided by a single uint64 instead of a 20-byte compare.
type xorDistance struct {
	hi, mid uint64
	lo      uint32
}

func newXORDistance(target, id []byte) xorDistance {
	return xorDistance{
		hi:  binary.BigEndian.Uint64(target[0:8]) ^ binary.BigEndian.Uint64(id[0:8]),
		mid: binary.BigEndian.Uint64(target[8:16]) ^ binary.BigEndian.Uint64(id[8:16]),
		lo:  binary.BigEndian.Uint32(target[16:20]) ^ binary.BigEndian.Uint32(id[16:20]),
	}
}

func (d xorDistance) compare(other xorDistance) int {
	if c := cmp.Compare(d.hi, other.hi); c != 0 {
		return c
	}
	if c := cmp.Compare(d.mid, other.mid); c != 0 {
		return c
	}
	return cmp.Compare(d.lo, other.lo)
}

type nodeHeapItem struct {
	distance xorDistance
	node     *node
}

// topKNodeHeap is a max-heap: the farthest retained node sits at the root.
type topKNodeHeap []nodeHeapItem

func (h topKNodeHeap) Len() int           { return len(h) }
func (h topKNodeHeap) Less(i, j int) bool { return h[i].distance.compare(h[j].distance) > 0 }
func (h topKNodeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *topKNodeHeap) Push(x interface{}) {
	*h = append(*h, x.(nodeHeapItem))
}

func (h *topKNodeHeap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func getTopKNodes(nodes []*node, id *bitmap, k int) []*node {
	k = min(k, len(nodes))
	if k <= 0 || len(id.data) < 20 {
		return nil
	}
	target := id.data[:20]
	h := make(topKNodeHeap, 0, k)
	for _, n := range nodes {
		if len(n.id.data) < 20 {
			continue
		}
		hi := binary.BigEndian.Uint64(target[0:8]) ^ binary.BigEndian.Uint64(n.id.data[0:8])
		if len(h) == k && hi > h[0].distance.hi {
			continue // Cheap rejection: farther than every retained node.
		}
		distance := newXORDistance(target, n.id.data)
		if len(h) < k {
			h = append(h, nodeHeapItem{distance: distance, node: n})
			if len(h) == k {
				heap.Init(&h)
			}
		} else if distance.compare(h[0].distance) < 0 {
			h[0] = nodeHeapItem{distance: distance, node: n}
			heap.Fix(&h, 0)
		}
	}

	slices.SortFunc(h, func(a, b nodeHeapItem) int {
		return a.distance.compare(b.distance)
	})
	result := make([]*node, len(h))
	for i := range h {
		result[i] = h[i].node
	}
	return result
}
