package dht

// blackList holds addresses the node never talks to: its own interface and
// public addresses (IP-only entries, port < 0). Entries are permanent.
type blackList struct {
	list    *syncedMap
	maxSize int
}

// newBlackList returns a blackList pointer.
func newBlackList(size int) *blackList {
	return &blackList{list: newSyncedMap(), maxSize: size}
}

// genKey returns a key. If port is less than 0, the key wil be ip. Otherwise,
// it will be `ip:port` format.
func (bl *blackList) genKey(ip string, port int) string {
	key := ip
	if port >= 0 {
		key = genAddress(ip, port)
	}
	return key
}

// insert adds a blocked item to the blacklist.
func (bl *blackList) insert(ip string, port int) {
	if bl.list.Len() >= bl.maxSize {
		return
	}
	bl.list.Set(bl.genKey(ip, port), struct{}{})
}

// in reports whether the ip, or the ip-port pair, is blocked.
func (bl *blackList) in(ip string, port int) bool {
	if _, ok := bl.list.Get(ip); ok {
		return true
	}
	_, ok := bl.list.Get(bl.genKey(ip, port))
	return ok
}

// len returns the number of items in the blacklist.
func (bl *blackList) len() int {
	return bl.list.Len()
}
