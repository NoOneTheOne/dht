package dht

import (
	"net"
	"sync"
	"testing"
	"time"
)

func invariantNode(first byte, port int) *node {
	id := make([]byte, 20)
	id[0] = first
	return &node{id: newBitmapFromBytes(id), addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}, lastActiveTime: time.Now()}
}

func invariantTable(k int) *routingTable {
	d := &DHT{InitParam: &InitParam{}, node: invariantNode(0, 1), blackList: newBlackList(100)}
	d.MaxNodes = 100
	d.NodeExpireAfter = 24 * time.Hour
	return newRoutingTable(k, d)
}

func assertRoutingIndexes(t *testing.T, rt *routingTable) {
	t.Helper()
	rt.RLock()
	defer rt.RUnlock()
	count := 0
	for e := range rt.cachedKBuckets.Iter() {
		b := e.Value.(*KBucket)
		if b.nodes.Len() > rt.k || b.candidates.Len() > rt.k {
			t.Error("unbounded bucket")
		}
		for _, q := range []*keyedDeque{b.nodes, b.candidates} {
			for item := range q.Iter() {
				n := item.Value.(*node)
				if indexed, ok := q.Get(n.id.RawString()); !ok || indexed != item {
					t.Error("missing deque index")
				}
				if q == b.nodes {
					count++
					if cached, ok := rt.cachedNodes.Get(n.addr.String()); !ok || cached != n {
						t.Error("missing address index")
					}
					if found, bucket := rt.getNodeKBucketByIDLocked(n.id); found != n || bucket != b {
						t.Error("missing ID lookup")
					}
				}
			}
		}
	}
	if count != rt.cachedNodes.Len() {
		t.Errorf("bucket count %d differs from cache %d", count, rt.cachedNodes.Len())
	}
}

func TestRoutingInvariantSplit(t *testing.T) {
	tree := newRoutingTableNode(newBitmap(0))
	for _, first := range []byte{0, 128} {
		n := invariantNode(first, int(first)+1)
		tree.bucket.nodes.Push(n.id.RawString(), n)
		tree.bucket.candidates.Push(n.id.RawString(), n)
	}
	tree.Split()
	for i := 0; i < 2; i++ {
		b := tree.Child(i).KBucket()
		id := invariantNode(byte(i*128), 1).id.RawString()
		for _, q := range []*keyedDeque{b.nodes, b.candidates} {
			if !q.HasKey(id) || q.Delete(id) == nil || q.Len() != 0 {
				t.Fatal("split lost keyed index")
			}
		}
	}
}

func TestRoutingInvariantPromotionAndBound(t *testing.T) {
	for _, evict := range []bool{false, true} {
		rt := invariantTable(1)
		a, b := invariantNode(128, 2), invariantNode(192, 3)
		if evict {
			a.lastActiveTime = time.Now().Add(-time.Hour)
		}
		rt.Insert(a)
		rt.Insert(b)
		rt.Insert(b)
		if rt.cachedKBuckets.Len() != 2 {
			t.Fatal("nonlocal bucket split")
		}
		assertRoutingIndexes(t, rt)
		if evict {
			if rt.EvictStaleNodes(time.Minute) != 1 {
				t.Fatal("eviction count")
			}
		} else {
			rt.Remove(a.id)
		}
		if got, _ := rt.GetNodeByAddress(b.addr.String()); got != b {
			t.Fatal("candidate not cached")
		}
		assertRoutingIndexes(t, rt)
		rt.Remove(b.id)
		if rt.Len() != 0 {
			t.Fatal("promoted node not removable")
		}
	}
}

func TestRoutingInvariantRefreshAndAliases(t *testing.T) {
	rt := invariantTable(2)
	rt.dht.MaxNodes = 1
	a := invariantNode(128, 2)
	rt.Insert(a)
	refreshed := invariantNode(128, 3)
	if rt.Insert(refreshed) {
		t.Fatal("refresh reported new")
	}
	if _, ok := rt.GetNodeByAddress(a.addr.String()); ok {
		t.Fatal("old address retained")
	}
	if got, _ := rt.GetNodeByAddress(refreshed.addr.String()); got != refreshed {
		t.Fatal("full table rejected refresh")
	}
	changed := invariantNode(192, 3)
	rt.Insert(changed)
	if n, _ := rt.GetNodeKBucketByID(a.id); n != nil {
		t.Fatal("old ID retained")
	}
	assertRoutingIndexes(t, rt)
	rt.RemoveByAddr(changed.addr.String())
	if rt.Len() != 0 {
		t.Fatal("address removal failed")
	}
}

func TestRoutingInvariantConcurrentMutation(t *testing.T) {
	rt := invariantTable(4)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				n := invariantNode(byte(128+g), g+10)
				rt.Insert(n)
				rt.GetNodeKBucketByID(n.id)
				if i%2 == 0 {
					rt.Remove(n.id)
				} else {
					rt.RemoveByAddr(n.addr.String())
				}
			}
		}(g)
	}
	wg.Wait()
	assertRoutingIndexes(t, rt)
}
