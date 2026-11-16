package dht

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"testing"
)

func TestPeersManagerCrawlModeDoesNotCache(t *testing.T) {
	d := &DHT{InitParam: &InitParam{Mode: "CrawlMode", K: 8}}
	pm := newPeersManager(d)
	for i := 0; i < 4097; i++ {
		pm.Insert(fmt.Sprintf("%020d", i), newPeer(net.IPv4(8, 8, 8, 8), 6881, ""))
	}
	if pm.table.Len() != 0 || len(pm.GetPeers(fmt.Sprintf("%020d", 0), 8)) != 0 {
		t.Fatal("crawl mode retained peers")
	}
}

func TestPeersManagerHashCapacityAndLRU(t *testing.T) {
	for _, access := range []string{"read", "write"} {
		t.Run(access, func(t *testing.T) {
			d := &DHT{InitParam: &InitParam{Mode: "StandardMode", K: 8}}
			pm := newPeersManager(d)
			peer := newPeer(net.IPv4(8, 8, 8, 8), 6881, "")
			hash := func(i int) string { return fmt.Sprintf("%020d", i) }
			for i := 0; i < 4096; i++ {
				pm.Insert(hash(i), peer)
			}
			if access == "read" {
				if got := pm.GetPeers(hash(0), 8); len(got) != 1 || got[0] != peer {
					t.Fatal("cached peers not returned")
				}
			} else {
				pm.Insert(hash(0), peer)
			}
			pm.GetPeers("missing", 8)
			pm.Insert(hash(4096), peer)
			if pm.table.Len() != 4096 {
				t.Fatalf("hash count = %d, want 4096", pm.table.Len())
			}
			if len(pm.GetPeers(hash(1), 8)) != 0 {
				t.Fatal("least recently used hash was not evicted")
			}
			for _, i := range []int{0, 2, 4095, 4096} {
				if got := pm.GetPeers(hash(i), 8); len(got) != 1 || got[0] != peer {
					t.Fatalf("hash %d lost its cached peer", i)
				}
			}
		})
	}
}

func TestPeersManagerPerHashCapacity(t *testing.T) {
	d := &DHT{InitParam: &InitParam{Mode: "StandardMode", K: 2}}
	pm := newPeersManager(d)
	first := newPeer(net.IPv4(8, 8, 8, 8), 6881, "old")
	second := newPeer(net.IPv4(8, 8, 4, 4), 6881, "")
	updated := newPeer(first.IP, first.Port, "new")
	third := newPeer(net.IPv4(1, 1, 1, 1), 6881, "")
	for _, p := range []*Peer{first, second, updated, third} {
		pm.Insert("hash", p)
	}
	peers := pm.GetPeers("hash", 8)
	if len(peers) != 2 || peers[0] != updated || peers[1] != third {
		t.Fatal("per-hash eviction or duplicate peer refresh regressed")
	}
	if got := pm.GetPeers("hash", 1); len(got) != 1 || got[0] != third {
		t.Fatal("GetPeers did not honor requested size")
	}
}

func TestPeersManagerConcurrentAccess(t *testing.T) {
	d := &DHT{InitParam: &InitParam{Mode: "StandardMode", K: 2}}
	pm := newPeersManager(d)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for i := 0; i < 1024; i++ {
				hash := fmt.Sprintf("%020d", worker*1024+i)
				pm.Insert(hash, newPeer(net.IPv4(8, 8, 8, 8), 6881, ""))
				pm.GetPeers(hash, 2)
				pm.GetPeers(fmt.Sprintf("%020d", i), 2)
			}
		})
	}
	wg.Wait()
	if pm.table.Len() != 4096 {
		t.Fatalf("concurrent hash count = %d, want 4096", pm.table.Len())
	}
}

func nearestTestNodes(size int) []*node {
	rng := rand.New(rand.NewSource(1))
	nodes := make([]*node, size)
	for i := range nodes {
		id := make([]byte, 20)
		_, _ = rng.Read(id)
		binary.BigEndian.PutUint32(id[16:], uint32(i))
		nodes[i] = &node{id: newBitmapFromBytes(id)}
	}
	return nodes
}

func TestGetTopKNodes(t *testing.T) {
	nodes := nearestTestNodes(100)
	target := newBitmapFromBytes(bytes.Repeat([]byte{0x59}, 20))
	want := append([]*node(nil), nodes...)
	sort.Slice(want, func(i, j int) bool {
		return want[i].id.Xor(target).Compare(want[j].id.Xor(target), 160) < 0
	})
	for _, k := range []int{1, 2, 8, 100, 200} {
		got := getTopKNodes(nodes, target, k)
		size := min(k, len(nodes))
		if len(got) != size {
			t.Fatalf("k=%d: got %d nodes, want %d", k, len(got), size)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("k=%d: node %d is not the nearest node", k, i)
			}
		}
	}
}

func TestGetTopKNodesEmpty(t *testing.T) {
	for _, k := range []int{-1, 0, 1} {
		if got := getTopKNodes(nil, newBitmap(160), k); len(got) != 0 {
			t.Fatalf("k=%d: expected empty result", k)
		}
	}
	for _, k := range []int{-1, 0} {
		if got := getTopKNodes(nearestTestNodes(1), newBitmap(160), k); len(got) != 0 {
			t.Fatalf("k=%d: expected empty result", k)
		}
	}
}

func BenchmarkGetTopKNodes(b *testing.B) {
	nodes := nearestTestNodes(5000)
	target := newBitmap(160)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		getTopKNodes(nodes, target, 8)
	}
}
