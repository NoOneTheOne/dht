package dht

import (
	"bytes"
	"net"
	"slices"
	"testing"
)

func BenchmarkGetNeighbors5000(b *testing.B) {
	d := &DHT{InitParam: &InitParam{MaxNodes: 5000, K: 8}, node: invariantNode(0, 1), blackList: newBlackList(16)}
	rt := newRoutingTable(1<<30, d)
	d.routingTable = rt
	for i := 0; rt.Len() < 5000; i++ {
		id := []byte(randomString(20))
		n := &node{id: newBitmapFromBytes(id), addr: &net.UDPAddr{IP: net.IPv4(8, byte(i>>16), byte(i>>8), byte(i)), Port: 6881}}
		rt.Insert(n)
	}
	target := newBitmapFromString(randomString(20))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(rt.GetNeighbors(target, 8)) != 8 {
			b.Fatal("short result")
		}
	}
}

func TestGetTopKNodesMatchesSort(t *testing.T) {
	for round := 0; round < 50; round++ {
		nodes := make([]*node, 300)
		for i := range nodes {
			id := []byte(randomString(20))
			if i%3 == 0 {
				id[0] = 0x42 // Force ties on the leading word's top byte.
			}
			nodes[i] = &node{id: newBitmapFromBytes(id)}
		}
		target := newBitmapFromString(randomString(20))
		target.data[0] = 0x42
		want := slices.Clone(nodes)
		slices.SortFunc(want, func(a, b *node) int {
			return bytes.Compare(a.id.Xor(target).data, b.id.Xor(target).data)
		})
		got := getTopKNodes(nodes, target, 8)
		if len(got) != 8 {
			t.Fatalf("got %d nodes", len(got))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("round %d position %d differs from sorted order", round, i)
			}
		}
	}
}
