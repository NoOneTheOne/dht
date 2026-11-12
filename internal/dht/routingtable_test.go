package dht

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"
)

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
