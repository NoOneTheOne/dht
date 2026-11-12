package dht

import (
	"sync"
	"testing"
)

func TestKeyedDequeRefreshKeepsIndexesBounded(t *testing.T) {
	deque := newKeyedDeque()
	deque.Push("key", 0)
	stale, _ := deque.Get("key")
	for i := 1; i <= 1000; i++ {
		deque.Push("key", i)
	}
	if deque.Len() != 1 || len(deque.index) != 1 || len(deque.invertedIndex) != 1 {
		t.Fatalf("unbounded indexes: list=%d index=%d inverted=%d", deque.Len(), len(deque.index), len(deque.invertedIndex))
	}
	if deque.Remove(stale) != nil {
		t.Fatal("stale element removed the replacement")
	}
	current, ok := deque.Get("key")
	if !ok || current.Value != 1000 {
		t.Fatal("current element lost after stale removal")
	}
	if deque.Remove(current) != 1000 || deque.Len() != 0 || len(deque.invertedIndex) != 0 {
		t.Fatal("removal did not clear indexes")
	}
}

func TestSyncedMapBasic(t *testing.T) {
	m := newSyncedMap()

	m.Set("key1", "val1")
	m.Set("key2", "val2")

	if m.Len() != 2 {
		t.Errorf("Len() = %d, want 2", m.Len())
	}

	v, ok := m.Get("key1")
	if !ok || v.(string) != "val1" {
		t.Errorf("Get(key1) = %v, %v", v, ok)
	}

	if !m.Has("key2") {
		t.Error("Has(key2) should be true")
	}

	m.Delete("key1")
	if m.Has("key1") {
		t.Error("key1 should be deleted")
	}
	if m.Len() != 1 {
		t.Errorf("Len() after delete = %d, want 1", m.Len())
	}
}

func TestSyncedMapIter(t *testing.T) {
	m := newSyncedMap()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	collected := make(map[interface{}]interface{})
	for item := range m.Iter() {
		collected[item.key] = item.val
	}

	if len(collected) != 3 {
		t.Errorf("Iter collected %d items, want 3", len(collected))
	}
	if collected["a"].(int) != 1 {
		t.Errorf("Iter a = %v, want 1", collected["a"])
	}
}

func TestSyncedMapIterNoDeadlock(t *testing.T) {
	m := newSyncedMap()
	for i := 0; i < 100; i++ {
		m.Set(i, i*2)
	}

	// Consume iter and modify map concurrently — should not deadlock
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range m.Iter() {
			// Simulate work that might write to the map
			m.Set("new", "value")
		}
	}()
	wg.Wait()
}

func TestSyncedMapDeleteMulti(t *testing.T) {
	m := newSyncedMap()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	m.DeleteMulti([]interface{}{"a", "c"})
	if m.Len() != 1 {
		t.Errorf("Len() after DeleteMulti = %d, want 1", m.Len())
	}
	if !m.Has("b") {
		t.Error("b should still exist")
	}
}

func TestSyncedMapClear(t *testing.T) {
	m := newSyncedMap()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Clear()
	if m.Len() != 0 {
		t.Errorf("Len() after Clear = %d, want 0", m.Len())
	}
}

func TestSyncedListBasic(t *testing.T) {
	l := newSyncedList()

	l.PushBack("a")
	l.PushBack("b")
	l.PushFront("z")

	if l.Len() != 3 {
		t.Errorf("Len() = %d, want 3", l.Len())
	}

	front := l.Front()
	if front.Value.(string) != "z" {
		t.Errorf("Front = %v, want z", front.Value)
	}

	back := l.Back()
	if back.Value.(string) != "b" {
		t.Errorf("Back = %v, want b", back.Value)
	}
}

func TestSyncedListRemove(t *testing.T) {
	l := newSyncedList()
	e1 := l.PushBack("a")
	l.PushBack("b")

	l.Remove(e1)
	if l.Len() != 1 {
		t.Errorf("Len() after Remove = %d, want 1", l.Len())
	}
	if l.Front().Value.(string) != "b" {
		t.Error("Front should be b after removing a")
	}
}

func TestSyncedListIter(t *testing.T) {
	l := newSyncedList()
	l.PushBack(1)
	l.PushBack(2)
	l.PushBack(3)

	var sum int
	for e := range l.Iter() {
		sum += e.Value.(int)
	}
	if sum != 6 {
		t.Errorf("Iter sum = %d, want 6", sum)
	}
}

func TestSyncedListClear(t *testing.T) {
	l := newSyncedList()
	l.PushBack("a")
	l.PushBack("b")
	l.Clear()
	if l.Len() != 0 {
		t.Errorf("Len() after Clear = %d, want 0", l.Len())
	}
}

func TestKeyedDequeBasic(t *testing.T) {
	d := newKeyedDeque()

	d.Push("k1", "v1")
	d.Push("k2", "v2")

	if d.Len() != 2 {
		t.Errorf("Len() = %d, want 2", d.Len())
	}
	if !d.HasKey("k1") {
		t.Error("should have k1")
	}

	e, ok := d.Get("k1")
	if !ok || e.Value.(string) != "v1" {
		t.Errorf("Get(k1) = %v, %v", e.Value, ok)
	}
}

func TestKeyedDequePushUpdate(t *testing.T) {
	d := newKeyedDeque()

	d.Push("k1", "v1")
	d.Push("k1", "v2")

	if d.Len() != 1 {
		t.Errorf("Len() after update = %d, want 1", d.Len())
	}

	e, _ := d.Get("k1")
	if e.Value.(string) != "v2" {
		t.Errorf("updated value = %v, want v2", e.Value)
	}
}

func TestKeyedDequeDelete(t *testing.T) {
	d := newKeyedDeque()
	d.Push("k1", "v1")
	d.Push("k2", "v2")

	d.Delete("k1")
	if d.Len() != 1 {
		t.Errorf("Len() after Delete = %d, want 1", d.Len())
	}
	if d.HasKey("k1") {
		t.Error("k1 should be deleted")
	}
}

func TestKeyedDequeRemove(t *testing.T) {
	d := newKeyedDeque()
	d.Push("k1", "v1")
	d.Push("k2", "v2")
	e, _ := d.Get("k2")

	d.Remove(e)
	if d.Len() != 1 {
		t.Errorf("Len() after Remove = %d, want 1", d.Len())
	}
	if d.HasKey("k2") {
		t.Error("k2 should be removed")
	}
}

func TestKeyedDequeClear(t *testing.T) {
	d := newKeyedDeque()
	d.Push("k1", "v1")
	d.Push("k2", "v2")
	d.Clear()
	if d.Len() != 0 {
		t.Errorf("Len() after Clear = %d, want 0", d.Len())
	}
}

func TestKeyedDequeIter(t *testing.T) {
	d := newKeyedDeque()
	d.Push("a", 1)
	d.Push("b", 2)
	d.Push("c", 3)

	var sum int
	for e := range d.Iter() {
		sum += e.Value.(int)
	}
	if sum != 6 {
		t.Errorf("Iter sum = %d, want 6", sum)
	}
}

func BenchmarkSyncedMapSet(b *testing.B) {
	m := newSyncedMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(i, i)
	}
}

func BenchmarkSyncedMapGet(b *testing.B) {
	m := newSyncedMap()
	for i := 0; i < 1000; i++ {
		m.Set(i, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(i % 1000)
	}
}
