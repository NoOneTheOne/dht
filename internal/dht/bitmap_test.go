package dht

import (
	"testing"
)

func TestNewBitmap(t *testing.T) {
	bm := newBitmap(160)
	if bm.Size != 160 {
		t.Errorf("Size = %d, want 160", bm.Size)
	}
	if len(bm.data) != 20 {
		t.Errorf("data len = %d, want 20", len(bm.data))
	}
}

func TestBitmapSetBit(t *testing.T) {
	bm := newBitmap(16)
	bm.Set(0)
	if bm.Bit(0) != 1 {
		t.Error("Bit(0) should be 1 after Set(0)")
	}
	bm.Set(7)
	if bm.Bit(7) != 1 {
		t.Error("Bit(7) should be 1 after Set(7)")
	}
	bm.Unset(0)
	if bm.Bit(0) != 0 {
		t.Error("Bit(0) should be 0 after Unset(0)")
	}
}

func TestBitmapString(t *testing.T) {
	bm := newBitmap(8)
	bm.Set(0)
	bm.Set(7)
	got := bm.String()
	if got != "10000001" {
		t.Errorf("String() = %v, want 10000001", got)
	}
}

func TestBitmapCompare(t *testing.T) {
	a := newBitmapFromString("\x00\xff")
	b := newBitmapFromString("\x00\xf0")

	if a.Compare(b, 16) != 1 {
		t.Error("a should be > b")
	}
	if b.Compare(a, 16) != -1 {
		t.Error("b should be < a")
	}

	c := newBitmapFromString("\x00\xff")
	if a.Compare(c, 16) != 0 {
		t.Error("a should equal c")
	}
}

func TestBitmapXor(t *testing.T) {
	a := newBitmapFromString("\xff\x00")
	b := newBitmapFromString("\x0f\xf0")
	result := a.Xor(b)

	if result.data[0] != 0xf0 {
		t.Errorf("Xor byte[0] = %02x, want f0", result.data[0])
	}
	if result.data[1] != 0xf0 {
		t.Errorf("Xor byte[1] = %02x, want f0", result.data[1])
	}
}

func TestBitmapRawString(t *testing.T) {
	original := "abcdefghij0123456789"
	bm := newBitmapFromString(original)
	if bm.RawString() != original {
		t.Errorf("RawString() roundtrip failed")
	}
}

func TestNewBitmapFrom(t *testing.T) {
	original := newBitmapFromString("\xab\xcd\xef")
	copy1 := newBitmapFrom(original, 24)

	if copy1.Size != 24 {
		t.Errorf("copy Size = %d, want 24", copy1.Size)
	}
	for i := 0; i < 24; i++ {
		if copy1.Bit(i) != original.Bit(i) {
			t.Errorf("Bit(%d) mismatch", i)
		}
	}

	copy2 := newBitmapFrom(original, 32)
	if copy2.Size != 32 {
		t.Errorf("oversized copy Size = %d, want 32", copy2.Size)
	}
}

func TestBitmapBitOutOfRange(t *testing.T) {
	bm := newBitmap(8)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Bit(8) on size-8 bitmap should panic")
		}
	}()
	bm.Bit(8)
}

func TestBitmapComparePrefix(t *testing.T) {
	a := newBitmapFromString("\xab\xcd")
	b := newBitmapFromString("\xab\xef")

	if a.Compare(b, 8) != 0 {
		t.Error("first 8 bits should be equal")
	}
	if a.Compare(b, 16) != -1 {
		t.Error("a should be < b in full 16-bit compare")
	}
}

func BenchmarkBitmapString(b *testing.B) {
	bm := newBitmapFromString("abcdefghij0123456789")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bm.String()
	}
}

func BenchmarkBitmapXor(b *testing.B) {
	a := newBitmapFromString("abcdefghij0123456789")
	c := newBitmapFromString("jihgfedcba0987654321")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = a.Xor(c)
	}
}
