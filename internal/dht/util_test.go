package dht

import (
	"net"
	"testing"
)

func TestInt2Bytes(t *testing.T) {
	tests := []struct {
		input uint64
		want  int
	}{
		{0, 1},
		{1, 1},
		{255, 1},
		{256, 2},
		{65535, 2},
		{65536, 3},
	}
	for _, tt := range tests {
		got := int2bytes(tt.input)
		if len(got) != tt.want {
			t.Errorf("int2bytes(%d) len = %d, want %d", tt.input, len(got), tt.want)
		}
	}
}

func TestBytes2Int(t *testing.T) {
	tests := []struct {
		input []byte
		want  uint64
	}{
		{[]byte{0}, 0},
		{[]byte{1}, 1},
		{[]byte{0, 1}, 1},
		{[]byte{1, 0}, 256},
		{[]byte{0xff, 0xff}, 65535},
	}
	for _, tt := range tests {
		got := bytes2int(tt.input)
		if got != tt.want {
			t.Errorf("bytes2int(%v) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestCompactIPPortInfoIPv4(t *testing.T) {
	ip := net.ParseIP("192.168.1.1").To4()
	port := 6881

	info, err := encodeCompactIPPortInfo(ip, port)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	if len(info) != 6 {
		t.Fatalf("encoded len = %d, want 6", len(info))
	}

	decodedIP, decodedPort, err := decodeCompactIPPortInfo(info)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !decodedIP.Equal(ip) {
		t.Errorf("decoded IP = %v, want %v", decodedIP, ip)
	}
	if decodedPort != port {
		t.Errorf("decoded port = %d, want %d", decodedPort, port)
	}
}

func TestCompactIPPortInfoIPv6(t *testing.T) {
	ip := net.ParseIP("2001:db8::1")
	port := 6881

	info, err := encodeCompactIPPortInfo(ip, port)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	if len(info) != 18 {
		t.Fatalf("encoded len = %d, want 18", len(info))
	}

	decodedIP, decodedPort, err := decodeCompactIPPortInfo(info)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !decodedIP.Equal(ip) {
		t.Errorf("decoded IP = %v, want %v", decodedIP, ip)
	}
	if decodedPort != port {
		t.Errorf("decoded port = %d, want %d", decodedPort, port)
	}
}

func TestEncodeCompactIPPortInfoInvalidPort(t *testing.T) {
	ip := net.ParseIP("127.0.0.1").To4()

	_, err := encodeCompactIPPortInfo(ip, -1)
	if err == nil {
		t.Error("should error on negative port")
	}

	_, err = encodeCompactIPPortInfo(ip, 70000)
	if err == nil {
		t.Error("should error on port > 65535")
	}
}

func TestDecodeCompactIPPortInfoInvalidLength(t *testing.T) {
	_, _, err := decodeCompactIPPortInfo("short")
	if err == nil {
		t.Error("should error on invalid length")
	}
}

func TestGenAddress(t *testing.T) {
	got := genAddress("192.168.1.1", 6881)
	want := "192.168.1.1:6881"
	if got != want {
		t.Errorf("genAddress = %v, want %v", got, want)
	}
}

func BenchmarkGenAddress(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = genAddress("192.168.1.1", 6881)
	}
}

func BenchmarkEncodeCompactIPv4(b *testing.B) {
	ip := net.ParseIP("192.168.1.1").To4()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encodeCompactIPPortInfo(ip, 6881)
	}
}
