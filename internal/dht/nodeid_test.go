package dht

import (
	"net"
	"testing"
)

// validNodeIDForIP reports whether id satisfies BEP-42 for ip.
func validNodeIDForIP(id string, ip net.IP) bool {
	if len(id) != 20 {
		return false
	}
	crc, ok := bep42Prefix(ip, id[19])
	if !ok {
		return false
	}
	return id[0] == byte(crc>>24) && id[1] == byte(crc>>16) && id[2]&0xf8 == byte(crc>>8)&0xf8
}

// Test vectors from BEP-42.
func TestBEP42Vectors(t *testing.T) {
	vectors := []struct {
		ip     string
		rand   byte
		prefix [3]byte
	}{
		{"124.31.75.21", 1, [3]byte{0x5f, 0xbf, 0xbf}},
		{"21.75.31.124", 86, [3]byte{0x5a, 0x3c, 0xe9}},
		{"65.23.51.170", 22, [3]byte{0xa5, 0xd4, 0x32}},
		{"84.124.73.14", 65, [3]byte{0x1b, 0x03, 0x21}},
		{"43.213.53.83", 90, [3]byte{0xe5, 0x6f, 0x6c}},
	}
	for _, v := range vectors {
		crc, ok := bep42Prefix(net.ParseIP(v.ip), v.rand)
		if !ok {
			t.Fatalf("%s: not covered", v.ip)
		}
		if byte(crc>>24) != v.prefix[0] || byte(crc>>16) != v.prefix[1] || byte(crc>>8)&0xf8 != v.prefix[2]&0xf8 {
			t.Fatalf("%s: prefix %08x, want %x", v.ip, crc, v.prefix)
		}
		id := []byte(randomString(20))
		id[0], id[1], id[2], id[19] = v.prefix[0], v.prefix[1], v.prefix[2], v.rand
		if !validNodeIDForIP(string(id), net.ParseIP(v.ip)) {
			t.Fatalf("%s: vector ID rejected", v.ip)
		}
	}
}

func TestGenerateNodeIDIsValid(t *testing.T) {
	for _, ip := range []string{"124.31.75.21", "8.8.8.8", "2001:4860:4860::8888"} {
		for i := 0; i < 32; i++ {
			id := generateNodeID(net.ParseIP(ip))
			if len(id) != 20 || !validNodeIDForIP(id, net.ParseIP(ip)) {
				t.Fatalf("%s: generated ID is not BEP-42 compliant", ip)
			}
		}
	}
	if validNodeIDForIP(generateNodeID(net.ParseIP("124.31.75.21")), net.ParseIP("21.75.31.124")) {
		t.Fatal("ID validated for a different IP")
	}
	if len(generateNodeID(nil)) != 20 {
		t.Fatal("uncovered address must still yield a 20-byte ID")
	}
}
