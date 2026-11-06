package dht

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"net"
)

// BEP-42: Generate node ID based on IP address
// The first 8 bits of node ID must be derived from the IP address
// This helps prevent Sybil attacks

// generateNodeID generates a BEP-42 compliant node ID from an IP address
func generateNodeID(ip net.IP) string {
	id := make([]byte, 20)

	// Fill with random bytes first
	rand.Read(id)

	// Apply BEP-42: mask the first byte based on IP hash
	if ip4 := ip.To4(); ip4 != nil {
		// IPv4: use first 3 octets for hash
		mask := generateIPv4Mask(ip4)
		id[0] = (id[0] & mask) | (mask ^ 0xFF)
	} else if ip6 := ip.To16(); ip6 != nil {
		// IPv6: use first 6 octets for hash
		mask := generateIPv6Mask(ip6)
		id[0] = (id[0] & mask) | (mask ^ 0xFF)
	}

	// BEP-42: Set the last byte's top 3 bits to 0b000
	id[19] = id[19] & 0x7F

	return string(id)
}

// generateIPv4Mask generates the mask for IPv4 address
// BEP-42: r = IP[0..3] & 0x03000000 (for IPv4)
func generateIPv4Mask(ip net.IP) byte {
	// Create a 4-byte representation
	ipBytes := ip.To4()
	if ipBytes == nil {
		return 0xFF
	}

	// r = (ip[0] << 24) | (ip[1] << 16) | (ip[2] << 8) | ip[3]
	// r = r & 0x03000000
	// This extracts bits 25-24 of the IP (from the second octet)
	r := binary.BigEndian.Uint32(ipBytes)
	r = r & 0x03000000

	// Hash r to get the mask
	hash := sha1.New()
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, r)
	hash.Write(buf)
	result := hash.Sum(nil)

	// Return first byte as mask
	return result[0]
}

// generateIPv6Mask generates the mask for IPv6 address
// BEP-42: r = IP[0..5] for IPv6
func generateIPv6Mask(ip net.IP) byte {
	ipBytes := ip.To16()
	if ipBytes == nil {
		return 0xFF
	}

	// Use first 6 bytes for IPv6
	hash := sha1.New()
	hash.Write(ipBytes[0:6])
	result := hash.Sum(nil)

	return result[0]
}
