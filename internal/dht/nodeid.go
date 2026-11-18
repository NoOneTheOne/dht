package dht

import (
	"crypto/rand"
	"hash/crc32"
	"net"
)

// BEP-42 (DHT security extension): the top 21 bits of a node ID are derived
// from the node's external IP so that peers can reject forged IDs.
// See https://www.bittorrent.org/beps/bep_0042.html.

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	bep42Mask4 = []byte{0x03, 0x0f, 0x3f, 0xff}
	bep42Mask6 = []byte{0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff}
)

// bep42Prefix returns the CRC32-C over the masked IP with r folded into the
// top three bits, or false for addresses BEP-42 does not cover.
func bep42Prefix(ip net.IP, r byte) (uint32, bool) {
	var masked []byte
	if ip4 := ip.To4(); ip4 != nil {
		masked = make([]byte, len(bep42Mask4))
		for i, m := range bep42Mask4 {
			masked[i] = ip4[i] & m
		}
	} else if ip6 := ip.To16(); ip6 != nil {
		masked = make([]byte, len(bep42Mask6))
		for i, m := range bep42Mask6 {
			masked[i] = ip6[i] & m
		}
	} else {
		return 0, false
	}
	masked[0] |= (r & 0x07) << 5
	return crc32.Checksum(masked, castagnoli), true
}

// generateNodeID generates a BEP-42 compliant node ID for ip. Addresses that
// BEP-42 cannot cover produce a fully random ID.
func generateNodeID(ip net.IP) string {
	id := make([]byte, 20)
	rand.Read(id)

	r := id[19] & 0x07
	crc, ok := bep42Prefix(ip, r)
	if !ok {
		return string(id)
	}
	id[0] = byte(crc >> 24)
	id[1] = byte(crc >> 16)
	id[2] = byte(crc>>8)&0xf8 | id[2]&0x07
	// id[19] stays random; its low three bits are r.
	return string(id)
}
