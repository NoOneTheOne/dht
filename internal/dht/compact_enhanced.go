package dht

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
)

// BEP-23 Compact Peer Info Format Enhancements
// Supports both IPv4 (6 bytes) and IPv6 (18 bytes) compact formats

const (
	// CompactIPv4Size is the size of compact IPv4 peer info (4 bytes IP + 2 bytes port)
	CompactIPv4Size = 6
	// CompactIPv6Size is the size of compact IPv6 peer info (16 bytes IP + 2 bytes port)
	CompactIPv6Size = 18
	// CompactNodeInfoIPv4Size is the size of compact IPv4 node info (20 bytes ID + 6 bytes peer)
	CompactNodeInfoIPv4Size = 26
	// CompactNodeInfoIPv6Size is the size of compact IPv6 node info (20 bytes ID + 18 bytes peer)
	CompactNodeInfoIPv6Size = 38
)

// CompactPeerInfo represents a peer in compact format
type CompactPeerInfo struct {
	IP   net.IP
	Port int
}

// IsIPv6 returns true if the peer info is IPv6
func (cpi *CompactPeerInfo) IsIPv6() bool {
	return cpi.IP.To4() == nil
}

// Encode encodes the peer info to compact format
func (cpi *CompactPeerInfo) Encode() ([]byte, error) {
	return EncodeCompactPeerInfo(cpi.IP, cpi.Port)
}

// Decode decodes compact peer info
func (cpi *CompactPeerInfo) Decode(data []byte) error {
	ip, port, err := DecodeCompactPeerInfo(data)
	if err != nil {
		return err
	}
	cpi.IP = ip
	cpi.Port = port
	return nil
}

// EncodeCompactPeerInfo encodes IP and port to compact format
// Supports both IPv4 (6 bytes) and IPv6 (18 bytes)
func EncodeCompactPeerInfo(ip net.IP, port int) ([]byte, error) {
	if port < 0 || port > 65535 {
		return nil, errors.New("port out of range")
	}

	// Try IPv4 first
	if ip4 := ip.To4(); ip4 != nil {
		result := make([]byte, CompactIPv4Size)
		copy(result[0:4], ip4)
		binary.BigEndian.PutUint16(result[4:6], uint16(port))
		return result, nil
	}

	// IPv6
	if ip6 := ip.To16(); ip6 != nil {
		result := make([]byte, CompactIPv6Size)
		copy(result[0:16], ip6)
		binary.BigEndian.PutUint16(result[16:18], uint16(port))
		return result, nil
	}

	return nil, errors.New("invalid IP address")
}

// DecodeCompactPeerInfo decodes compact peer info
// Automatically detects IPv4 (6 bytes) or IPv6 (18 bytes)
func DecodeCompactPeerInfo(data []byte) (net.IP, int, error) {
	switch len(data) {
	case CompactIPv4Size:
		ip := net.IP(make([]byte, 4))
		copy(ip, data[0:4])
		port := int(binary.BigEndian.Uint16(data[4:6]))
		return ip, port, nil

	case CompactIPv6Size:
		ip := net.IP(make([]byte, 16))
		copy(ip, data[0:16])
		port := int(binary.BigEndian.Uint16(data[16:18]))
		return ip, port, nil

	default:
		return nil, 0, errors.New("invalid compact peer info length")
	}
}

// CompactPeerList represents a list of peers in compact format
type CompactPeerList struct {
	Peers  []CompactPeerInfo
	IsIPv6 bool
}

// NewCompactPeerList creates a new compact peer list
func NewCompactPeerList(isIPv6 bool) *CompactPeerList {
	return &CompactPeerList{
		Peers:  make([]CompactPeerInfo, 0),
		IsIPv6: isIPv6,
	}
}

// Add adds a peer to the list
func (cpl *CompactPeerList) Add(ip net.IP, port int) error {
	peer := CompactPeerInfo{IP: ip, Port: port}

	// Validate consistency
	if peer.IsIPv6() != cpl.IsIPv6 {
		if cpl.IsIPv6 {
			return errors.New("cannot add IPv4 peer to IPv6 list")
		}
		return errors.New("cannot add IPv6 peer to IPv4 list")
	}

	cpl.Peers = append(cpl.Peers, peer)
	return nil
}

// Encode encodes the peer list to compact format
func (cpl *CompactPeerList) Encode() ([]byte, error) {
	expectedSize := CompactIPv4Size
	if cpl.IsIPv6 {
		expectedSize = CompactIPv6Size
	}

	result := make([]byte, len(cpl.Peers)*expectedSize)

	for i, peer := range cpl.Peers {
		encoded, err := peer.Encode()
		if err != nil {
			return nil, err
		}
		copy(result[i*expectedSize:], encoded)
	}

	return result, nil
}

// Decode decodes a compact peer list
func (cpl *CompactPeerList) Decode(data []byte) error {
	expectedSize := CompactIPv4Size
	if cpl.IsIPv6 {
		expectedSize = CompactIPv6Size
	}

	if len(data)%expectedSize != 0 {
		return errors.New("invalid compact peer list length")
	}

	count := len(data) / expectedSize
	cpl.Peers = make([]CompactPeerInfo, count)

	for i := 0; i < count; i++ {
		start := i * expectedSize
		end := start + expectedSize
		if err := cpl.Peers[i].Decode(data[start:end]); err != nil {
			return err
		}
	}

	return nil
}

// CompactNodeList represents a list of nodes in compact format
type CompactNodeList struct {
	Nodes  []CompactNodeInfo
	IsIPv6 bool
}

// CompactNodeInfo represents a node in compact format
type CompactNodeInfo struct {
	ID   string // 20-byte node ID
	IP   net.IP
	Port int
}

// NewCompactNodeList creates a new compact node list
func NewCompactNodeList(isIPv6 bool) *CompactNodeList {
	return &CompactNodeList{
		Nodes:  make([]CompactNodeInfo, 0),
		IsIPv6: isIPv6,
	}
}

// Add adds a node to the list
func (cnl *CompactNodeList) Add(id string, ip net.IP, port int) error {
	if len(id) != 20 {
		return errors.New("node ID must be 20 bytes")
	}

	node := CompactNodeInfo{ID: id, IP: ip, Port: port}

	// Validate consistency
	isIPv6 := ip.To4() == nil
	if isIPv6 != cnl.IsIPv6 {
		if cnl.IsIPv6 {
			return errors.New("cannot add IPv4 node to IPv6 list")
		}
		return errors.New("cannot add IPv6 node to IPv4 list")
	}

	cnl.Nodes = append(cnl.Nodes, node)
	return nil
}

// Encode encodes the node list to compact format
func (cnl *CompactNodeList) Encode() ([]byte, error) {
	expectedSize := CompactNodeInfoIPv4Size
	if cnl.IsIPv6 {
		expectedSize = CompactNodeInfoIPv6Size
	}

	result := make([]byte, len(cnl.Nodes)*expectedSize)

	for i, node := range cnl.Nodes {
		offset := i * expectedSize

		// Copy node ID (20 bytes)
		copy(result[offset:offset+20], node.ID)

		// Encode and copy peer info
		peerInfo, err := EncodeCompactPeerInfo(node.IP, node.Port)
		if err != nil {
			return nil, err
		}
		copy(result[offset+20:], peerInfo)
	}

	return result, nil
}

// Decode decodes a compact node list
func (cnl *CompactNodeList) Decode(data []byte) error {
	expectedSize := CompactNodeInfoIPv4Size
	if cnl.IsIPv6 {
		expectedSize = CompactNodeInfoIPv6Size
	}

	if len(data)%expectedSize != 0 {
		return errors.New("invalid compact node list length")
	}

	count := len(data) / expectedSize
	cnl.Nodes = make([]CompactNodeInfo, count)

	for i := 0; i < count; i++ {
		offset := i * expectedSize

		// Extract node ID
		cnl.Nodes[i].ID = string(data[offset : offset+20])

		// Extract peer info
		peerSize := CompactIPv4Size
		if cnl.IsIPv6 {
			peerSize = CompactIPv6Size
		}

		ip, port, err := DecodeCompactPeerInfo(data[offset+20 : offset+20+peerSize])
		if err != nil {
			return err
		}
		cnl.Nodes[i].IP = ip
		cnl.Nodes[i].Port = port
	}

	return nil
}

// BatchCompactEncoder provides batch encoding/decoding utilities
type BatchCompactEncoder struct{}

// EncodePeersBatch encodes multiple peers in batch
func (bce *BatchCompactEncoder) EncodePeersBatch(peers []CompactPeerInfo) ([]byte, error) {
	if len(peers) == 0 {
		return []byte{}, nil
	}

	// Determine if IPv6 based on first peer
	isIPv6 := peers[0].IsIPv6()

	list := NewCompactPeerList(isIPv6)
	for _, peer := range peers {
		if err := list.Add(peer.IP, peer.Port); err != nil {
			return nil, err
		}
	}

	return list.Encode()
}

// DecodePeersBatch decodes multiple peers in batch
func (bce *BatchCompactEncoder) DecodePeersBatch(data []byte, isIPv6 bool) ([]CompactPeerInfo, error) {
	list := NewCompactPeerList(isIPv6)
	if err := list.Decode(data); err != nil {
		return nil, err
	}
	return list.Peers, nil
}

// EncodeNodesBatch encodes multiple nodes in batch
func (bce *BatchCompactEncoder) EncodeNodesBatch(nodes []CompactNodeInfo) ([]byte, error) {
	if len(nodes) == 0 {
		return []byte{}, nil
	}

	// Determine if IPv6 based on first node
	isIPv6 := nodes[0].IP.To4() == nil

	list := NewCompactNodeList(isIPv6)
	for _, node := range nodes {
		if err := list.Add(node.ID, node.IP, node.Port); err != nil {
			return nil, err
		}
	}

	return list.Encode()
}

// DecodeNodesBatch decodes multiple nodes in batch
func (bce *BatchCompactEncoder) DecodeNodesBatch(data []byte, isIPv6 bool) ([]CompactNodeInfo, error) {
	list := NewCompactNodeList(isIPv6)
	if err := list.Decode(data); err != nil {
		return nil, err
	}
	return list.Nodes, nil
}

// CompactFormatStats holds statistics about compact format usage
type CompactFormatStats struct {
	IPv4Encoded int64
	IPv4Decoded int64
	IPv6Encoded int64
	IPv6Decoded int64
	Errors      int64
}

var (
	compactStats   CompactFormatStats
	compactStatsMu sync.Mutex
)

// RecordIPv4Encode records an IPv4 encode operation
func RecordIPv4Encode() {
	compactStatsMu.Lock()
	compactStats.IPv4Encoded++
	compactStatsMu.Unlock()
}

// RecordIPv4Decode records an IPv4 decode operation
func RecordIPv4Decode() {
	compactStatsMu.Lock()
	compactStats.IPv4Decoded++
	compactStatsMu.Unlock()
}

// RecordIPv6Encode records an IPv6 encode operation
func RecordIPv6Encode() {
	compactStatsMu.Lock()
	compactStats.IPv6Encoded++
	compactStatsMu.Unlock()
}

// RecordIPv6Decode records an IPv6 decode operation
func RecordIPv6Decode() {
	compactStatsMu.Lock()
	compactStats.IPv6Decoded++
	compactStatsMu.Unlock()
}

// RecordCompactError records a compact format error
func RecordCompactError() {
	compactStatsMu.Lock()
	compactStats.Errors++
	compactStatsMu.Unlock()
}

// GetCompactStats returns compact format statistics
func GetCompactStats() CompactFormatStats {
	compactStatsMu.Lock()
	defer compactStatsMu.Unlock()
	return compactStats
}
