package dht

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"sync"
)

// BEP-10 Extension Protocol Implementation
// https://www.bittorrent.org/beps/bep_0010.html

const (
	// Extension message IDs
	ExtHandshakeID = 0 // Extension handshake

	// Standard extension names
	ExtUTMetadata = "ut_metadata"  // BEP-9 metadata exchange
	ExtUTPex      = "ut_pex"       // BEP-11 peer exchange
	ExtUT_HaveAll = "ut_holepunch" // BEP-46 hole punching
)

// Extension represents a single extension
type Extension struct {
	Name string
	ID   byte
}

// ExtensionHandshake represents the extension handshake message
type ExtensionHandshake struct {
	// M maps extension names to their message IDs
	M map[string]byte

	// P is the local TCP listen port (optional)
	P int

	// V is the client name and version (optional)
	V string

	// YourIP is the client's external IP as seen by the remote (optional)
	YourIP []byte

	// IPv4 is the client's IPv4 address (optional)
	IPv4 []byte

	// IPv6 is the client's IPv6 address (optional)
	IPv6 []byte

	// ReqQ is the maximum number of outstanding request messages (optional)
	ReqQ int

	// Req is the number of outstanding request messages (optional)
	Req int

	// MetadataSize is the total size of the metadata in bytes (BEP-9)
	MetadataSize int

	// UploadOnly indicates this is a seed that doesn't accept downloads
	UploadOnly bool
}

// ExtensionManager manages extensions for a connection
type ExtensionManager struct {
	mu              sync.RWMutex
	supportedExts   map[string]byte // Extensions we support
	remoteExts      map[string]byte // Extensions remote peer supports
	localHandshake  *ExtensionHandshake
	remoteHandshake *ExtensionHandshake
	handshakeDone   bool
}

// NewExtensionManager creates a new extension manager
func NewExtensionManager() *ExtensionManager {
	em := &ExtensionManager{
		supportedExts: make(map[string]byte),
		remoteExts:    make(map[string]byte),
		localHandshake: &ExtensionHandshake{
			M: make(map[string]byte),
			V: "GoDHT/1.0",
		},
	}

	// Register default extensions
	em.RegisterExtension(ExtUTMetadata, 1)

	return em
}

// RegisterExtension registers a local extension
func (em *ExtensionManager) RegisterExtension(name string, id byte) {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.supportedExts[name] = id
	em.localHandshake.M[name] = id
}

// GetLocalExtensionID returns the local ID for an extension
func (em *ExtensionManager) GetLocalExtensionID(name string) (byte, bool) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	id, ok := em.supportedExts[name]
	return id, ok
}

// GetRemoteExtensionID returns the remote peer's ID for an extension
func (em *ExtensionManager) GetRemoteExtensionID(name string) (byte, bool) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	id, ok := em.remoteExts[name]
	return id, ok
}

// SupportsExtension checks if remote peer supports an extension
func (em *ExtensionManager) SupportsExtension(name string) bool {
	em.mu.RLock()
	defer em.mu.RUnlock()

	_, ok := em.remoteExts[name]
	return ok
}

// ProcessRemoteHandshake processes the remote peer's extension handshake
func (em *ExtensionManager) ProcessRemoteHandshake(data []byte) error {
	decoded, err := Decode(data)
	if err != nil {
		return err
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return errors.New("invalid handshake dict")
	}

	em.mu.Lock()
	defer em.mu.Unlock()

	// Parse extension mapping
	if m, ok := dict["m"].(map[string]interface{}); ok {
		for name, id := range m {
			if idInt, ok := id.(int); ok {
				em.remoteExts[name] = byte(idInt)
			}
		}
	}

	// Parse optional fields
	if p, ok := dict["p"].(int); ok {
		em.remoteHandshake.P = p
	}
	if v, ok := dict["v"].(string); ok {
		em.remoteHandshake.V = v
	}
	if reqQ, ok := dict["reqq"].(int); ok {
		em.remoteHandshake.ReqQ = reqQ
	}
	if metadataSize, ok := dict["metadata_size"].(int); ok {
		em.remoteHandshake.MetadataSize = metadataSize
	}
	if uploadOnly, ok := dict["upload_only"].(bool); ok {
		em.remoteHandshake.UploadOnly = uploadOnly
	}

	em.handshakeDone = true
	log.Printf("[BEP-10] Remote peer supports extensions: %v", em.remoteExts)

	return nil
}

// CreateHandshakeMessage creates the extension handshake message
func (em *ExtensionManager) CreateHandshakeMessage() []byte {
	em.mu.RLock()
	defer em.mu.RUnlock()

	// Build the handshake dict
	handshakeDict := map[string]interface{}{
		"m": em.localHandshake.M,
		"v": em.localHandshake.V,
	}

	if em.localHandshake.P > 0 {
		handshakeDict["p"] = em.localHandshake.P
	}
	if em.localHandshake.ReqQ > 0 {
		handshakeDict["reqq"] = em.localHandshake.ReqQ
	}
	if em.localHandshake.MetadataSize > 0 {
		handshakeDict["metadata_size"] = em.localHandshake.MetadataSize
	}

	// Encode the message
	// Format: <message length><message id=20><extended id=0><payload>
	payload := Encode(handshakeDict)

	// Message: extended ID (20) + handshake ID (0) + payload
	msg := make([]byte, 2+len(payload))
	msg[0] = EXTENDED
	msg[1] = HANDSHAKE
	copy(msg[2:], payload)

	return msg
}

// SetMetadataSize sets the metadata size in the handshake
func (em *ExtensionManager) SetMetadataSize(size int) {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.localHandshake.MetadataSize = size
}

// IsHandshakeDone returns whether the extension handshake is complete
func (em *ExtensionManager) IsHandshakeDone() bool {
	em.mu.RLock()
	defer em.mu.RUnlock()
	return em.handshakeDone
}

// ExtensionMessage represents a parsed extension message
type ExtensionMessage struct {
	ExtensionID byte
	Payload     []byte
}

// ParseExtensionMessage parses an extension message
func ParseExtensionMessage(data []byte) (*ExtensionMessage, error) {
	if len(data) < 2 {
		return nil, errors.New("message too short")
	}

	if data[0] != EXTENDED {
		return nil, errors.New("not an extension message")
	}

	return &ExtensionMessage{
		ExtensionID: data[1],
		Payload:     data[2:],
	}, nil
}

// CreateExtensionMessage creates an extension message
func CreateExtensionMessage(extID byte, payload map[string]interface{}) []byte {
	encoded := Encode(payload)

	msg := make([]byte, 2+len(encoded))
	msg[0] = EXTENDED
	msg[1] = extID
	copy(msg[2:], encoded)

	return msg
}

// BEP-9 ut_metadata message types
const (
	UTMetadataRequest = 0
	UTMetadataData    = 1
	UTMetadataReject  = 2
)

// CreateUTMetadataRequest creates a ut_metadata request message
func CreateUTMetadataRequest(extID byte, piece int) []byte {
	return CreateExtensionMessage(extID, map[string]interface{}{
		"msg_type": UTMetadataRequest,
		"piece":    piece,
	})
}

// CreateUTMetadataData creates a ut_metadata data message
func CreateUTMetadataData(extID byte, piece int, totalSize int, data []byte) []byte {
	payload := map[string]interface{}{
		"msg_type":   UTMetadataData,
		"piece":      piece,
		"total_size": totalSize,
	}

	encoded := Encode(payload)

	msg := make([]byte, 2+len(encoded)+len(data))
	msg[0] = EXTENDED
	msg[1] = extID
	copy(msg[2:], encoded)
	copy(msg[2+len(encoded):], data)

	return msg
}

// CreateUTMetadataReject creates a ut_metadata reject message
func CreateUTMetadataReject(extID byte, piece int) []byte {
	return CreateExtensionMessage(extID, map[string]interface{}{
		"msg_type": UTMetadataReject,
		"piece":    piece,
	})
}

// ExtensionStats holds extension protocol statistics
type ExtensionStats struct {
	HandshakesSent     int64
	HandshakesReceived int64
	MessagesSent       int64
	MessagesReceived   int64
	Errors             int64
}

var (
	extensionStats   ExtensionStats
	extensionStatsMu sync.Mutex
)

// RecordHandshakeSent records a sent handshake
func RecordHandshakeSent() {
	extensionStatsMu.Lock()
	extensionStats.HandshakesSent++
	extensionStatsMu.Unlock()
}

// RecordHandshakeReceived records a received handshake
func RecordHandshakeReceived() {
	extensionStatsMu.Lock()
	extensionStats.HandshakesReceived++
	extensionStatsMu.Unlock()
}

// RecordMessageSent records a sent message
func RecordMessageSent() {
	extensionStatsMu.Lock()
	extensionStats.MessagesSent++
	extensionStatsMu.Unlock()
}

// RecordMessageReceived records a received message
func RecordMessageReceived() {
	extensionStatsMu.Lock()
	extensionStats.MessagesReceived++
	extensionStatsMu.Unlock()
}

// RecordExtensionError records an extension error
func RecordExtensionError() {
	extensionStatsMu.Lock()
	extensionStats.Errors++
	extensionStatsMu.Unlock()
}

// GetExtensionStats returns current extension statistics
func GetExtensionStats() ExtensionStats {
	extensionStatsMu.Lock()
	defer extensionStatsMu.Unlock()
	return extensionStats
}

// CompactIPHelper provides utilities for BEP-10 IP handling
type CompactIPHelper struct{}

// EncodeCompactIP encodes an IP address in compact format (4 bytes for IPv4)
func EncodeCompactIP(ip []byte) []byte {
	if len(ip) == 4 {
		return ip
	}
	if len(ip) == 16 {
		// IPv6 - return as is
		return ip
	}
	return nil
}

// DecodeCompactIP decodes a compact IP address
func DecodeCompactIP(data []byte) []byte {
	if len(data) == 4 || len(data) == 16 {
		return data
	}
	return nil
}

// EncodeCompactPort encodes a port in compact format (2 bytes, big endian)
func EncodeCompactPort(port int) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(port))
	return buf
}

// DecodeCompactPort decodes a compact port
func DecodeCompactPort(data []byte) int {
	if len(data) != 2 {
		return 0
	}
	return int(binary.BigEndian.Uint16(data))
}

// ValidateExtensionMessage validates an extension message
func ValidateExtensionMessage(data []byte) error {
	if len(data) < 2 {
		return errors.New("message too short")
	}
	if data[0] != EXTENDED {
		return errors.New("not an extension message")
	}
	return nil
}

// ExtensionNegotiator handles extension negotiation between peers
type ExtensionNegotiator struct {
	manager       *ExtensionManager
	negotiated    bool
	preferredExts []string
}

// NewExtensionNegotiator creates a new extension negotiator
func NewExtensionNegotiator(manager *ExtensionManager) *ExtensionNegotiator {
	return &ExtensionNegotiator{
		manager:       manager,
		preferredExts: []string{ExtUTMetadata, ExtUTPex},
	}
}

// Negotiate performs extension negotiation
func (en *ExtensionNegotiator) Negotiate() []byte {
	// Create handshake message
	handshake := en.manager.CreateHandshakeMessage()
	RecordHandshakeSent()
	return handshake
}

// ProcessNegotiation processes received negotiation data
func (en *ExtensionNegotiator) ProcessNegotiation(data []byte) error {
	if err := en.manager.ProcessRemoteHandshake(data); err != nil {
		return err
	}
	RecordHandshakeReceived()
	en.negotiated = true

	// Log supported extensions
	for _, ext := range en.preferredExts {
		if en.manager.SupportsExtension(ext) {
			log.Printf("[BEP-10] Remote peer supports %s", ext)
		}
	}

	return nil
}

// IsNegotiated returns whether negotiation is complete
func (en *ExtensionNegotiator) IsNegotiated() bool {
	return en.negotiated
}

// Helper function to build extension message with proper framing
func buildFramedMessage(msgID byte, extID byte, payload []byte) []byte {
	// Length prefix (4 bytes) + message ID + extension ID + payload
	length := 1 + 1 + len(payload)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, int32(length))
	buf.WriteByte(msgID)
	buf.WriteByte(extID)
	buf.Write(payload)

	return buf.Bytes()
}
