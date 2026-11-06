package dht

import (
	"log"
	"net"
	"sync"
	"time"
)

// BEP-32 IPv6 Support for DHT

// IPv6Manager manages IPv6 DHT operations
type IPv6Manager struct {
	mu          sync.RWMutex
	enabled     bool
	ipv6Conn    *net.UDPConn
	ipv6Address string
	ipv6Node    *node
	dualStack   bool // Whether to run both IPv4 and IPv6

	// Statistics
	stats IPv6Stats
}

// IPv6Stats holds IPv6 statistics
type IPv6Stats struct {
	PacketsReceived int64
	PacketsSent     int64
	NodesDiscovered int64
	QueriesSent     int64
	QueriesReceived int64
	Errors          int64
	StartTime       time.Time
}

// NewIPv6Manager creates a new IPv6 manager
func NewIPv6Manager() *IPv6Manager {
	return &IPv6Manager{
		enabled:   false,
		dualStack: true,
		stats: IPv6Stats{
			StartTime: time.Now(),
		},
	}
}

// Enable enables IPv6 support
func (m *IPv6Manager) Enable(address string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.enabled {
		return nil
	}

	// Check if IPv6 is available
	if !IsIPv6Available() {
		return &IPv6Error{Message: "IPv6 not available on this system"}
	}

	m.ipv6Address = address

	// Generate IPv6-compliant node ID
	localIPv6, err := GetLocalIPv6()
	if err != nil {
		log.Printf("[IPv6] Warning: Could not get local IPv6 address: %v", err)
		// Continue anyway, we can still receive IPv6 packets
	}

	nodeID := generateNodeID(localIPv6)
	m.ipv6Node, err = newNode(nodeID, "udp6", address)
	if err != nil {
		return err
	}

	m.enabled = true
	log.Printf("[IPv6] IPv6 support enabled on %s", address)

	return nil
}

// Disable disables IPv6 support
func (m *IPv6Manager) Disable() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.enabled {
		return
	}

	if m.ipv6Conn != nil {
		m.ipv6Conn.Close()
		m.ipv6Conn = nil
	}

	m.enabled = false
	log.Printf("[IPv6] IPv6 support disabled")
}

// IsEnabled returns whether IPv6 is enabled
func (m *IPv6Manager) IsEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enabled
}

// StartListening starts listening for IPv6 packets
func (m *IPv6Manager) StartListening(packetHandler func([]byte, *net.UDPAddr)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.enabled {
		return &IPv6Error{Message: "IPv6 not enabled"}
	}

	conn, err := net.ListenPacket("udp6", m.ipv6Address)
	if err != nil {
		return err
	}

	m.ipv6Conn = conn.(*net.UDPConn)

	// Start receiving packets
	go m.receivePackets(packetHandler)

	log.Printf("[IPv6] Started listening on %s", m.ipv6Address)
	return nil
}

// receivePackets receives IPv6 packets
func (m *IPv6Manager) receivePackets(handler func([]byte, *net.UDPAddr)) {
	buf := make([]byte, 8192)

	for {
		n, addr, err := m.ipv6Conn.ReadFromUDP(buf)
		if err != nil {
			if m.enabled {
				log.Printf("[IPv6] Read error: %v", err)
				m.stats.Errors++
			}
			return
		}

		m.stats.PacketsReceived++

		// Copy data to avoid race conditions
		data := make([]byte, n)
		copy(data, buf[:n])

		// Handle packet
		handler(data, addr)
	}
}

// SendPacket sends a packet to an IPv6 address
func (m *IPv6Manager) SendPacket(data []byte, addr *net.UDPAddr) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.enabled || m.ipv6Conn == nil {
		return &IPv6Error{Message: "IPv6 not enabled or not listening"}
	}

	m.ipv6Conn.SetWriteDeadline(time.Now().Add(15 * time.Second))
	_, err := m.ipv6Conn.WriteToUDP(data, addr)
	if err != nil {
		m.stats.Errors++
		return err
	}

	m.stats.PacketsSent++
	return nil
}

// GetStats returns IPv6 statistics
func (m *IPv6Manager) GetStats() IPv6Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stats
}

// IsIPv6Available checks if IPv6 is available on the system
func IsIPv6Available() bool {
	// Try to listen on IPv6
	conn, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// GetLocalIPv6 returns the local IPv6 address
func GetLocalIPv6() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}

			// Return first non-link-local IPv6 address
			if ip.To4() == nil && ip.To16() != nil {
				return ip, nil
			}
		}
	}

	return nil, &IPv6Error{Message: "no IPv6 address found"}
}

// IsIPv6Address checks if a string is an IPv6 address
func IsIPv6Address(addr string) bool {
	ip := net.ParseIP(addr)
	if ip == nil {
		// Try parsing as host:port
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return false
		}
		ip = net.ParseIP(host)
	}

	return ip != nil && ip.To4() == nil
}

// ConvertToIPv6Mapped converts an IPv4 address to IPv6-mapped format
func ConvertToIPv6Mapped(ip4 net.IP) net.IP {
	if ip4.To4() == nil {
		return ip4 // Already IPv6
	}

	// Create IPv6-mapped IPv4 address: ::ffff:x.x.x.x
	mapped := make(net.IP, 16)
	mapped[10] = 0xff
	mapped[11] = 0xff
	copy(mapped[12:16], ip4.To4())
	return mapped
}

// ExtractIPv4FromMapped extracts IPv4 from IPv6-mapped address
func ExtractIPv4FromMapped(ip6 net.IP) net.IP {
	if len(ip6) != 16 {
		return nil
	}

	// Check if it's an IPv4-mapped address (::ffff:x.x.x.x)
	if ip6[0] == 0 && ip6[1] == 0 && ip6[2] == 0 && ip6[3] == 0 &&
		ip6[4] == 0 && ip6[5] == 0 && ip6[6] == 0 && ip6[7] == 0 &&
		ip6[8] == 0 && ip6[9] == 0 && ip6[10] == 0xff && ip6[11] == 0xff {
		return ip6[12:16]
	}

	return nil
}

// IPv6BootstrapNodes contains default IPv6 bootstrap nodes
var IPv6BootstrapNodes = []string{
	"[2001:1898:0:80::a2d5]:6881", // router.bittorrent.com IPv6
	"[2001:470:79:880::2]:6881",   // router.utorrent.com IPv6 (example)
	"[2401:5180:0:15::2]:6881",    // dht.transmissionbt.com IPv6 (example)
}

// DualStackDHT manages both IPv4 and IPv6 DHT instances
type DualStackDHT struct {
	ipv4DHT *DHT
	ipv6Mgr *IPv6Manager
	mu      sync.RWMutex
}

// NewDualStackDHT creates a new dual-stack DHT
func NewDualStackDHT(ipv4DHT *DHT) *DualStackDHT {
	return &DualStackDHT{
		ipv4DHT: ipv4DHT,
		ipv6Mgr: NewIPv6Manager(),
	}
}

// EnableIPv6 enables IPv6 support
func (ds *DualStackDHT) EnableIPv6(address string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	return ds.ipv6Mgr.Enable(address)
}

// IsIPv6Enabled returns whether IPv6 is enabled
func (ds *DualStackDHT) IsIPv6Enabled() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.ipv6Mgr.IsEnabled()
}

// GetIPv6Manager returns the IPv6 manager
func (ds *DualStackDHT) GetIPv6Manager() *IPv6Manager {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.ipv6Mgr
}

// GetIPv4DHT returns the IPv4 DHT instance
func (ds *DualStackDHT) GetIPv4DHT() *DHT {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.ipv4DHT
}

// CombinedStats returns combined statistics
type CombinedStats struct {
	IPv4Nodes   int
	IPv6Nodes   int
	IPv4Packets int64
	IPv6Packets int64
	IPv4Errors  int64
	IPv6Errors  int64
	Uptime      time.Duration
}

// GetCombinedStats returns combined IPv4/IPv6 statistics
func (ds *DualStackDHT) GetCombinedStats() CombinedStats {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	ipv6Stats := ds.ipv6Mgr.GetStats()

	return CombinedStats{
		IPv4Nodes:   ds.ipv4DHT.routingTable.Len(),
		IPv6Nodes:   0,                                                 // TODO: Track IPv6 nodes separately
		IPv4Packets: int64(ds.ipv4DHT.GetDHTStats().RoutingTableNodes), // Placeholder
		IPv6Packets: ipv6Stats.PacketsReceived + ipv6Stats.PacketsSent,
		IPv4Errors:  0,
		IPv6Errors:  ipv6Stats.Errors,
		Uptime:      time.Since(ipv6Stats.StartTime),
	}
}

// IPv6Error represents an IPv6-related error
type IPv6Error struct {
	Message string
}

func (e *IPv6Error) Error() string {
	return e.Message
}

// IPv6CompactPeer represents a peer in IPv6 compact format
type IPv6CompactPeer struct {
	IP   net.IP // 16 bytes
	Port int    // 2 bytes
}

// Encode encodes to 18-byte compact format
func (p *IPv6CompactPeer) Encode() []byte {
	result := make([]byte, 18)
	copy(result[0:16], p.IP.To16())
	result[16] = byte(p.Port >> 8)
	result[17] = byte(p.Port & 0xFF)
	return result
}

// Decode decodes from 18-byte compact format
func (p *IPv6CompactPeer) Decode(data []byte) error {
	if len(data) != 18 {
		return &IPv6Error{Message: "invalid IPv6 compact peer length"}
	}
	p.IP = make(net.IP, 16)
	copy(p.IP, data[0:16])
	p.Port = int(data[16])<<8 | int(data[17])
	return nil
}

// IPv6CompactNode represents a node in IPv6 compact format
type IPv6CompactNode struct {
	ID   string // 20 bytes
	IP   net.IP // 16 bytes
	Port int    // 2 bytes
}

// Encode encodes to 38-byte compact format
func (n *IPv6CompactNode) Encode() []byte {
	result := make([]byte, 38)
	copy(result[0:20], n.ID)
	copy(result[20:36], n.IP.To16())
	result[36] = byte(n.Port >> 8)
	result[37] = byte(n.Port & 0xFF)
	return result
}

// Decode decodes from 38-byte compact format
func (n *IPv6CompactNode) Decode(data []byte) error {
	if len(data) != 38 {
		return &IPv6Error{Message: "invalid IPv6 compact node length"}
	}
	n.ID = string(data[0:20])
	n.IP = make(net.IP, 16)
	copy(n.IP, data[20:36])
	n.Port = int(data[36])<<8 | int(data[37])
	return nil
}
