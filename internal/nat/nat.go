// Package nat provides NAT traversal functionality including UPnP and NAT-PMP support.
package nat

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/huin/goupnp/dcps/internetgateway2"
	"github.com/jackpal/gateway"
	natpmp "github.com/jackpal/go-nat-pmp"
)

// Mapping represents a port mapping
type Mapping struct {
	Protocol   string // "tcp" or "udp"
	ExternalIP net.IP
	External   int // External port
	Internal   int // Internal port
	Desc       string
	Expires    time.Time
}

// NAT interface for NAT traversal operations
type NAT interface {
	GetExternalAddress() (net.IP, error)
	AddPortMapping(protocol string, externalPort, internalPort int, desc string, timeout time.Duration) error
	DeletePortMapping(protocol string, externalPort, internalPort int) error
	Type() string
}

// UPnPNAT implements NAT using UPnP-IGD
type UPnPNAT struct {
	ipService  *internetgateway2.WANIPConnection1
	pppService *internetgateway2.WANPPPConnection1
	devURL     string
}

// NATPMP implements NAT using NAT-PMP
type NATPMP struct {
	client *natpmp.Client
	gw     net.IP
}

var (
	discoveredNAT NAT
	discoverOnce  sync.Once
	discoverErr   error
)

// Discover attempts to discover a NAT device using UPnP first, then NAT-PMP
func Discover() (NAT, error) {
	discoverOnce.Do(func() {
		// Try UPnP first
		if nat, err := discoverUPnP(); err == nil {
			discoveredNAT = nat
			log.Printf("[NAT] Discovered UPnP IGD at %s", nat.devURL)
			return
		}

		// Try NAT-PMP
		if nat, err := discoverNATPMP(); err == nil {
			discoveredNAT = nat
			log.Printf("[NAT] Discovered NAT-PMP at %s", nat.gw)
			return
		}

		discoverErr = errors.New("no NAT device found")
		log.Printf("[NAT] No NAT device discovered")
	})

	return discoveredNAT, discoverErr
}

func discoverUPnP() (*UPnPNAT, error) {
	// Try WANIPConnection1
	ipServices, _, err := internetgateway2.NewWANIPConnection1Clients()
	if err == nil && len(ipServices) > 0 {
		return &UPnPNAT{
			ipService: ipServices[0],
			devURL:    ipServices[0].ServiceClient.Location.String(),
		}, nil
	}

	// Try WANPPPConnection1
	pppServices, _, err := internetgateway2.NewWANPPPConnection1Clients()
	if err == nil && len(pppServices) > 0 {
		return &UPnPNAT{
			pppService: pppServices[0],
			devURL:     pppServices[0].ServiceClient.Location.String(),
		}, nil
	}

	return nil, errors.New("UPnP not available")
}

func discoverNATPMP() (*NATPMP, error) {
	gwIP, err := gateway.DiscoverGateway()
	if err != nil {
		return nil, err
	}

	if gwIP == nil {
		return nil, errors.New("no gateway found")
	}

	client := natpmp.NewClient(gwIP)
	_, err = client.GetExternalAddress()
	if err != nil {
		return nil, err
	}

	return &NATPMP{
		client: client,
		gw:     gwIP,
	}, nil
}

// GetExternalAddress returns the external IP address
func (u *UPnPNAT) GetExternalAddress() (net.IP, error) {
	var ip string
	var err error

	if u.ipService != nil {
		ip, err = u.ipService.GetExternalIPAddress()
	} else if u.pppService != nil {
		ip, err = u.pppService.GetExternalIPAddress()
	} else {
		return nil, errors.New("no UPnP service available")
	}

	if err != nil {
		return nil, err
	}
	return net.ParseIP(ip), nil
}

// AddPortMapping adds a port mapping
func (u *UPnPNAT) AddPortMapping(protocol string, externalPort, internalPort int, desc string, timeout time.Duration) error {
	var err error
	leaseDuration := uint32(timeout.Seconds())

	if u.ipService != nil {
		err = u.ipService.AddPortMapping("", uint16(externalPort), protocol, uint16(internalPort), "", true, desc, leaseDuration)
	} else if u.pppService != nil {
		err = u.pppService.AddPortMapping("", uint16(externalPort), protocol, uint16(internalPort), "", true, desc, leaseDuration)
	} else {
		return errors.New("no UPnP service available")
	}

	return err
}

// DeletePortMapping removes a port mapping
func (u *UPnPNAT) DeletePortMapping(protocol string, externalPort, internalPort int) error {
	var err error

	if u.ipService != nil {
		err = u.ipService.DeletePortMapping("", uint16(externalPort), protocol)
	} else if u.pppService != nil {
		err = u.pppService.DeletePortMapping("", uint16(externalPort), protocol)
	} else {
		return errors.New("no UPnP service available")
	}

	return err
}

// Type returns the NAT type
func (u *UPnPNAT) Type() string {
	return "UPnP-IGD"
}

// GetExternalAddress returns the external IP address
func (n *NATPMP) GetExternalAddress() (net.IP, error) {
	resp, err := n.client.GetExternalAddress()
	if err != nil {
		return nil, err
	}
	return net.IP(resp.ExternalIPAddress[:]), nil
}

// AddPortMapping adds a port mapping
func (n *NATPMP) AddPortMapping(protocol string, externalPort, internalPort int, desc string, timeout time.Duration) error {
	var lifetime int
	if timeout > 0 {
		lifetime = int(timeout.Seconds())
	} else {
		lifetime = 3600 // Default 1 hour
	}

	_, err := n.client.AddPortMapping(protocol, internalPort, externalPort, lifetime)
	return err
}

// DeletePortMapping removes a port mapping
func (n *NATPMP) DeletePortMapping(protocol string, externalPort, internalPort int) error {
	// NAT-PMP doesn't have a direct delete, set lifetime to 0
	_, err := n.client.AddPortMapping(protocol, internalPort, externalPort, 0)
	return err
}

// Type returns the NAT type
func (n *NATPMP) Type() string {
	return "NAT-PMP"
}

// AutoMap automatically maps ports and maintains them
type AutoMap struct {
	nat       NAT
	mappings  map[string]*Mapping
	mu        sync.RWMutex
	stopCh    chan struct{}
	refreshCh chan struct{}
}

// NewAutoMap creates a new AutoMap instance
func NewAutoMap(nat NAT) *AutoMap {
	return &AutoMap{
		nat:       nat,
		mappings:  make(map[string]*Mapping),
		stopCh:    make(chan struct{}),
		refreshCh: make(chan struct{}, 1),
	}
}

// Map adds a port mapping and maintains it
func (am *AutoMap) Map(protocol string, externalPort, internalPort int, desc string) (*Mapping, error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	key := mappingKey(protocol, externalPort, internalPort)
	if m, ok := am.mappings[key]; ok {
		return m, nil
	}

	// Get external IP
	extIP, err := am.nat.GetExternalAddress()
	if err != nil {
		return nil, err
	}

	// Add mapping with 1 hour timeout
	timeout := time.Hour
	err = am.nat.AddPortMapping(protocol, externalPort, internalPort, desc, timeout)
	if err != nil {
		return nil, err
	}

	m := &Mapping{
		Protocol:   protocol,
		ExternalIP: extIP,
		External:   externalPort,
		Internal:   internalPort,
		Desc:       desc,
		Expires:    time.Now().Add(timeout),
	}

	am.mappings[key] = m

	// Start refresh goroutine if not already running
	go am.refreshLoop()

	return m, nil
}

// Unmap removes a port mapping
func (am *AutoMap) Unmap(protocol string, externalPort, internalPort int) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	key := mappingKey(protocol, externalPort, internalPort)
	if _, ok := am.mappings[key]; !ok {
		return nil
	}

	err := am.nat.DeletePortMapping(protocol, externalPort, internalPort)
	if err != nil {
		return err
	}

	delete(am.mappings, key)
	return nil
}

// GetMapping returns a specific mapping
func (am *AutoMap) GetMapping(protocol string, externalPort, internalPort int) (*Mapping, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	key := mappingKey(protocol, externalPort, internalPort)
	m, ok := am.mappings[key]
	return m, ok
}

// GetAllMappings returns all active mappings
func (am *AutoMap) GetAllMappings() []*Mapping {
	am.mu.RLock()
	defer am.mu.RUnlock()

	mappings := make([]*Mapping, 0, len(am.mappings))
	for _, m := range am.mappings {
		mappings = append(mappings, m)
	}
	return mappings
}

// Stop stops the auto-refresh loop
func (am *AutoMap) Stop() {
	close(am.stopCh)
}

func (am *AutoMap) refreshLoop() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-am.stopCh:
			return
		case <-ticker.C:
			am.refreshMappings()
		case <-am.refreshCh:
			am.refreshMappings()
		}
	}
}

func (am *AutoMap) refreshMappings() {
	am.mu.Lock()
	defer am.mu.Unlock()

	now := time.Now()
	for key, m := range am.mappings {
		// Refresh if expiring within 10 minutes
		if m.Expires.Before(now.Add(10 * time.Minute)) {
			timeout := time.Hour
			err := am.nat.AddPortMapping(m.Protocol, m.External, m.Internal, m.Desc, timeout)
			if err != nil {
				log.Printf("[NAT] Failed to refresh mapping %s: %v", key, err)
				delete(am.mappings, key)
				continue
			}
			m.Expires = now.Add(timeout)
			log.Printf("[NAT] Refreshed mapping %s", key)
		}
	}
}

func mappingKey(protocol string, external, internal int) string {
	return fmt.Sprintf("%s:%d:%d", protocol, external, internal)
}

// GetLocalIP returns the local IP address
func GetLocalIP() (net.IP, error) {
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

			if ip == nil || ip.IsLoopback() {
				continue
			}

			if ip.To4() != nil {
				return ip, nil
			}
		}
	}

	return nil, errors.New("no local IP found")
}

// IsPrivateIP checks if an IP is in private range
func IsPrivateIP(ip net.IP) bool {
	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fc00::/7",
	}

	for _, cidr := range privateRanges {
		_, network, _ := net.ParseCIDR(cidr)
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// DetectNATType attempts to detect the NAT type
func DetectNATType() string {
	localIP, err := GetLocalIP()
	if err != nil {
		return "unknown"
	}

	if IsPrivateIP(localIP) {
		nat, err := Discover()
		if err != nil {
			return "symmetric"
		}

		extIP, err := nat.GetExternalAddress()
		if err != nil {
			return "symmetric"
		}

		if extIP.Equal(localIP) {
			return "none"
		}
		return "NAT"
	}

	return "none"
}

// ParsePort parses a port string to int
func ParsePort(port string) (int, error) {
	return strconv.Atoi(port)
}
