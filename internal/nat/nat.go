// Package nat provides NAT traversal functionality including UPnP and NAT-PMP support.
package nat

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/huin/goupnp/dcps/internetgateway2"
	"github.com/huin/goupnp/soap"
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
	lease      time.Duration
	// Renewal backoff after failures; zero means renew on schedule.
	retryAt  time.Time
	failures int
}

// Active reports whether the gateway lease is still in force at now.
func (m *Mapping) Active(now time.Time) bool {
	return m.Expires.IsZero() || now.Before(m.Expires)
}

// NAT interface for NAT traversal operations
type NAT interface {
	GetExternalAddress() (net.IP, error)
	AddPortMapping(protocol string, externalPort, internalPort int, desc string, timeout time.Duration) (int, time.Duration, error)
	DeletePortMapping(protocol string, externalPort, internalPort int) error
	Type() string
}

// UPnPNAT implements NAT using UPnP-IGD
type UPnPNAT struct {
	ipService  *internetgateway2.WANIPConnection1
	pppService *internetgateway2.WANPPPConnection1
	devURL     string
	localIP    net.IP
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
	if err == nil {
		for _, service := range ipServices {
			localIP, err := localIPv4ForGateway(service.ServiceClient.Location)
			if err != nil {
				continue
			}
			return &UPnPNAT{
				ipService: service,
				devURL:    service.ServiceClient.Location.String(),
				localIP:   localIP,
			}, nil
		}
	}

	// Try WANPPPConnection1
	pppServices, _, err := internetgateway2.NewWANPPPConnection1Clients()
	if err == nil {
		for _, service := range pppServices {
			localIP, err := localIPv4ForGateway(service.ServiceClient.Location)
			if err != nil {
				continue
			}
			return &UPnPNAT{
				pppService: service,
				devURL:     service.ServiceClient.Location.String(),
				localIP:    localIP,
			}, nil
		}
	}

	return nil, errors.New("UPnP not available")
}

// localIPv4ForGateway selects the source IPv4 for the gateway route without sending packets.
func localIPv4ForGateway(location *url.URL) (net.IP, error) {
	if location == nil || location.Hostname() == "" {
		return nil, errors.New("missing UPnP gateway location")
	}
	port := location.Port()
	if port == "" {
		port = "80"
		if location.Scheme == "https" {
			port = "443"
		}
	}
	conn, err := net.Dial("udp4", net.JoinHostPort(location.Hostname(), port))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || addr.IP.To4() == nil || addr.IP.IsUnspecified() {
		return nil, errors.New("no local IPv4 route to UPnP gateway")
	}
	return addr.IP.To4(), nil
}

func discoverNATPMP() (*NATPMP, error) {
	gwIP, err := gateway.DiscoverGateway()
	if err != nil {
		return nil, err
	}

	if gwIP == nil {
		return nil, errors.New("no gateway found")
	}

	// Bound every request; the default client retries for about 128s.
	client := natpmp.NewClientWithTimeout(gwIP, 3*time.Second)
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
func (u *UPnPNAT) AddPortMapping(protocol string, externalPort, internalPort int, desc string, timeout time.Duration) (int, time.Duration, error) {
	if u.localIP.To4() == nil || u.localIP.IsUnspecified() {
		return 0, 0, errors.New("no local IPv4 address for UPnP gateway")
	}
	if externalPort <= 0 || externalPort > 65535 || internalPort <= 0 || internalPort > 65535 {
		return 0, 0, errors.New("UPnP mapping ports must be between 1 and 65535")
	}
	seconds := timeout / time.Second
	if timeout < 0 || seconds > 1<<32-1 || (timeout > 0 && seconds == 0) {
		return 0, 0, errors.New("invalid UPnP lease duration")
	}
	var err error
	leaseDuration := uint32(seconds)
	protocol = strings.ToUpper(protocol)

	add := func(lease uint32) error {
		if u.ipService != nil {
			return u.ipService.AddPortMapping("", uint16(externalPort), protocol, uint16(internalPort), u.localIP.String(), true, desc, lease)
		}
		if u.pppService != nil {
			return u.pppService.AddPortMapping("", uint16(externalPort), protocol, uint16(internalPort), u.localIP.String(), true, desc, lease)
		}
		return errors.New("no UPnP service available")
	}
	err = add(leaseDuration)
	var fault *soap.SOAPFaultError
	if err != nil && leaseDuration != 0 && errors.As(err, &fault) && fault.Detail.UPnPError.Errorcode == 725 {
		// OnlyPermanentLeasesSupported: request a permanent mapping instead.
		leaseDuration = 0
		err = add(0)
	}
	if err != nil {
		return 0, 0, err
	}
	return externalPort, time.Duration(leaseDuration) * time.Second, nil
}

// DeletePortMapping removes a port mapping
func (u *UPnPNAT) DeletePortMapping(protocol string, externalPort, internalPort int) error {
	var err error
	protocol = strings.ToUpper(protocol)

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
func (n *NATPMP) AddPortMapping(protocol string, externalPort, internalPort int, desc string, timeout time.Duration) (int, time.Duration, error) {
	var lifetime int
	if timeout > 0 {
		lifetime = int(timeout.Seconds())
	} else {
		lifetime = 3600 // Default 1 hour
	}

	resp, err := n.client.AddPortMapping(protocol, internalPort, externalPort, lifetime)
	if err != nil {
		return 0, 0, err
	}
	if resp.MappedExternalPort == 0 || resp.PortMappingLifetimeInSeconds == 0 || int(resp.InternalPort) != internalPort {
		return 0, 0, errors.New("invalid NAT-PMP mapping response")
	}
	return int(resp.MappedExternalPort), time.Duration(resp.PortMappingLifetimeInSeconds) * time.Second, nil
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
	startOnce sync.Once
	stopOnce  sync.Once
	workers   sync.WaitGroup
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

	select {
	case <-am.stopCh:
		return nil, errors.New("port mapping is stopped")
	default:
	}
	key := mappingKey(protocol, externalPort, internalPort)
	if m, ok := am.mappings[key]; ok {
		copy := *m
		return &copy, nil
	}

	// Get external IP
	extIP, err := am.nat.GetExternalAddress()
	if err != nil {
		return nil, err
	}

	// Request one hour, but track the lease and port actually granted by the gateway.
	external, lease, err := am.nat.AddPortMapping(protocol, externalPort, internalPort, desc, time.Hour)
	if err != nil {
		return nil, err
	}
	if external <= 0 || external > 65535 || lease < 0 {
		return nil, errors.New("invalid port mapping response")
	}
	expires := time.Time{}
	if lease > 0 {
		expires = time.Now().Add(lease)
	}

	m := &Mapping{
		Protocol:   protocol,
		ExternalIP: extIP,
		External:   external,
		Internal:   internalPort,
		Desc:       desc,
		Expires:    expires,
		lease:      lease,
	}

	am.mappings[key] = m

	am.startOnce.Do(func() {
		am.workers.Add(1)
		go func() {
			defer am.workers.Done()
			am.refreshLoop()
		}()
	})

	copy := *m
	return &copy, nil
}

// Unmap removes a port mapping
func (am *AutoMap) Unmap(protocol string, externalPort, internalPort int) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	key := mappingKey(protocol, externalPort, internalPort)
	m, ok := am.mappings[key]
	if !ok {
		return nil
	}

	err := am.nat.DeletePortMapping(m.Protocol, m.External, m.Internal)
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
	if !ok {
		return nil, false
	}
	copy := *m
	return &copy, true
}

// GetAllMappings returns all active mappings
func (am *AutoMap) GetAllMappings() []*Mapping {
	am.mu.RLock()
	defer am.mu.RUnlock()

	mappings := make([]*Mapping, 0, len(am.mappings))
	for _, m := range am.mappings {
		copy := *m
		mappings = append(mappings, &copy)
	}
	return mappings
}

// Stop stops the auto-refresh loop
func (am *AutoMap) Stop() {
	am.mu.Lock()
	am.stopOnce.Do(func() { close(am.stopCh) })
	am.mu.Unlock()
	am.workers.Wait()
}

func (am *AutoMap) refreshLoop() {
	ticker := time.NewTicker(250 * time.Millisecond)
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

const (
	renewRetryMin = 5 * time.Second
	renewRetryMax = 5 * time.Minute
)

// refreshMappings renews finite leases halfway through. Gateway I/O happens
// outside am.mu so Stop and readers never wait on a slow router, and failed
// renewals back off exponentially instead of retrying on every tick.
func (am *AutoMap) refreshMappings() {
	type renewal struct {
		key                string
		protocol, desc     string
		external, internal int
		lease              time.Duration
	}
	am.mu.Lock()
	select {
	case <-am.stopCh:
		am.mu.Unlock()
		return
	default:
	}
	now := time.Now()
	var due []renewal
	for key, m := range am.mappings {
		// A zero lease is permanent (UPnP).
		if m.Expires.IsZero() || now.Before(m.Expires.Add(-m.lease/2)) || now.Before(m.retryAt) {
			continue
		}
		due = append(due, renewal{key, m.Protocol, m.Desc, m.External, m.Internal, m.lease})
	}
	am.mu.Unlock()

	for _, r := range due {
		external, lease, err := am.nat.AddPortMapping(r.protocol, r.external, r.internal, r.desc, r.lease)
		if err == nil && (external <= 0 || external > 65535 || lease < 0) {
			err = errors.New("invalid port mapping response")
		}
		am.mu.Lock()
		m, ok := am.mappings[r.key]
		stopped := false
		select {
		case <-am.stopCh:
			stopped = true
		default:
		}
		if !ok || stopped {
			am.mu.Unlock()
			continue
		}
		if err != nil {
			m.failures++
			delay := min(renewRetryMin<<min(m.failures-1, 16), renewRetryMax)
			m.retryAt = time.Now().Add(delay)
			am.mu.Unlock()
			log.Printf("[NAT] Failed to refresh mapping %s (attempt %d, retry in %s): %v", r.key, m.failures, delay, err)
			continue
		}
		m.External = external
		m.lease = lease
		m.failures, m.retryAt = 0, time.Time{}
		m.Expires = time.Time{}
		if lease > 0 {
			m.Expires = time.Now().Add(lease)
		}
		am.mu.Unlock()
		log.Printf("[NAT] Refreshed mapping %s at external port %d", r.key, external)
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
