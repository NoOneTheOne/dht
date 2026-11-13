package dht

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var defaultBootstrapNodes = []string{
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
	"dht.transmissionbt.com:6881",
	"dht.aelitis.com:6881",
}

const bootstrapMinRetry = 15 * time.Second
const bootstrapMaxRetry = 5 * time.Minute

type bootstrapManager struct {
	dht            *DHT
	addresses      []string
	bootstrapMu    sync.Mutex
	lastBootstrap  time.Time
	bootstrapCount int
	joining        bool
	nextRetry      time.Time
	retryDelay     time.Duration
}

func newBootstrapManager(dht *DHT) *bootstrapManager {
	return &bootstrapManager{
		dht:        dht,
		addresses:  bootstrapAddresses(dht.PrimeNodes),
		retryDelay: bootstrapMinRetry,
	}
}

func bootstrapAddresses(configured []string) []string {
	addresses := make([]string, 0, len(configured)+len(defaultBootstrapNodes))
	seen := make(map[string]bool)
	for _, nodes := range [][]string{configured, defaultBootstrapNodes} {
		for _, addr := range nodes {
			addr = strings.TrimSpace(addr)
			if seen[addr] {
				continue
			}
			seen[addr] = true
			if _, _, err := parseBootstrapAddress(addr); err != nil {
				log.Printf("[Bootstrap] Ignoring invalid DHT bootstrap address %q: %v; use host:port, not a Tracker announce URL", addr, err)
				continue
			}
			addresses = append(addresses, addr)
		}
	}
	return addresses
}

func parseBootstrapAddress(address string) (string, int, error) {
	if strings.Contains(address, "://") {
		return "", 0, fmt.Errorf("URL endpoints are not DHT bootstrap nodes")
	}
	host, portString, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portString)
	if err != nil || port < 1 || port > 65535 || host == "" || strings.ContainsAny(host, "/?#@ \t\r\n") {
		return "", 0, fmt.Errorf("expected a hostname or IP and a port between 1 and 65535")
	}
	return host, port, nil
}

func resolveBootstrapAddress(ctx context.Context, network, address string) (*net.UDPAddr, error) {
	host, port, err := parseBootstrapAddress(address)
	if err != nil {
		return nil, err
	}
	family := "ip"
	if network == "udp4" {
		family = "ip4"
	} else if network == "udp6" {
		family = "ip6"
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, family, host)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		ip = ip.Unmap()
		if (network == "udp4" && !ip.Is4()) || (network == "udp6" && !ip.Is6()) {
			continue
		}
		if !ip.IsUnspecified() && !ip.IsMulticast() {
			return &net.UDPAddr{IP: net.IP(ip.AsSlice()), Port: port, Zone: ip.Zone()}, nil
		}
	}
	return nil, fmt.Errorf("no usable address for %q", address)
}

func (bm *bootstrapManager) join() {
	bm.bootstrapMu.Lock()
	if bm.joining || time.Now().Before(bm.nextRetry) {
		bm.bootstrapMu.Unlock()
		return
	}
	bm.joining = true
	bm.lastBootstrap = time.Now()
	bm.bootstrapCount++
	attempt := bm.bootstrapCount
	bm.bootstrapMu.Unlock()

	defer func() {
		bm.bootstrapMu.Lock()
		defer bm.bootstrapMu.Unlock()
		bm.joining = false
		bm.nextRetry = time.Now().Add(bm.retryDelay)
		bm.retryDelay = min(bm.retryDelay*2, bootstrapMaxRetry)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addresses := bm.addresses
	resolved := make(chan *net.UDPAddr, len(addresses))
	for _, address := range addresses {
		go func(address string) {
			addr, err := resolveBootstrapAddress(ctx, bm.dht.Network, address)
			if err != nil {
				log.Printf("[Bootstrap] Failed to resolve %s: %v", address, err)
			}
			resolved <- addr
		}(address)
	}

	attempted := 0
	for range addresses {
		select {
		case addr := <-resolved:
			if addr != nil {
				bm.dht.transactionManager.findNode(&node{addr: addr}, bm.dht.node.id.RawString())
				attempted++
			}
		case <-ctx.Done():
			log.Printf("[Bootstrap] Attempt #%d timed out; attempted %d queries", attempt, attempted)
			return
		}
	}
	log.Printf("[Bootstrap] Attempt #%d finished; attempted %d queries (responses pending)", attempt, attempted)
}

func (bm *bootstrapManager) needsRebootstrap() bool {
	nodes := bm.dht.routingTable.Len()
	bm.bootstrapMu.Lock()
	defer bm.bootstrapMu.Unlock()
	if nodes >= 8 {
		bm.retryDelay = bootstrapMinRetry
		return false
	}
	return !bm.joining && !time.Now().Before(bm.nextRetry)
}

func (bm *bootstrapManager) getBootstrapStats() (count int, lastTime time.Time) {
	bm.bootstrapMu.Lock()
	defer bm.bootstrapMu.Unlock()
	return bm.bootstrapCount, bm.lastBootstrap
}
