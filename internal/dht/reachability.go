package dht

import (
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type reachabilityCheck struct {
	mu        sync.Mutex
	contacted map[string]struct{}
	inbound   chan *net.UDPAddr
	responses bool
	stopped   atomic.Bool
}

func newReachabilityCheck() *reachabilityCheck {
	return &reachabilityCheck{
		contacted: make(map[string]struct{}),
		inbound:   make(chan *net.UDPAddr, 1),
	}
}

func (check *reachabilityCheck) recordSend(addr *net.UDPAddr) {
	if check.stopped.Load() {
		return
	}
	check.mu.Lock()
	defer check.mu.Unlock()
	if !check.stopped.Load() {
		check.contacted[addr.IP.String()] = struct{}{}
	}
}

func (check *reachabilityCheck) unsolicited(addr *net.UDPAddr) bool {
	if check.stopped.Load() || !addr.IP.IsGlobalUnicast() || addr.IP.IsPrivate() ||
		(&net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}).Contains(addr.IP) {
		return false
	}
	check.mu.Lock()
	defer check.mu.Unlock()
	_, contacted := check.contacted[addr.IP.String()]
	return !check.stopped.Load() && !contacted
}

func (check *reachabilityCheck) recordInbound(addr *net.UDPAddr) {
	select {
	case check.inbound <- addr:
	default:
	}
}

func (check *reachabilityCheck) recordResponse() {
	if check.stopped.Load() {
		return
	}
	check.mu.Lock()
	defer check.mu.Unlock()
	check.responses = true
}

func (check *reachabilityCheck) wait(ctx context.Context) (*net.UDPAddr, bool) {
	var addr *net.UDPAddr
	select {
	case addr = <-check.inbound:
	case <-ctx.Done():
	}
	check.mu.Lock()
	defer check.mu.Unlock()
	check.stopped.Store(true)
	check.contacted = nil
	return addr, check.responses
}

func (check *reachabilityCheck) logStartup(port int) {
	log.Printf("[DHT Reachability] Checking public UDP ingress to local port %d for 30s", port)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	addr, responses := check.wait(ctx)
	if addr != nil {
		log.Printf("[DHT Reachability] UDP %d: public inbound traffic observed from %s (valid unsolicited DHT request); exact public NAT port is not verified", port, addr)
		return
	}
	log.Printf("[DHT Reachability] UDP %d: UNKNOWN; no unsolicited public DHT request observed in 30s; outbound DHT responses=%t (responses do not prove inbound reachability); check UDP NAT/firewall or use an external probe", port, responses)
}
