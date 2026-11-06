package dht

import (
	"log"
	"net"
	"sync"
	"time"
)

// Default bootstrap nodes from mainline DHT network
var defaultBootstrapNodes = []string{
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
	"dht.transmissionbt.com:6881",
	"dht.aelitis.com:6881",     // Vuze
	"router.bitcomet.com:6881", // BitComet
}

// bootstrapManager handles the DHT bootstrap process
type bootstrapManager struct {
	dht            *DHT
	bootstrapMu    sync.Mutex
	lastBootstrap  time.Time
	bootstrapCount int
}

// newBootstrapManager creates a new bootstrap manager
func newBootstrapManager(dht *DHT) *bootstrapManager {
	return &bootstrapManager{
		dht: dht,
	}
}

// join makes current node join the dht network with improved logic
func (bm *bootstrapManager) join() {
	bm.bootstrapMu.Lock()
	defer bm.bootstrapMu.Unlock()

	// Combine configured prime nodes with default bootstrap nodes
	allNodes := make([]string, 0, len(bm.dht.PrimeNodes)+len(defaultBootstrapNodes))
	allNodes = append(allNodes, bm.dht.PrimeNodes...)

	// Add default nodes that aren't already in the list
	existing := make(map[string]bool)
	for _, n := range bm.dht.PrimeNodes {
		existing[n] = true
	}
	for _, n := range defaultBootstrapNodes {
		if !existing[n] {
			allNodes = append(allNodes, n)
		}
	}

	if len(allNodes) == 0 {
		log.Printf("[Bootstrap] No bootstrap nodes available")
		return
	}

	log.Printf("[Bootstrap] Attempting to join DHT network via %d nodes", len(allNodes))

	// Use parallel bootstrap with wait group
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// Set a timeout for the entire bootstrap process
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	for _, addr := range allNodes {
		wg.Add(1)
		go func(nodeAddr string) {
			defer wg.Done()

			select {
			case <-done:
				return // Timeout reached
			default:
			}

			realAddr, err := net.ResolveUDPAddr(bm.dht.Network, nodeAddr)
			if err != nil {
				log.Printf("[Bootstrap] Failed to resolve %s: %v", nodeAddr, err)
				return
			}

			// Send find_node to bootstrap the routing table
			tempNode := &node{addr: realAddr}
			bm.dht.transactionManager.findNode(tempNode, bm.dht.node.id.RawString())

			mu.Lock()
			successCount++
			mu.Unlock()

			log.Printf("[Bootstrap] Sent find_node to %s", nodeAddr)
		}(addr)
	}

	// Wait with timeout
	select {
	case <-done:
		// All goroutines completed
	case <-time.After(30 * time.Second):
		log.Printf("[Bootstrap] Bootstrap process timed out")
	}

	bm.lastBootstrap = time.Now()
	bm.bootstrapCount++

	log.Printf("[Bootstrap] Bootstrap attempt #%d completed, contacted %d nodes",
		bm.bootstrapCount, successCount)
}

// needsRebootstrap checks if we need to re-bootstrap
func (bm *bootstrapManager) needsRebootstrap() bool {
	// Re-bootstrap if routing table is empty
	if bm.dht.routingTable.Len() == 0 {
		return true
	}

	// Re-bootstrap if last bootstrap was more than 5 minutes ago and we have few nodes
	if time.Since(bm.lastBootstrap) > 5*time.Minute && bm.dht.routingTable.Len() < 8 {
		return true
	}

	return false
}

// getBootstrapStats returns bootstrap statistics
func (bm *bootstrapManager) getBootstrapStats() (count int, lastTime time.Time) {
	bm.bootstrapMu.Lock()
	defer bm.bootstrapMu.Unlock()
	return bm.bootstrapCount, bm.lastBootstrap
}
