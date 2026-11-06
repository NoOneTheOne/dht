package dht

import (
	"context"
	"log"
	"sync"
	"time"
)

// Parallel Query Engine for DHT operations

// QueryType represents the type of DHT query
type QueryType int

const (
	QueryFindNode QueryType = iota
	QueryGetPeers
	QueryAnnouncePeer
	QueryPing
)

// QueryRequest represents a single query request
type QueryRequest struct {
	Type     QueryType
	Target   string // target node ID or infohash
	Node     *node
	InfoHash []byte // for get_peers/announce_peer
	PeerIP   string // for announce_peer
	PeerPort int    // for announce_peer
}

// QueryResponse represents a query response
type QueryResponse struct {
	Request   *QueryRequest
	Nodes     []*node
	Peers     []*Peer
	Success   bool
	Error     error
	Latency   time.Duration
	Timestamp time.Time
}

// QueryOptions configures parallel query behavior
type QueryOptions struct {
	MaxConcurrency int           // Maximum concurrent queries
	Timeout        time.Duration // Per-query timeout
	TotalTimeout   time.Duration // Overall timeout
	RetryCount     int           // Number of retries
	Alpha          int           // Number of parallel queries per round (typically 3)
	CollectResults bool          // Whether to collect all results
}

// DefaultQueryOptions returns default query options
func DefaultQueryOptions() *QueryOptions {
	return &QueryOptions{
		MaxConcurrency: 16,
		Timeout:        15 * time.Second,
		TotalTimeout:   60 * time.Second,
		RetryCount:     2,
		Alpha:          3,
		CollectResults: true,
	}
}

// ParallelQueryEngine manages parallel DHT queries
type ParallelQueryEngine struct {
	dht        *DHT
	options    *QueryOptions
	queryChan  chan *QueryRequest
	resultChan chan *QueryResponse
	workerWg   sync.WaitGroup
	mu         sync.RWMutex
	running    bool
	stopChan   chan struct{}

	// Statistics
	stats   QueryStats
	statsMu sync.Mutex
}

// QueryStats holds query engine statistics
type QueryStats struct {
	TotalQueries      int64
	SuccessfulQueries int64
	FailedQueries     int64
	AvgLatency        time.Duration
	MaxLatency        time.Duration
	MinLatency        time.Duration
	TotalNodesFound   int64
	TotalPeersFound   int64
}

// NewParallelQueryEngine creates a new parallel query engine
func NewParallelQueryEngine(dht *DHT, options *QueryOptions) *ParallelQueryEngine {
	if options == nil {
		options = DefaultQueryOptions()
	}

	return &ParallelQueryEngine{
		dht:        dht,
		options:    options,
		queryChan:  make(chan *QueryRequest, 1024),
		resultChan: make(chan *QueryResponse, 1024),
		stopChan:   make(chan struct{}),
		stats: QueryStats{
			MinLatency: time.Hour, // Initialize to large value
		},
	}
}

// Start starts the query engine workers
func (pqe *ParallelQueryEngine) Start() {
	pqe.mu.Lock()
	if pqe.running {
		pqe.mu.Unlock()
		return
	}
	pqe.running = true
	pqe.mu.Unlock()

	// Start worker goroutines
	for i := 0; i < pqe.options.MaxConcurrency; i++ {
		pqe.workerWg.Add(1)
		go pqe.worker()
	}

	// Start result collector
	go pqe.collectResults()

	log.Printf("[QueryEngine] Started with %d workers", pqe.options.MaxConcurrency)
}

// Stop stops the query engine
func (pqe *ParallelQueryEngine) Stop() {
	pqe.mu.Lock()
	if !pqe.running {
		pqe.mu.Unlock()
		return
	}
	pqe.running = false
	close(pqe.stopChan)
	pqe.mu.Unlock()

	pqe.workerWg.Wait()
	log.Printf("[QueryEngine] Stopped")
}

// worker processes query requests
func (pqe *ParallelQueryEngine) worker() {
	defer pqe.workerWg.Done()

	for {
		select {
		case <-pqe.stopChan:
			return
		case req := <-pqe.queryChan:
			response := pqe.executeQuery(req)
			pqe.resultChan <- response
		}
	}
}

// executeQuery executes a single query
func (pqe *ParallelQueryEngine) executeQuery(req *QueryRequest) *QueryResponse {
	start := time.Now()
	response := &QueryResponse{
		Request:   req,
		Timestamp: start,
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), pqe.options.Timeout)
	defer cancel()

	// Execute query based on type
	var err error
	switch req.Type {
	case QueryFindNode:
		err = pqe.executeFindNode(ctx, req, response)
	case QueryGetPeers:
		err = pqe.executeGetPeers(ctx, req, response)
	case QueryPing:
		err = pqe.executePing(ctx, req, response)
	default:
		err = ErrUnsupportedQueryType
	}

	response.Latency = time.Since(start)
	response.Error = err
	response.Success = err == nil

	// Update statistics
	pqe.updateStats(response)

	return response
}

// executeFindNode executes a find_node query
func (pqe *ParallelQueryEngine) executeFindNode(ctx context.Context, req *QueryRequest, resp *QueryResponse) error {
	// Use transaction manager to send find_node
	done := make(chan struct{})

	go func() {
		defer close(done)
		pqe.dht.transactionManager.findNode(req.Node, req.Target)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		// Query completed, collect results from routing table
		neighbors := pqe.dht.routingTable.GetNeighbors(newBitmapFromString(req.Target), pqe.dht.K)
		resp.Nodes = neighbors
		return nil
	}
}

// executeGetPeers executes a get_peers query
func (pqe *ParallelQueryEngine) executeGetPeers(ctx context.Context, req *QueryRequest, resp *QueryResponse) error {
	done := make(chan struct{})

	go func() {
		defer close(done)
		pqe.dht.transactionManager.getPeers(req.Node, string(req.InfoHash))
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		// Get peers from peer manager
		peers := pqe.dht.peersManager.GetPeers(string(req.InfoHash), pqe.dht.K)
		resp.Peers = peers
		return nil
	}
}

// executePing executes a ping query
func (pqe *ParallelQueryEngine) executePing(ctx context.Context, req *QueryRequest, resp *QueryResponse) error {
	done := make(chan struct{})

	go func() {
		defer close(done)
		pqe.dht.transactionManager.ping(req.Node)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// collectResults collects and aggregates query results
func (pqe *ParallelQueryEngine) collectResults() {
	for {
		select {
		case <-pqe.stopChan:
			return
		case resp := <-pqe.resultChan:
			if resp.Success {
				pqe.statsMu.Lock()
				pqe.stats.SuccessfulQueries++
				if len(resp.Nodes) > 0 {
					pqe.stats.TotalNodesFound += int64(len(resp.Nodes))
				}
				if len(resp.Peers) > 0 {
					pqe.stats.TotalPeersFound += int64(len(resp.Peers))
				}
				pqe.statsMu.Unlock()
			} else {
				pqe.statsMu.Lock()
				pqe.stats.FailedQueries++
				pqe.statsMu.Unlock()
			}
		}
	}
}

// updateStats updates query statistics
func (pqe *ParallelQueryEngine) updateStats(resp *QueryResponse) {
	pqe.statsMu.Lock()
	defer pqe.statsMu.Unlock()

	pqe.stats.TotalQueries++

	// Update latency stats
	if resp.Latency > pqe.stats.MaxLatency {
		pqe.stats.MaxLatency = resp.Latency
	}
	if resp.Latency < pqe.stats.MinLatency {
		pqe.stats.MinLatency = resp.Latency
	}

	// Update average latency
	if pqe.stats.TotalQueries == 1 {
		pqe.stats.AvgLatency = resp.Latency
	} else {
		total := pqe.stats.TotalQueries
		pqe.stats.AvgLatency = (pqe.stats.AvgLatency*time.Duration(total-1) + resp.Latency) / time.Duration(total)
	}
}

// Submit submits a query request
func (pqe *ParallelQueryEngine) Submit(req *QueryRequest) {
	select {
	case pqe.queryChan <- req:
	default:
		log.Printf("[QueryEngine] Query queue full, dropping request")
	}
}

// GetStats returns current query statistics
func (pqe *ParallelQueryEngine) GetStats() QueryStats {
	pqe.statsMu.Lock()
	defer pqe.statsMu.Unlock()
	return pqe.stats
}

// IterativeFindNode performs iterative find_node operation (BEP-5)
func (pqe *ParallelQueryEngine) IterativeFindNode(target string) []*node {
	ctx, cancel := context.WithTimeout(context.Background(), pqe.options.TotalTimeout)
	defer cancel()

	// Get initial closest nodes from routing table
	closest := pqe.dht.routingTable.GetNeighbors(newBitmapFromString(target), pqe.options.Alpha)
	if len(closest) == 0 {
		return nil
	}

	// Track visited nodes
	visited := make(map[string]bool)
	var result []*node
	var mu sync.Mutex

	// Iterative lookup
	for round := 0; round < 10; round++ { // Max 10 rounds
		select {
		case <-ctx.Done():
			return result
		default:
		}

		// Query alpha closest unvisited nodes in parallel
		var wg sync.WaitGroup
		newNodes := make(chan []*node, pqe.options.Alpha)

		queried := 0
		for _, n := range closest {
			if queried >= pqe.options.Alpha {
				break
			}

			nodeID := n.id.RawString()
			mu.Lock()
			if visited[nodeID] {
				mu.Unlock()
				continue
			}
			visited[nodeID] = true
			mu.Unlock()

			wg.Add(1)
			queried++

			go func(n *node) {
				defer wg.Done()

				req := &QueryRequest{
					Type:   QueryFindNode,
					Target: target,
					Node:   n,
				}

				resp := pqe.executeQuery(req)
				if resp.Success && len(resp.Nodes) > 0 {
					newNodes <- resp.Nodes
				}
			}(n)
		}

		// Wait for all queries in this round
		wg.Wait()
		close(newNodes)

		// Collect new nodes
		var allNewNodes []*node
		for nodes := range newNodes {
			allNewNodes = append(allNewNodes, nodes...)
		}

		if len(allNewNodes) == 0 {
			break // No new nodes found, we're done
		}

		// Add to result
		mu.Lock()
		result = append(result, allNewNodes...)
		mu.Unlock()

		// Update closest nodes for next round
		closest = allNewNodes
	}

	return result
}

// IterativeGetPeers performs iterative get_peers operation
func (pqe *ParallelQueryEngine) IterativeGetPeers(infoHash []byte) []*Peer {
	ctx, cancel := context.WithTimeout(context.Background(), pqe.options.TotalTimeout)
	defer cancel()

	target := string(infoHash)

	// Get initial closest nodes
	closest := pqe.dht.routingTable.GetNeighbors(newBitmapFromString(target), pqe.options.Alpha)
	if len(closest) == 0 {
		return nil
	}

	visited := make(map[string]bool)
	var peers []*Peer
	var peersMu sync.Mutex

	for round := 0; round < 10; round++ {
		select {
		case <-ctx.Done():
			peersMu.Lock()
			result := make([]*Peer, len(peers))
			copy(result, peers)
			peersMu.Unlock()
			return result
		default:
		}

		var wg sync.WaitGroup
		newData := make(chan *QueryResponse, pqe.options.Alpha)

		queried := 0
		for _, n := range closest {
			if queried >= pqe.options.Alpha {
				break
			}

			nodeID := n.id.RawString()
			if visited[nodeID] {
				continue
			}
			visited[nodeID] = true

			wg.Add(1)
			queried++

			go func(n *node) {
				defer wg.Done()

				req := &QueryRequest{
					Type:     QueryGetPeers,
					Target:   target,
					Node:     n,
					InfoHash: infoHash,
				}

				resp := pqe.executeQuery(req)
				newData <- resp
			}(n)
		}

		wg.Wait()
		close(newData)

		var newNodes []*node
		for resp := range newData {
			if resp.Success {
				if len(resp.Peers) > 0 {
					peersMu.Lock()
					peers = append(peers, resp.Peers...)
					peersMu.Unlock()
				}
				if len(resp.Nodes) > 0 {
					newNodes = append(newNodes, resp.Nodes...)
				}
			}
		}

		if len(newNodes) == 0 {
			break
		}

		closest = newNodes
	}

	peersMu.Lock()
	result := make([]*Peer, len(peers))
	copy(result, peers)
	peersMu.Unlock()

	return result
}

// Error types
var (
	ErrUnsupportedQueryType = &QueryError{Message: "unsupported query type"}
	ErrQueryTimeout         = &QueryError{Message: "query timeout"}
	ErrQueryFailed          = &QueryError{Message: "query failed"}
)

// QueryError represents a query error
type QueryError struct {
	Message string
}

func (e *QueryError) Error() string {
	return e.Message
}
