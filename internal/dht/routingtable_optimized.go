package dht

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// BEP-5 Routing Table Optimizations

// NodeQuality tracks the quality/reliability of a node
type NodeQuality struct {
	SuccessCount   int32
	FailureCount   int32
	LastSeen       time.Time
	ResponseTimeMs int64 // Average response time in milliseconds
}

// UpdateSuccess records a successful interaction with the node
func (nq *NodeQuality) UpdateSuccess(responseTime time.Duration) {
	atomic.AddInt32(&nq.SuccessCount, 1)
	atomic.StoreInt32(&nq.FailureCount, 0) // Reset failure count on success
	nq.LastSeen = time.Now()

	// Update average response time (exponential moving average)
	rtMs := responseTime.Milliseconds()
	currentRT := atomic.LoadInt64(&nq.ResponseTimeMs)
	if currentRT == 0 {
		atomic.StoreInt64(&nq.ResponseTimeMs, rtMs)
	} else {
		// EMA with alpha = 0.3
		newRT := int64(float64(currentRT)*0.7 + float64(rtMs)*0.3)
		atomic.StoreInt64(&nq.ResponseTimeMs, newRT)
	}
}

// UpdateFailure records a failed interaction with the node
func (nq *NodeQuality) UpdateFailure() {
	atomic.AddInt32(&nq.FailureCount, 1)
	nq.LastSeen = time.Now()
}

// GetFailureCount returns the current failure count
func (nq *NodeQuality) GetFailureCount() int32 {
	return atomic.LoadInt32(&nq.FailureCount)
}

// IsReliable returns true if the node is considered reliable
func (nq *NodeQuality) IsReliable() bool {
	// Node is unreliable if it has failed more than 3 times consecutively
	return nq.GetFailureCount() < 3
}

// ShouldEvict returns true if the node should be evicted from routing table
func (nq *NodeQuality) ShouldEvict(maxAge time.Duration) bool {
	// Evict if not seen for maxAge duration
	if time.Since(nq.LastSeen) > maxAge {
		return true
	}
	// Evict if too many consecutive failures
	if !nq.IsReliable() {
		return true
	}
	return false
}

// nodeQualityMap tracks quality metrics for nodes
type nodeQualityMap struct {
	mu        sync.RWMutex
	qualities map[string]*NodeQuality
}

var globalNodeQuality = &nodeQualityMap{
	qualities: make(map[string]*NodeQuality),
}

// GetNodeQuality returns or creates quality tracking for a node
func GetNodeQuality(nodeID string) *NodeQuality {
	globalNodeQuality.mu.RLock()
	if nq, ok := globalNodeQuality.qualities[nodeID]; ok {
		globalNodeQuality.mu.RUnlock()
		return nq
	}
	globalNodeQuality.mu.RUnlock()

	globalNodeQuality.mu.Lock()
	defer globalNodeQuality.mu.Unlock()
	// Double-check after acquiring write lock
	if nq, ok := globalNodeQuality.qualities[nodeID]; ok {
		return nq
	}
	nq := &NodeQuality{
		LastSeen: time.Now(),
	}
	globalNodeQuality.qualities[nodeID] = nq
	return nq
}

// CleanupNodeQuality removes quality tracking for a node
func CleanupNodeQuality(nodeID string) {
	globalNodeQuality.mu.Lock()
	defer globalNodeQuality.mu.Unlock()
	delete(globalNodeQuality.qualities, nodeID)
}

// RoutingTableStats holds statistics about the routing table
type RoutingTableStats struct {
	TotalNodes        int
	TotalBuckets      int
	NonEmptyBuckets   int
	AvgNodesPerBucket float64
	OldestNode        time.Time
	NewestNode        time.Time
}

// GetRoutingTableStats returns statistics about the routing table
func (rt *routingTable) GetStats() RoutingTableStats {
	rt.RLock()
	defer rt.RUnlock()

	stats := RoutingTableStats{
		TotalNodes: rt.cachedNodes.Len(),
	}

	now := time.Now()
	stats.OldestNode = now
	stats.NewestNode = time.Time{}

	bucketCount := 0
	nonEmptyCount := 0

	for e := range rt.cachedKBuckets.Iter() {
		bucket := e.Value.(*KBucket)
		bucketCount++

		nodeCount := bucket.nodes.Len()
		if nodeCount > 0 {
			nonEmptyCount++

			// Check node timestamps
			for n := range bucket.nodes.Iter() {
				node := n.Value.(*node)
				if node.lastActiveTime.Before(stats.OldestNode) {
					stats.OldestNode = node.lastActiveTime
				}
				if node.lastActiveTime.After(stats.NewestNode) {
					stats.NewestNode = node.lastActiveTime
				}
			}
		}
	}

	stats.TotalBuckets = bucketCount
	stats.NonEmptyBuckets = nonEmptyCount
	if nonEmptyCount > 0 {
		stats.AvgNodesPerBucket = float64(stats.TotalNodes) / float64(nonEmptyCount)
	}

	return stats
}

// OptimizedRefresh performs a smarter refresh of the routing table
// It prioritizes buckets that are:
// 1. Older (haven't been refreshed recently)
// 2. Have fewer nodes (need more coverage)
// 3. Cover ID ranges we haven't explored much
func (rt *routingTable) OptimizedRefresh() {
	now := time.Now()

	type bucketPriority struct {
		bucket   *KBucket
		priority float64
	}

	var priorities []bucketPriority

	rt.RLock()
	for e := range rt.cachedKBuckets.Iter() {
		bucket := e.Value.(*KBucket)
		age := now.Sub(bucket.LastChanged())
		nodeCount := bucket.nodes.Len()

		// Priority = age factor + scarcity factor
		// Older buckets and buckets with fewer nodes get higher priority
		ageFactor := float64(age.Minutes()) / 15.0 // Normalize to 15-minute intervals
		scarcityFactor := 1.0
		if nodeCount > 0 {
			scarcityFactor = float64(rt.k) / float64(nodeCount)
		}

		priority := ageFactor * scarcityFactor

		priorities = append(priorities, bucketPriority{bucket: bucket, priority: priority})
	}
	rt.RUnlock()

	// Sort by priority (highest first)
	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i].priority > priorities[j].priority
	})

	// Refresh top priority buckets
	refreshCount := 0
	maxRefresh := 3 // Refresh at most 3 buckets per cycle

	for _, bp := range priorities {
		if refreshCount >= maxRefresh {
			break
		}

		if bp.bucket.nodes.Len() == 0 {
			continue
		}

		// Refresh this bucket
		i := 0
		for e := range bp.bucket.nodes.Iter() {
			if i < rt.dht.RefreshNodeNum {
				no := e.Value.(*node)
				rt.dht.transactionManager.findNode(no, bp.bucket.RandomChildID())
				if rt.dht.IsCrawlMode() {
					rt.clearQueue.PushBack(no)
				}
			}
			i++
		}
		refreshCount++
	}

	if rt.dht.IsCrawlMode() {
		for e := range rt.clearQueue.Iter() {
			rt.Remove(e.Value.(*node).id)
		}
		rt.clearQueue.Clear()
	}
}

// EvictStaleNodes removes nodes that haven't been seen for a long time
func (rt *routingTable) EvictStaleNodes(maxAge time.Duration) int {
	rt.Lock()
	defer rt.Unlock()

	evicted := 0
	nodesToEvict := make([]*node, 0)

	for item := range rt.cachedNodes.Iter() {
		n := item.val.(*node)
		if time.Since(n.lastActiveTime) > maxAge {
			nodesToEvict = append(nodesToEvict, n)
		}
	}

	for _, n := range nodesToEvict {
		if nd, _ := rt.getNodeKBucketByIDLocked(n.id); nd != nil {
			rt.removeLocked(nd.id)
			evicted++
		}
	}

	return evicted
}

// getNodeKBucketByIDLocked is like GetNodeKBucketByID but assumes the caller holds rt.Lock().
func (rt *routingTable) getNodeKBucketByIDLocked(id *bitmap) (nd *node, bucket *KBucket) {
	root := rt.root
	for bit := 0; root.KBucket() == nil; bit++ {
		root = root.Child(id.Bit(bit))
	}
	bucket = root.KBucket()
	if v, ok := bucket.nodes.Get(id.RawString()); ok {
		return v.Value.(*node), bucket
	}
	return nil, nil
}
