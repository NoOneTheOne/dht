package dht

import (
	"sort"
	"time"
)

// BEP-5 Routing Table Optimizations

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

		selected := make([]*node, 0, bp.bucket.nodes.Len())
		for e := range bp.bucket.nodes.Iter() {
			selected = append(selected, e.Value.(*node))
		}
		if len(selected) == 0 {
			continue
		}
		// Rotating the snapshot avoids repeatedly probing the same oldest contacts.
		rt.Lock()
		offset := rt.refreshOffset % len(selected)
		rt.refreshOffset += min(rt.dht.RefreshNodeNum, len(selected))
		rt.Unlock()
		for i := 0; i < min(rt.dht.RefreshNodeNum, len(selected)); i++ {
			no := selected[(offset+i)%len(selected)]
			rt.dht.transactionManager.findNode(no, bp.bucket.RandomChildID())
			if rt.dht.IsCrawlMode() {
				rt.clearQueue.PushBack(no)
			}
		}
		refreshCount++
	}
	if rt.dht.IsCrawlMode() {
		for e := range rt.clearQueue.Iter() {
			rt.removeContact(e.Value.(*node), false)
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
