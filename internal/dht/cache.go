package dht

import (
	"container/list"
	"sync"
	"time"
)

// Generic LRU Cache with TTL support

// CacheItem represents a cached item with metadata
type CacheItem struct {
	Key         string
	Value       interface{}
	Expiration  time.Time
	AccessCount int64
	LastAccess  time.Time
	CreatedAt   time.Time
}

// LRUCache implements a thread-safe LRU cache with TTL
type LRUCache struct {
	mu         sync.RWMutex
	maxSize    int
	items      map[string]*list.Element
	evictList  *list.List
	defaultTTL time.Duration

	// Statistics
	stats CacheStats
}

// CacheStats holds cache statistics
type CacheStats struct {
	Hits         int64
	Misses       int64
	Evictions    int64
	Inserts      int64
	Updates      int64
	ExpiredItems int64
}

// NewLRUCache creates a new LRU cache
func NewLRUCache(maxSize int, defaultTTL time.Duration) *LRUCache {
	return &LRUCache{
		maxSize:    maxSize,
		items:      make(map[string]*list.Element),
		evictList:  list.New(),
		defaultTTL: defaultTTL,
	}
}

// Get retrieves an item from the cache
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		item := elem.Value.(*CacheItem)

		// Check expiration
		if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
			c.removeElement(elem)
			c.stats.ExpiredItems++
			c.stats.Misses++
			return nil, false
		}

		// Move to front (most recently used)
		c.evictList.MoveToFront(elem)

		// Update access stats
		item.AccessCount++
		item.LastAccess = time.Now()

		c.stats.Hits++
		return item.Value, true
	}

	c.stats.Misses++
	return nil, false
}

// Set adds or updates an item in the cache
func (c *LRUCache) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

// SetWithTTL adds or updates an item with a specific TTL
func (c *LRUCache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration time.Time
	if ttl > 0 {
		expiration = time.Now().Add(ttl)
	}

	if elem, ok := c.items[key]; ok {
		// Update existing item
		c.evictList.MoveToFront(elem)
		item := elem.Value.(*CacheItem)
		item.Value = value
		item.Expiration = expiration
		item.LastAccess = time.Now()
		c.stats.Updates++
		return
	}

	// Create new item
	item := &CacheItem{
		Key:         key,
		Value:       value,
		Expiration:  expiration,
		AccessCount: 1,
		LastAccess:  time.Now(),
		CreatedAt:   time.Now(),
	}

	elem := c.evictList.PushFront(item)
	c.items[key] = elem
	c.stats.Inserts++

	// Evict if over capacity
	if c.evictList.Len() > c.maxSize {
		c.removeOldest()
	}
}

// Remove removes an item from the cache
func (c *LRUCache) Remove(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

// Contains checks if a key exists in the cache
func (c *LRUCache) Contains(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elem, ok := c.items[key]
	if !ok {
		return false
	}

	item := elem.Value.(*CacheItem)
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		return false
	}

	return true
}

// Len returns the number of items in the cache
func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}

// Clear removes all items from the cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.evictList.Init()
}

// GetStats returns cache statistics
func (c *LRUCache) GetStats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// ResetStats resets cache statistics
func (c *LRUCache) ResetStats() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = CacheStats{}
}

// removeOldest removes the oldest item from the cache
// Must be called with lock held
func (c *LRUCache) removeOldest() {
	if elem := c.evictList.Back(); elem != nil {
		c.removeElement(elem)
		c.stats.Evictions++
	}
}

// removeElement removes a specific element from the cache
// Must be called with lock held
func (c *LRUCache) removeElement(elem *list.Element) {
	c.evictList.Remove(elem)
	item := elem.Value.(*CacheItem)
	delete(c.items, item.Key)
}

// Cleanup removes expired items
func (c *LRUCache) Cleanup() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	removed := 0

	for key, elem := range c.items {
		item := elem.Value.(*CacheItem)
		if !item.Expiration.IsZero() && now.After(item.Expiration) {
			c.removeElement(elem)
			c.stats.ExpiredItems++
			removed++
			_ = key // avoid unused variable warning
		}
	}

	return removed
}

// StartCleanupRoutine starts a background routine to clean up expired items
func (c *LRUCache) StartCleanupRoutine(interval time.Duration, stopChan <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			c.Cleanup()
		}
	}
}

// DHT-specific caches

// NodeCache caches node information
type NodeCache struct {
	cache *LRUCache
}

// CachedNodeInfo represents cached node information
type CachedNodeInfo struct {
	Node        *node
	LastPing    time.Time
	PingCount   int
	LastPong    time.Time
	PongCount   int
	Reliability float64 // 0.0 to 1.0
}

// NewNodeCache creates a new node cache
func NewNodeCache(maxSize int) *NodeCache {
	return &NodeCache{
		cache: NewLRUCache(maxSize, 30*time.Minute),
	}
}

// Get retrieves node info from cache
func (nc *NodeCache) Get(addr string) (*CachedNodeInfo, bool) {
	if val, ok := nc.cache.Get(addr); ok {
		return val.(*CachedNodeInfo), true
	}
	return nil, false
}

// Set stores node info in cache
func (nc *NodeCache) Set(addr string, info *CachedNodeInfo) {
	nc.cache.Set(addr, info)
}

// UpdatePing updates ping statistics for a node
func (nc *NodeCache) UpdatePing(addr string) {
	if info, ok := nc.Get(addr); ok {
		info.LastPing = time.Now()
		info.PingCount++
		nc.Set(addr, info)
	}
}

// UpdatePong updates pong statistics for a node
func (nc *NodeCache) UpdatePong(addr string) {
	if info, ok := nc.Get(addr); ok {
		info.LastPong = time.Now()
		info.PongCount++

		// Calculate reliability
		if info.PingCount > 0 {
			info.Reliability = float64(info.PongCount) / float64(info.PingCount)
		}

		nc.Set(addr, info)
	}
}

// QueryResultCache caches query results
type QueryResultCache struct {
	cache *LRUCache
}

// CachedQueryResult represents a cached query result
type CachedQueryResult struct {
	QueryType   QueryType
	Target      string
	Results     interface{} // []*node or []*Peer
	CachedAt    time.Time
	ResultCount int
}

// NewQueryResultCache creates a new query result cache
func NewQueryResultCache(maxSize int) *QueryResultCache {
	return &QueryResultCache{
		cache: NewLRUCache(maxSize, 5*time.Minute), // Short TTL for query results
	}
}

// Get retrieves query result from cache
func (qrc *QueryResultCache) Get(key string) (*CachedQueryResult, bool) {
	if val, ok := qrc.cache.Get(key); ok {
		return val.(*CachedQueryResult), true
	}
	return nil, false
}

// Set stores query result in cache
func (qrc *QueryResultCache) Set(key string, result *CachedQueryResult) {
	result.CachedAt = time.Now()
	qrc.cache.Set(key, result)
}

// GenerateQueryKey generates a cache key for a query
func GenerateQueryKey(queryType QueryType, target string) string {
	return string(rune(queryType)) + ":" + target
}

// InfoHashCache caches infohash to peer mappings
type InfoHashCache struct {
	cache *LRUCache
}

// CachedPeers represents cached peers for an infohash
type CachedPeers struct {
	InfoHash string
	Peers    []*Peer
	CachedAt time.Time
	Source   string // Where we learned about these peers
}

// NewInfoHashCache creates a new infohash cache
func NewInfoHashCache(maxSize int) *InfoHashCache {
	return &InfoHashCache{
		cache: NewLRUCache(maxSize, 10*time.Minute),
	}
}

// Get retrieves peers from cache
func (ihc *InfoHashCache) Get(infoHash string) (*CachedPeers, bool) {
	if val, ok := ihc.cache.Get(infoHash); ok {
		return val.(*CachedPeers), true
	}
	return nil, false
}

// Set stores peers in cache
func (ihc *InfoHashCache) Set(infoHash string, peers *CachedPeers) {
	peers.CachedAt = time.Now()
	ihc.cache.Set(infoHash, peers)
}

// Global cache instances
var (
	globalNodeCache      = NewNodeCache(10000)
	globalQueryCache     = NewQueryResultCache(5000)
	globalInfoHashCache  = NewInfoHashCache(10000)
	cacheCleanupStopChan = make(chan struct{})
)

// GetGlobalNodeCache returns the global node cache
func GetGlobalNodeCache() *NodeCache {
	return globalNodeCache
}

// GetGlobalQueryCache returns the global query cache
func GetGlobalQueryCache() *QueryResultCache {
	return globalQueryCache
}

// GetGlobalInfoHashCache returns the global infohash cache
func GetGlobalInfoHashCache() *InfoHashCache {
	return globalInfoHashCache
}

// StartCacheCleanup starts background cleanup for all caches
func StartCacheCleanup() {
	go globalNodeCache.cache.StartCleanupRoutine(time.Minute, cacheCleanupStopChan)
	go globalQueryCache.cache.StartCleanupRoutine(30*time.Second, cacheCleanupStopChan)
	go globalInfoHashCache.cache.StartCleanupRoutine(time.Minute, cacheCleanupStopChan)
}

// StopCacheCleanup stops background cleanup
func StopCacheCleanup() {
	close(cacheCleanupStopChan)
}

// CacheStatsSummary provides a summary of all cache statistics
type CacheStatsSummary struct {
	NodeCache     CacheStats
	QueryCache    CacheStats
	InfoHashCache CacheStats
	TotalSize     int
	TotalCapacity int
}

// GetCacheStatsSummary returns statistics for all caches
func GetCacheStatsSummary() CacheStatsSummary {
	return CacheStatsSummary{
		NodeCache:     globalNodeCache.cache.GetStats(),
		QueryCache:    globalQueryCache.cache.GetStats(),
		InfoHashCache: globalInfoHashCache.cache.GetStats(),
		TotalSize:     globalNodeCache.cache.Len() + globalQueryCache.cache.Len() + globalInfoHashCache.cache.Len(),
		TotalCapacity: 25000, // Sum of all cache capacities
	}
}
