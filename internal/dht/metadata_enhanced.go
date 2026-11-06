package dht

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"log"
	"sync"
	"time"
)

// BEP-9 Metadata Exchange Enhancements

// MetadataCache caches fetched metadata to avoid redundant network requests
type MetadataCache struct {
	cache   map[string]*CachedMetadata
	mu      sync.RWMutex
	maxSize int
	maxAge  time.Duration
}

// CachedMetadata represents cached metadata with expiration
type CachedMetadata struct {
	InfoHash    string
	Metadata    []byte
	Name        string
	Size        int64
	CachedAt    time.Time
	AccessCount int
}

// NewMetadataCache creates a new metadata cache
func NewMetadataCache(maxSize int, maxAge time.Duration) *MetadataCache {
	mc := &MetadataCache{
		cache:   make(map[string]*CachedMetadata),
		maxSize: maxSize,
		maxAge:  maxAge,
	}

	// Start cleanup goroutine
	go mc.cleanupLoop()

	return mc
}

// Get retrieves metadata from cache
func (mc *MetadataCache) Get(infoHash string) (*CachedMetadata, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	cached, ok := mc.cache[infoHash]
	if !ok {
		return nil, false
	}

	// Check if expired
	if time.Since(cached.CachedAt) > mc.maxAge {
		return nil, false
	}

	// Update access count
	cached.AccessCount++

	return cached, true
}

// Set stores metadata in cache
func (mc *MetadataCache) Set(infoHash string, metadata []byte, name string, size int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Evict if at capacity
	if len(mc.cache) >= mc.maxSize {
		mc.evictOldest()
	}

	mc.cache[infoHash] = &CachedMetadata{
		InfoHash:    infoHash,
		Metadata:    metadata,
		Name:        name,
		Size:        size,
		CachedAt:    time.Now(),
		AccessCount: 1,
	}
}

// evictOldest removes the oldest entry from cache
// Must be called with lock held
func (mc *MetadataCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, cached := range mc.cache {
		if oldestKey == "" || cached.CachedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = cached.CachedAt
		}
	}

	if oldestKey != "" {
		delete(mc.cache, oldestKey)
	}
}

// cleanupLoop periodically removes expired entries
func (mc *MetadataCache) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		mc.cleanup()
	}
}

// cleanup removes expired entries
func (mc *MetadataCache) cleanup() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	now := time.Now()
	for key, cached := range mc.cache {
		if now.Sub(cached.CachedAt) > mc.maxAge {
			delete(mc.cache, key)
		}
	}
}

// Stats returns cache statistics
func (mc *MetadataCache) Stats() (size int, maxSize int) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return len(mc.cache), mc.maxSize
}

// Global metadata cache instance
var globalMetadataCache = NewMetadataCache(10000, 24*time.Hour)

// GetMetadataCache returns the global metadata cache
func GetMetadataCache() *MetadataCache {
	return globalMetadataCache
}

// MetadataFetcher provides enhanced metadata fetching with retry and parallel requests
type MetadataFetcher struct {
	wire          *Wire
	maxRetries    int
	timeout       time.Duration
	parallelPeers int
}

// NewMetadataFetcher creates a new metadata fetcher
func NewMetadataFetcher(wire *Wire, maxRetries int, timeout time.Duration) *MetadataFetcher {
	return &MetadataFetcher{
		wire:          wire,
		maxRetries:    maxRetries,
		timeout:       timeout,
		parallelPeers: 3,
	}
}

// FetchResult represents the result of a metadata fetch
type FetchResult struct {
	InfoHash string
	Metadata []byte
	Name     string
	Size     int64
	Source   string // "cache" or "network"
	Duration time.Duration
	Error    error
}

// Fetch attempts to fetch metadata with caching and retry
func (mf *MetadataFetcher) Fetch(infoHash []byte, peers []Request) *FetchResult {
	start := time.Now()
	infoHashHex := hex.EncodeToString(infoHash)

	// Check cache first
	if cached, ok := globalMetadataCache.Get(infoHashHex); ok {
		return &FetchResult{
			InfoHash: infoHashHex,
			Metadata: cached.Metadata,
			Name:     cached.Name,
			Size:     cached.Size,
			Source:   "cache",
			Duration: time.Since(start),
		}
	}

	// Try fetching from peers with retry
	var lastErr error
	for retry := 0; retry < mf.maxRetries; retry++ {
		for _, peer := range peers {
			result := mf.fetchFromPeer(infoHash, peer)
			if result != nil {
				// Parse metadata to extract name and size
				name, size := parseMetadataInfo(result)

				// Cache the result
				globalMetadataCache.Set(infoHashHex, result, name, size)

				return &FetchResult{
					InfoHash: infoHashHex,
					Metadata: result,
					Name:     name,
					Size:     size,
					Source:   "network",
					Duration: time.Since(start),
				}
			}
		}

		// Wait before retry
		if retry < mf.maxRetries-1 {
			time.Sleep(time.Second * time.Duration(retry+1))
		}
		lastErr = ErrMetadataFetchFailed
	}

	return &FetchResult{
		InfoHash: infoHashHex,
		Error:    lastErr,
		Duration: time.Since(start),
	}
}

// fetchFromPeer attempts to fetch metadata from a single peer
func (mf *MetadataFetcher) fetchFromPeer(infoHash []byte, peer Request) []byte {
	// Create a channel to receive the response
	responseChan := make(chan *Response, 1)

	// Submit request to wire
	mf.wire.Request(infoHash, peer.IP, peer.Port)

	// Wait for response with timeout
	select {
	case resp := <-responseChan:
		return resp.MetadataInfo
	case <-time.After(mf.timeout):
		return nil
	}
}

// ErrMetadataFetchFailed is returned when metadata fetch fails
var ErrMetadataFetchFailed = &MetadataError{Message: "metadata fetch failed after all retries"}

// MetadataError represents a metadata-related error
type MetadataError struct {
	Message string
}

func (e *MetadataError) Error() string {
	return e.Message
}

// parseMetadataInfo extracts name and size from metadata
func parseMetadataInfo(metadata []byte) (string, int64) {
	decoded, err := Decode(metadata)
	if err != nil {
		return "", 0
	}

	info, ok := decoded.(map[string]interface{})
	if !ok {
		return "", 0
	}

	name, _ := info["name"].(string)

	var size int64
	if length, ok := info["length"].(int); ok {
		size = int64(length)
	} else if files, ok := info["files"].([]interface{}); ok {
		// Sum up file sizes for multi-file torrents
		for _, f := range files {
			if file, ok := f.(map[string]interface{}); ok {
				if length, ok := file["length"].(int); ok {
					size += int64(length)
				}
			}
		}
	}

	return name, size
}

// VerifyMetadata verifies that metadata matches the expected infohash
func VerifyMetadata(infoHash []byte, metadata []byte) bool {
	hash := sha1.Sum(metadata)
	return bytes.Equal(infoHash, hash[:])
}

// MetadataStats holds metadata fetching statistics
type MetadataStats struct {
	CacheHits      int64
	CacheMisses    int64
	NetworkFetches int64
	FailedFetches  int64
	AvgFetchTime   time.Duration
}

var (
	metadataStats   MetadataStats
	metadataStatsMu sync.Mutex
)

// RecordCacheHit records a cache hit
func RecordCacheHit() {
	metadataStatsMu.Lock()
	metadataStats.CacheHits++
	metadataStatsMu.Unlock()
}

// RecordCacheMiss records a cache miss
func RecordCacheMiss() {
	metadataStatsMu.Lock()
	metadataStats.CacheMisses++
	metadataStatsMu.Unlock()
}

// RecordNetworkFetch records a network fetch
func RecordNetworkFetch(duration time.Duration) {
	metadataStatsMu.Lock()
	metadataStats.NetworkFetches++
	// Update average fetch time
	total := metadataStats.NetworkFetches
	if total == 1 {
		metadataStats.AvgFetchTime = duration
	} else {
		metadataStats.AvgFetchTime = (metadataStats.AvgFetchTime*time.Duration(total-1) + duration) / time.Duration(total)
	}
	metadataStatsMu.Unlock()
}

// RecordFailedFetch records a failed fetch
func RecordFailedFetch() {
	metadataStatsMu.Lock()
	metadataStats.FailedFetches++
	metadataStatsMu.Unlock()
}

// GetMetadataStats returns current metadata statistics
func GetMetadataStats() MetadataStats {
	metadataStatsMu.Lock()
	defer metadataStatsMu.Unlock()
	return metadataStats
}

// LogMetadataStats logs current metadata statistics
func LogMetadataStats() {
	stats := GetMetadataStats()
	cacheSize, cacheMax := globalMetadataCache.Stats()
	log.Printf("[Metadata] Cache: %d/%d, Hits: %d, Misses: %d, Fetches: %d, Failed: %d, AvgTime: %v",
		cacheSize, cacheMax, stats.CacheHits, stats.CacheMisses,
		stats.NetworkFetches, stats.FailedFetches, stats.AvgFetchTime)
}
