package dht

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Security enhancements for DHT network

// SecurityLevel represents the security level
type SecurityLevel int

const (
	SecurityLevelLow SecurityLevel = iota
	SecurityLevelMedium
	SecurityLevelHigh
)

// SecurityManager manages security features
type SecurityManager struct {
	level             SecurityLevel
	blacklist         *AdvancedBlacklist
	rateLimiter       *RateLimiter
	maliciousDetector *MaliciousNodeDetector
	auditLogger       *AuditLogger
}

// NewSecurityManager creates a new security manager
func NewSecurityManager(level SecurityLevel) *SecurityManager {
	return &SecurityManager{
		level:             level,
		blacklist:         NewAdvancedBlacklist(100000),
		rateLimiter:       NewRateLimiter(100, time.Second), // 100 requests per second
		maliciousDetector: NewMaliciousNodeDetector(),
		auditLogger:       NewAuditLogger(10000),
	}
}

// AdvancedBlacklist provides enhanced IP blacklisting
type AdvancedBlacklist struct {
	mu       sync.RWMutex
	maxSize  int
	ips      map[string]*BlacklistEntry
	subnets  map[string]*BlacklistEntry // CIDR notation
	patterns []BlacklistPattern
}

// BlacklistEntry represents a blacklisted IP or subnet
type BlacklistEntry struct {
	Value     string
	Reason    string
	AddedAt   time.Time
	ExpiresAt time.Time
	HitCount  int64
	Permanent bool
}

// BlacklistPattern represents a pattern-based blacklist rule
type BlacklistPattern struct {
	Pattern   string // IP prefix pattern like "192.168." or "10."
	Reason    string
	Permanent bool
}

// NewAdvancedBlacklist creates a new advanced blacklist
func NewAdvancedBlacklist(maxSize int) *AdvancedBlacklist {
	bl := &AdvancedBlacklist{
		maxSize:  maxSize,
		ips:      make(map[string]*BlacklistEntry),
		subnets:  make(map[string]*BlacklistEntry),
		patterns: make([]BlacklistPattern, 0),
	}

	// Add default patterns for known bad ranges
	bl.AddPattern("0.", "Zero network", true)
	bl.AddPattern("127.", "Loopback", true)
	bl.AddPattern("169.254.", "Link-local", true)
	bl.AddPattern("224.", "Multicast", true)
	bl.AddPattern("240.", "Reserved", true)
	bl.AddPattern("255.", "Broadcast", true)

	return bl
}

// AddIP adds an IP to the blacklist
func (bl *AdvancedBlacklist) AddIP(ip string, reason string, duration time.Duration) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	if len(bl.ips) >= bl.maxSize {
		bl.evictOldest()
	}

	entry := &BlacklistEntry{
		Value:     ip,
		Reason:    reason,
		AddedAt:   time.Now(),
		HitCount:  0,
		Permanent: duration == 0,
	}

	if duration > 0 {
		entry.ExpiresAt = time.Now().Add(duration)
	}

	bl.ips[ip] = entry
	log.Printf("[Security] Blacklisted IP %s: %s", ip, reason)
}

// AddSubnet adds a subnet to the blacklist
func (bl *AdvancedBlacklist) AddSubnet(cidr string, reason string, duration time.Duration) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	entry := &BlacklistEntry{
		Value:     cidr,
		Reason:    reason,
		AddedAt:   time.Now(),
		HitCount:  0,
		Permanent: duration == 0,
	}

	if duration > 0 {
		entry.ExpiresAt = time.Now().Add(duration)
	}

	bl.subnets[cidr] = entry
	log.Printf("[Security] Blacklisted subnet %s: %s", cidr, reason)
}

// AddPattern adds a pattern to the blacklist
func (bl *AdvancedBlacklist) AddPattern(pattern string, reason string, permanent bool) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	bl.patterns = append(bl.patterns, BlacklistPattern{
		Pattern:   pattern,
		Reason:    reason,
		Permanent: permanent,
	})
}

// IsBlacklisted checks if an IP is blacklisted
func (bl *AdvancedBlacklist) IsBlacklisted(ipStr string) (bool, string) {
	bl.mu.RLock()
	defer bl.mu.RUnlock()

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false, ""
	}

	// Check exact IP match
	if entry, ok := bl.ips[ipStr]; ok {
		if entry.Permanent || time.Now().Before(entry.ExpiresAt) {
			atomic.AddInt64(&entry.HitCount, 1)
			return true, entry.Reason
		}
	}

	// Check subnet matches
	for cidr, entry := range bl.subnets {
		if entry.Permanent || time.Now().Before(entry.ExpiresAt) {
			_, network, err := net.ParseCIDR(cidr)
			if err == nil && network.Contains(ip) {
				atomic.AddInt64(&entry.HitCount, 1)
				return true, entry.Reason
			}
		}
	}

	// Check pattern matches
	for _, pattern := range bl.patterns {
		if len(ipStr) >= len(pattern.Pattern) && ipStr[:len(pattern.Pattern)] == pattern.Pattern {
			return true, pattern.Reason
		}
	}

	return false, ""
}

// RemoveIP removes an IP from the blacklist
func (bl *AdvancedBlacklist) RemoveIP(ip string) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	delete(bl.ips, ip)
}

// Cleanup removes expired entries
func (bl *AdvancedBlacklist) Cleanup() int {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	now := time.Now()
	removed := 0

	for ip, entry := range bl.ips {
		if !entry.Permanent && now.After(entry.ExpiresAt) {
			delete(bl.ips, ip)
			removed++
		}
	}

	for cidr, entry := range bl.subnets {
		if !entry.Permanent && now.After(entry.ExpiresAt) {
			delete(bl.subnets, cidr)
			removed++
		}
	}

	return removed
}

// evictOldest removes the oldest non-permanent entry
// Must be called with lock held
func (bl *AdvancedBlacklist) evictOldest() {
	var oldestIP string
	var oldestTime time.Time

	for ip, entry := range bl.ips {
		if entry.Permanent {
			continue
		}
		if oldestIP == "" || entry.AddedAt.Before(oldestTime) {
			oldestIP = ip
			oldestTime = entry.AddedAt
		}
	}

	if oldestIP != "" {
		delete(bl.ips, oldestIP)
	}
}

// Stats returns blacklist statistics
func (bl *AdvancedBlacklist) Stats() (ips int, subnets int, patterns int) {
	bl.mu.RLock()
	defer bl.mu.RUnlock()
	return len(bl.ips), len(bl.subnets), len(bl.patterns)
}

// RateLimiter implements token bucket rate limiting
type RateLimiter struct {
	mu       sync.Mutex
	rate     int           // tokens per interval
	interval time.Duration // refill interval
	tokens   map[string]*TokenBucket
	stopChan chan struct{}
}

// TokenBucket represents a token bucket for an IP
type TokenBucket struct {
	Tokens     float64
	LastRefill time.Time
	MaxTokens  float64
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(rate int, interval time.Duration) *RateLimiter {
	rl := &RateLimiter{
		rate:     rate,
		interval: interval,
		tokens:   make(map[string]*TokenBucket),
		stopChan: make(chan struct{}),
	}

	// Start cleanup routine
	go rl.cleanup()

	return rl
}

// Allow checks if a request from IP should be allowed
func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, exists := rl.tokens[ip]
	if !exists {
		bucket = &TokenBucket{
			Tokens:     float64(rl.rate),
			LastRefill: time.Now(),
			MaxTokens:  float64(rl.rate * 2),
		}
		rl.tokens[ip] = bucket
	}

	// Refill tokens
	now := time.Now()
	elapsed := now.Sub(bucket.LastRefill)
	refill := float64(rl.rate) * elapsed.Seconds() / rl.interval.Seconds()
	bucket.Tokens = min(bucket.Tokens+refill, bucket.MaxTokens)
	bucket.LastRefill = now

	// Check if allowed
	if bucket.Tokens >= 1 {
		bucket.Tokens--
		return true
	}

	return false
}

// GetRemainingTokens returns remaining tokens for an IP
func (rl *RateLimiter) GetRemainingTokens(ip string) float64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if bucket, exists := rl.tokens[ip]; exists {
		return bucket.Tokens
	}
	return float64(rl.rate)
}

// cleanup removes stale entries
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-rl.stopChan:
			return
		case <-ticker.C:
			rl.mu.Lock()
			now := time.Now()
			for ip, bucket := range rl.tokens {
				// Remove entries not seen in last hour
				if now.Sub(bucket.LastRefill) > time.Hour {
					delete(rl.tokens, ip)
				}
			}
			rl.mu.Unlock()
		}
	}
}

// Stop stops the rate limiter
func (rl *RateLimiter) Stop() {
	close(rl.stopChan)
}

// MaliciousNodeDetector detects malicious behavior
type MaliciousNodeDetector struct {
	mu              sync.RWMutex
	suspiciousNodes map[string]*SuspiciousNode
	thresholds      DetectionThresholds
}

// SuspiciousNode tracks suspicious behavior
type SuspiciousNode struct {
	NodeID          string
	IPAddress       string
	FirstSeen       time.Time
	LastSeen        time.Time
	FailedRequests  int
	InvalidMessages int
	SpamCount       int
	Score           float64 // 0.0 to 1.0, higher is more suspicious
	Reasons         []string
}

// DetectionThresholds defines thresholds for malicious detection
type DetectionThresholds struct {
	MaxFailedRequests  int
	MaxInvalidMessages int
	MaxSpamCount       int
	SuspiciousScore    float64
	MaliciousScore     float64
}

// NewMaliciousNodeDetector creates a new malicious node detector
func NewMaliciousNodeDetector() *MaliciousNodeDetector {
	return &MaliciousNodeDetector{
		suspiciousNodes: make(map[string]*SuspiciousNode),
		thresholds: DetectionThresholds{
			MaxFailedRequests:  10,
			MaxInvalidMessages: 5,
			MaxSpamCount:       20,
			SuspiciousScore:    0.5,
			MaliciousScore:     0.8,
		},
	}
}

// RecordFailedRequest records a failed request from a node
func (mnd *MaliciousNodeDetector) RecordFailedRequest(nodeID string, ip string) {
	mnd.mu.Lock()
	defer mnd.mu.Unlock()

	node := mnd.getOrCreateNode(nodeID, ip)
	node.FailedRequests++
	node.LastSeen = time.Now()
	mnd.updateScore(node)
}

// RecordInvalidMessage records an invalid message from a node
func (mnd *MaliciousNodeDetector) RecordInvalidMessage(nodeID string, ip string, reason string) {
	mnd.mu.Lock()
	defer mnd.mu.Unlock()

	node := mnd.getOrCreateNode(nodeID, ip)
	node.InvalidMessages++
	node.LastSeen = time.Now()
	node.Reasons = append(node.Reasons, "invalid: "+reason)
	mnd.updateScore(node)
}

// RecordSpam records spam behavior from a node
func (mnd *MaliciousNodeDetector) RecordSpam(nodeID string, ip string) {
	mnd.mu.Lock()
	defer mnd.mu.Unlock()

	node := mnd.getOrCreateNode(nodeID, ip)
	node.SpamCount++
	node.LastSeen = time.Now()
	mnd.updateScore(node)
}

// getOrCreateNode gets or creates a suspicious node entry
// Must be called with lock held
func (mnd *MaliciousNodeDetector) getOrCreateNode(nodeID string, ip string) *SuspiciousNode {
	key := nodeID
	if key == "" {
		key = ip
	}

	node, exists := mnd.suspiciousNodes[key]
	if !exists {
		node = &SuspiciousNode{
			NodeID:    nodeID,
			IPAddress: ip,
			FirstSeen: time.Now(),
			LastSeen:  time.Now(),
			Reasons:   make([]string, 0),
		}
		mnd.suspiciousNodes[key] = node
	}

	return node
}

// updateScore updates the suspicious score for a node
// Must be called with lock held
func (mnd *MaliciousNodeDetector) updateScore(node *SuspiciousNode) {
	// Calculate score based on various factors
	score := 0.0

	// Failed requests contribute to score
	if node.FailedRequests > 0 {
		score += float64(node.FailedRequests) / float64(mnd.thresholds.MaxFailedRequests) * 0.3
	}

	// Invalid messages contribute more
	if node.InvalidMessages > 0 {
		score += float64(node.InvalidMessages) / float64(mnd.thresholds.MaxInvalidMessages) * 0.4
	}

	// Spam contributes significantly
	if node.SpamCount > 0 {
		score += float64(node.SpamCount) / float64(mnd.thresholds.MaxSpamCount) * 0.3
	}

	// Cap at 1.0
	node.Score = min(score, 1.0)
}

// IsSuspicious checks if a node is suspicious
func (mnd *MaliciousNodeDetector) IsSuspicious(nodeID string) (bool, float64) {
	mnd.mu.RLock()
	defer mnd.mu.RUnlock()

	if node, exists := mnd.suspiciousNodes[nodeID]; exists {
		return node.Score >= mnd.thresholds.SuspiciousScore, node.Score
	}
	return false, 0
}

// IsMalicious checks if a node is malicious
func (mnd *MaliciousNodeDetector) IsMalicious(nodeID string) (bool, float64) {
	mnd.mu.RLock()
	defer mnd.mu.RUnlock()

	if node, exists := mnd.suspiciousNodes[nodeID]; exists {
		return node.Score >= mnd.thresholds.MaliciousScore, node.Score
	}
	return false, 0
}

// GetSuspiciousNodes returns all suspicious nodes
func (mnd *MaliciousNodeDetector) GetSuspiciousNodes() []*SuspiciousNode {
	mnd.mu.RLock()
	defer mnd.mu.RUnlock()

	result := make([]*SuspiciousNode, 0)
	for _, node := range mnd.suspiciousNodes {
		if node.Score >= mnd.thresholds.SuspiciousScore {
			result = append(result, node)
		}
	}
	return result
}

// Cleanup removes old entries
func (mnd *MaliciousNodeDetector) Cleanup(maxAge time.Duration) int {
	mnd.mu.Lock()
	defer mnd.mu.Unlock()

	now := time.Now()
	removed := 0

	for key, node := range mnd.suspiciousNodes {
		if now.Sub(node.LastSeen) > maxAge && node.Score < mnd.thresholds.SuspiciousScore {
			delete(mnd.suspiciousNodes, key)
			removed++
		}
	}

	return removed
}

// AuditLogger provides security audit logging
type AuditLogger struct {
	mu      sync.Mutex
	maxSize int
	entries []*AuditEntry
}

// AuditEntry represents an audit log entry
type AuditEntry struct {
	Timestamp time.Time
	EventType string
	SourceIP  string
	TargetIP  string
	NodeID    string
	Details   string
	Severity  string // INFO, WARNING, ERROR, CRITICAL
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(maxSize int) *AuditLogger {
	return &AuditLogger{
		maxSize: maxSize,
		entries: make([]*AuditEntry, 0),
	}
}

// Log adds an audit log entry
func (al *AuditLogger) Log(eventType string, sourceIP string, targetIP string, nodeID string, details string, severity string) {
	al.mu.Lock()
	defer al.mu.Unlock()

	entry := &AuditEntry{
		Timestamp: time.Now(),
		EventType: eventType,
		SourceIP:  sourceIP,
		TargetIP:  targetIP,
		NodeID:    nodeID,
		Details:   details,
		Severity:  severity,
	}

	al.entries = append(al.entries, entry)

	// Trim if over capacity
	if len(al.entries) > al.maxSize {
		al.entries = al.entries[len(al.entries)-al.maxSize:]
	}

	// Log critical events immediately
	if severity == "CRITICAL" {
		log.Printf("[AUDIT] CRITICAL: %s - %s (from %s to %s)", eventType, details, sourceIP, targetIP)
	}
}

// GetEntries returns recent audit entries
func (al *AuditLogger) GetEntries(limit int) []*AuditEntry {
	al.mu.Lock()
	defer al.mu.Unlock()

	if limit > len(al.entries) {
		limit = len(al.entries)
	}

	result := make([]*AuditEntry, limit)
	copy(result, al.entries[len(al.entries)-limit:])
	return result
}

// GetEntriesBySeverity returns entries filtered by severity
func (al *AuditLogger) GetEntriesBySeverity(severity string) []*AuditEntry {
	al.mu.Lock()
	defer al.mu.Unlock()

	result := make([]*AuditEntry, 0)
	for _, entry := range al.entries {
		if entry.Severity == severity {
			result = append(result, entry)
		}
	}
	return result
}

// Global security manager instance
var globalSecurityManager *SecurityManager
var securityOnce sync.Once

// GetSecurityManager returns the global security manager
func GetSecurityManager() *SecurityManager {
	securityOnce.Do(func() {
		globalSecurityManager = NewSecurityManager(SecurityLevelMedium)
	})
	return globalSecurityManager
}

// ValidateRequest performs security validation on a request
func ValidateRequest(sourceIP string, nodeID string, messageType string) error {
	sm := GetSecurityManager()

	// Check blacklist
	if blocked, reason := sm.blacklist.IsBlacklisted(sourceIP); blocked {
		sm.auditLogger.Log("BLOCKED_REQUEST", sourceIP, "", nodeID,
			"Blocked: "+reason+" (message: "+messageType+")", "WARNING")
		return &SecurityError{Message: "IP blacklisted: " + reason}
	}

	// Check rate limit
	if !sm.rateLimiter.Allow(sourceIP) {
		sm.auditLogger.Log("RATE_LIMITED", sourceIP, "", nodeID,
			"Rate limited (message: "+messageType+")", "WARNING")
		return &SecurityError{Message: "rate limit exceeded"}
	}

	// Check malicious node
	if isMalicious, score := sm.maliciousDetector.IsMalicious(nodeID); isMalicious {
		sm.auditLogger.Log("MALICIOUS_NODE", sourceIP, "", nodeID,
			"Malicious node detected (score: "+string(rune(score))+")", "CRITICAL")
		return &SecurityError{Message: "malicious node detected"}
	}

	return nil
}

// SecurityError represents a security-related error
type SecurityError struct {
	Message string
}

func (e *SecurityError) Error() string {
	return e.Message
}
