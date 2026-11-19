package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

// trendingLimitMax is the largest ranking a request may ask for. The cache
// keeps this many entries per window and serves every smaller limit from a
// prefix, which matches the query's deterministic (hits, infohash) order.
const trendingLimitMax = 100

// Ranking a window aggregates every observation in it, so rankings are
// computed in the background and served from memory. TTLs are the staleness
// accepted per window; variables so tests can shorten them.
var (
	trendingTTLs = map[time.Duration]time.Duration{
		time.Hour:      time.Minute,
		3 * time.Hour:  time.Minute,
		6 * time.Hour:  2 * time.Minute,
		12 * time.Hour: 5 * time.Minute,
		24 * time.Hour: 5 * time.Minute,
	}
	trendingRetry = 10 * time.Second // After a failed computation.
)

type trendingRanking struct {
	items      []mysql.Data
	until      time.Time
	ready      bool
	err        error
	expires    time.Time
	refreshing chan struct{}
}

// trendingCache serves rankings computed by compute. One refresh per window
// runs at a time on its own timeout. A cached ranking is served at once, even
// while it refreshes or after a failed refresh; requests wait only for the
// first ranking of a window.
type trendingCache struct {
	compute  func(context.Context, time.Duration, int, time.Time) ([]mysql.Data, error)
	timeout  time.Duration
	ttls     map[time.Duration]time.Duration
	retry    time.Duration
	mu       sync.Mutex
	rankings map[time.Duration]*trendingRanking
}

func newTrendingCache(compute func(context.Context, time.Duration, int, time.Time) ([]mysql.Data, error)) *trendingCache {
	return &trendingCache{compute: compute, timeout: mysql.TrendingTimeout, ttls: trendingTTLs, retry: trendingRetry,
		rankings: make(map[time.Duration]*trendingRanking)}
}

func (c *trendingCache) refresh(window time.Duration, ranking *trendingRanking, done chan struct{}) {
	defer close(done)
	until := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	items, err := c.compute(ctx, window, trendingLimitMax, until)
	cancel()
	if err != nil {
		slog.Error("trending computation failed", "component", "mysql", "event", "trending_failed", "window", window.String(), "error", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ranking.refreshing = nil
	ranking.err = err
	if err != nil {
		ranking.expires = time.Now().Add(c.retry)
		return
	}
	ranking.items, ranking.until, ranking.ready = items, until, true
	ranking.expires = time.Now().Add(c.ttls[window])
}

// lookup returns the top limit entries of window's ranking and when it was
// computed.
func (c *trendingCache) lookup(ctx context.Context, window time.Duration, limit int) ([]mysql.Data, time.Time, error) {
	c.mu.Lock()
	ranking := c.rankings[window]
	if ranking == nil {
		ranking = &trendingRanking{}
		c.rankings[window] = ranking
	}
	if ranking.refreshing == nil && !time.Now().Before(ranking.expires) {
		ranking.refreshing = make(chan struct{})
		go c.refresh(window, ranking, ranking.refreshing)
	}
	wait := ranking.refreshing
	if !ranking.ready {
		c.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, time.Time{}, ctx.Err()
		}
		c.mu.Lock()
	}
	defer c.mu.Unlock()
	if !ranking.ready {
		return nil, time.Time{}, ranking.err
	}
	items := ranking.items
	if len(items) > limit {
		items = items[:limit]
	}
	return items, ranking.until, nil
}

// warm computes every window once, so the first visitors need not wait.
func (c *trendingCache) warm() {
	for window := range c.ttls {
		_, _, _ = c.lookup(context.Background(), window, 1)
	}
}
