package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

func testTrendingCache(compute func(context.Context, time.Duration, int, time.Time) ([]mysql.Data, error), ttl time.Duration) *trendingCache {
	cache := newTrendingCache(compute)
	cache.ttls = map[time.Duration]time.Duration{time.Hour: ttl, 12 * time.Hour: ttl}
	cache.retry = ttl
	return cache
}

func rankedItems(n int, tag string) []mysql.Data {
	items := make([]mysql.Data, n)
	for i := range items {
		items[i] = mysql.Data{Name: tag, InfoHash: strings.Repeat("a", 39) + string(rune('0'+i%10)), Count: int64(n - i)}
	}
	return items
}

func TestTrendingCacheServesPrefixAndSharesComputation(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	cache := testTrendingCache(func(_ context.Context, window time.Duration, limit int, _ time.Time) ([]mysql.Data, error) {
		calls.Add(1)
		if limit != trendingLimitMax || window != 24*time.Hour {
			t.Errorf("computed window %v limit %d", window, limit)
		}
		<-release
		return rankedItems(30, "first"), nil
	}, time.Hour)
	var wg sync.WaitGroup
	for _, limit := range []int{1, 10, 100} {
		wg.Go(func() {
			items, until, err := cache.lookup(context.Background(), 24*time.Hour, limit)
			if err != nil || len(items) != min(limit, 30) || until.IsZero() || items[0].Count != 30 {
				t.Errorf("limit %d: %d items, until %v, err %v", limit, len(items), until, err)
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	if _, _, err := cache.lookup(context.Background(), 24*time.Hour, 5); err != nil || calls.Load() != 1 {
		t.Fatalf("%d computations, err %v; want one shared, cached computation", calls.Load(), err)
	}
}

func TestTrendingCacheKeepsRankingWhenRefreshFails(t *testing.T) {
	var fail atomic.Bool
	var calls atomic.Int32
	cache := testTrendingCache(func(context.Context, time.Duration, int, time.Time) ([]mysql.Data, error) {
		calls.Add(1)
		if fail.Load() {
			return nil, errors.New("timeout")
		}
		return rankedItems(3, "kept"), nil
	}, time.Millisecond)
	if _, _, err := cache.lookup(context.Background(), time.Hour, 10); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	time.Sleep(5 * time.Millisecond)
	for calls.Load() < 3 { // Expired: each lookup serves the old ranking and refreshes in the background.
		items, _, err := cache.lookup(context.Background(), time.Hour, 10)
		if err != nil || len(items) != 3 || items[0].Name != "kept" {
			t.Fatalf("failed refresh discarded ranking: %v, %v", items, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestTrendingCacheFirstComputationFailureAndCancellation(t *testing.T) {
	cache := testTrendingCache(func(context.Context, time.Duration, int, time.Time) ([]mysql.Data, error) {
		return nil, errors.New("unavailable")
	}, time.Hour)
	if _, _, err := cache.lookup(context.Background(), time.Hour, 10); err == nil {
		t.Fatal("failure without a cached ranking must be reported")
	}

	release := make(chan struct{})
	defer close(release)
	slow := testTrendingCache(func(context.Context, time.Duration, int, time.Time) ([]mysql.Data, error) {
		<-release
		return nil, nil
	}, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, err := slow.lookup(ctx, time.Hour, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting request ignored its context: %v", err)
	}
}
