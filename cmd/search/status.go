package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NoOneTheOne/dht/internal/metrics"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
	"github.com/gin-gonic/gin"
)

func fetchCrawlerStatus(ctx context.Context, client *http.Client, url string) (*metrics.CrawlerStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("crawler unavailable")
	}
	var status metrics.CrawlerStatus
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 64<<10)).Decode(&status); err != nil {
		return nil, err
	}
	if status.StartedAt.IsZero() {
		return nil, errors.New("crawler status invalid")
	}
	return &status, nil
}

// Status refresh budgets. Variables so tests can shorten them.
var (
	statusTTL          = 10 * time.Second
	statusFetchTimeout = 4 * time.Second
	statusPingTimeout  = 3 * time.Second
	// Counting scans the whole resource table: it runs on its own schedule,
	// never delays the crawler status, and is cached for long.
	statusCountTimeout = mysql.CountTimeout
	statusCountTTL     = 10 * time.Minute
	statusCountRetry   = time.Minute
	// statusFirstCountWait is how long the very first status waits for the
	// first count, so a fast database shows totals on the first visit.
	statusFirstCountWait = 3 * time.Second
	// statusStaleGrace is how long past its TTL a cached status may still be
	// served instantly while it refreshes. Older caches (the first visit after
	// an idle period) wait up to statusStaleWait for the fresh status instead,
	// so the page never opens on data that may be hours old.
	statusStaleGrace = 10 * time.Second
	statusStaleWait  = 5 * time.Second
)

// statusHandler serves a cached system status. One background refresh runs
// at a time on its own timeouts, so slow backends never hold a lock across
// requests and a disconnecting client cannot abort or poison the refresh.
// Requests wait (bounded) only when the cache is missing or long expired.
// Resource counts refresh separately (see statusCountTTL); ping decides
// whether the database is reachable, so a slow count is not reported as an
// outage.
func statusHandler(fetch func(context.Context) (*metrics.CrawlerStatus, error), count func(context.Context, time.Time) (mysql.ResourceCounts, error), ping func(context.Context) error) gin.HandlerFunc {
	var mu sync.Mutex
	var cached metrics.SystemStatus
	var hasCache bool
	var expires time.Time
	var refreshing chan struct{}
	var crawler *metrics.CrawlerStatus
	var storageUp bool
	// Counts and the crawler start time they were computed for.
	var totals *mysql.ResourceCounts
	var countedStart, countExpires time.Time
	var counting chan struct{}
	// Read the budgets once: refreshes outlive requests, so tests may restore
	// the package variables while one is still running.
	ttl, fetchTimeout, pingTimeout := statusTTL, statusFetchTimeout, statusPingTimeout
	countTimeout, countTTL, countRetry, firstCountWait := statusCountTimeout, statusCountTTL, statusCountRetry, statusFirstCountWait
	staleGrace, staleWait := statusStaleGrace, statusStaleWait

	// build assembles the status from the latest parts; mu must be held.
	build := func() metrics.SystemStatus {
		var total *int64
		if totals != nil {
			total = &totals.Total
		}
		next := metrics.NewSystemStatus(crawler, total, storageUp, totals == nil && counting != nil)
		if totals != nil {
			next.Ingestion.AllTime = metrics.NewFileCoverage(totals.Total, totals.WithFiles)
			if crawler != nil && crawler.StartedAt.Equal(countedStart) {
				next.Ingestion.ThisRun = metrics.NewFileCoverage(totals.ThisRunTotal, totals.ThisRunWithFiles)
			}
		}
		return next
	}

	recount := func(startedAt time.Time, done chan struct{}) {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), countTimeout)
		value, err := count(ctx, startedAt)
		cancel()
		mu.Lock()
		defer mu.Unlock()
		counting = nil
		if err != nil {
			slog.Warn("resource count failed", "component", "mysql", "event", "status_count_failed", "error", err)
			countExpires = time.Now().Add(countRetry)
			if !startedAt.Equal(countedStart) {
				totals = nil // Per-run counts would describe another run.
			}
		} else {
			totals, countedStart = &value, startedAt
			countExpires = time.Now().Add(countTTL)
		}
		if hasCache {
			cached = build()
		}
	}

	refresh := func(done chan struct{}) {
		defer close(done)
		fetchCtx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
		nextCrawler, err := fetch(fetchCtx)
		cancel()
		if err != nil {
			nextCrawler = nil
		}
		pingCtx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		up := ping(pingCtx) == nil
		cancel()

		mu.Lock()
		crawler, storageUp = nextCrawler, up
		startedAt := countedStart
		if crawler != nil {
			startedAt = crawler.StartedAt
		}
		if counting == nil && up && (time.Now().After(countExpires) || !startedAt.Equal(countedStart)) {
			counting = make(chan struct{})
			go recount(startedAt, counting)
		}
		first, wait := !hasCache && totals == nil, counting
		mu.Unlock()
		if first && wait != nil {
			select {
			case <-wait:
			case <-time.After(firstCountWait):
			}
		}

		mu.Lock()
		defer mu.Unlock()
		cached, hasCache = build(), true
		expires = time.Now().Add(ttl)
		refreshing = nil
	}

	respond := func(c *gin.Context, status metrics.SystemStatus) {
		c.Header("Cache-Control", "no-store")
		c.JSON(http.StatusOK, status)
	}

	return func(c *gin.Context) {
		mu.Lock()
		if hasCache && time.Now().Before(expires) {
			status := cached
			mu.Unlock()
			respond(c, status)
			return
		}
		if refreshing == nil {
			refreshing = make(chan struct{})
			go refresh(refreshing)
		}
		wait := refreshing
		if hasCache && time.Now().Before(expires.Add(staleGrace)) {
			// Recently expired: serve it while the refresh runs.
			status := cached
			mu.Unlock()
			respond(c, status)
			return
		}
		stale := hasCache
		mu.Unlock()
		var deadline <-chan time.Time
		if stale {
			timer := time.NewTimer(staleWait)
			defer timer.Stop()
			deadline = timer.C
		}
		select {
		case <-wait:
		case <-deadline: // Refresh is slow: fall back to the stale status.
		case <-c.Request.Context().Done():
			return
		}
		mu.Lock()
		status := cached
		mu.Unlock()
		respond(c, status)
	}
}

func crawlerStatusGetter() func(context.Context) (*metrics.CrawlerStatus, error) {
	url := os.Getenv("SPIDER_STATUS_URL")
	if url == "" {
		url = "http://127.0.0.1:30081/status"
	}
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context) (*metrics.CrawlerStatus, error) { return fetchCrawlerStatus(ctx, client, url) }
}
