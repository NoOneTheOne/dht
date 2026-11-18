package main

import (
	"context"
	"encoding/json"
	"errors"
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
	statusCountTimeout = 10 * time.Second
)

// statusHandler serves a cached system status. One background refresh runs
// at a time on its own timeouts, so slow backends never hold a lock across
// requests and a disconnecting client cannot abort or poison the refresh.
// Requests are only made to wait when no status has been built yet.
func statusHandler(fetch func(context.Context) (*metrics.CrawlerStatus, error), count func(context.Context, time.Time) (mysql.ResourceCounts, error)) gin.HandlerFunc {
	var mu sync.Mutex
	var cached metrics.SystemStatus
	var hasCache bool
	var expires, countExpires, countedStart time.Time
	var totals *mysql.ResourceCounts
	var refreshing chan struct{}

	refresh := func(done chan struct{}) {
		defer close(done)
		fetchCtx, cancel := context.WithTimeout(context.Background(), statusFetchTimeout)
		crawler, err := fetch(fetchCtx)
		cancel()
		if err != nil {
			crawler = nil
		}

		mu.Lock()
		startedAt := countedStart
		if crawler != nil {
			startedAt = crawler.StartedAt
		}
		refreshCount := time.Now().After(countExpires) || !startedAt.Equal(countedStart)
		nextTotals := totals
		mu.Unlock()

		if refreshCount {
			countCtx, cancel := context.WithTimeout(context.Background(), statusCountTimeout)
			value, err := count(countCtx, startedAt)
			cancel()
			nextTotals = nil
			if err == nil {
				nextTotals = &value
			}
		}

		mu.Lock()
		defer mu.Unlock()
		if refreshCount {
			totals = nextTotals
			countedStart = startedAt
			countExpires = time.Now().Add(time.Minute)
			if totals == nil {
				countExpires = time.Now().Add(10 * time.Second)
			}
		}
		var total *int64
		if totals != nil {
			total = &totals.Total
		}
		next := metrics.NewSystemStatus(crawler, total)
		if totals != nil {
			next.Ingestion.AllTime = metrics.NewFileCoverage(totals.Total, totals.WithFiles)
			if crawler != nil {
				next.Ingestion.ThisRun = metrics.NewFileCoverage(totals.ThisRunTotal, totals.ThisRunWithFiles)
			}
		}
		cached, hasCache = next, true
		expires = time.Now().Add(statusTTL)
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
		if hasCache {
			// Serve the previous status while the refresh runs.
			status := cached
			mu.Unlock()
			respond(c, status)
			return
		}
		mu.Unlock()
		select {
		case <-wait:
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
