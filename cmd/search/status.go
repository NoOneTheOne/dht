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

func statusHandler(fetch func(context.Context) (*metrics.CrawlerStatus, error), count func(context.Context, time.Time) (mysql.ResourceCounts, error)) gin.HandlerFunc {
	var mu sync.Mutex
	var cached metrics.SystemStatus
	var expires, countExpires, countedStart time.Time
	var totals *mysql.ResourceCounts
	return func(c *gin.Context) {
		mu.Lock()
		defer mu.Unlock()
		if c.Request.Context().Err() != nil {
			return
		}
		if time.Now().After(expires) {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
			defer cancel()
			crawler, err := fetch(ctx)
			if err != nil {
				crawler = nil
			}
			startedAt := countedStart
			if crawler != nil {
				startedAt = crawler.StartedAt
			}
			refreshCount := time.Now().After(countExpires) || !startedAt.Equal(countedStart)
			nextTotals := totals
			if refreshCount {
				nextTotals = nil
				type countResult struct {
					value mysql.ResourceCounts
					err   error
				}
				counts := make(chan countResult, 1)
				go func() {
					value, err := count(ctx, startedAt)
					counts <- countResult{value, err}
				}()
				select {
				case result := <-counts:
					if result.err == nil {
						nextTotals = &result.value
					}
				case <-ctx.Done():
				}
			}
			if c.Request.Context().Err() != nil {
				return
			}
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
			cached = metrics.NewSystemStatus(crawler, total)
			if totals != nil {
				cached.Ingestion.AllTime = metrics.NewFileCoverage(totals.Total, totals.WithFiles)
				if crawler != nil {
					cached.Ingestion.ThisRun = metrics.NewFileCoverage(totals.ThisRunTotal, totals.ThisRunWithFiles)
				}
			}
			expires = time.Now().Add(10 * time.Second)
		}
		c.Header("Cache-Control", "no-store")
		c.JSON(http.StatusOK, cached)
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
