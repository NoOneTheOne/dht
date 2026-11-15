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

func totalResources(ctx context.Context) (int64, error) {
	db, err := mysql.GetDB()
	if err != nil {
		return 0, err
	}
	var total int64
	err = db.WithContext(ctx).Model(&mysql.Data{}).Count(&total).Error
	return total, err
}

func statusHandler(fetch func(context.Context) (*metrics.CrawlerStatus, error), count func(context.Context) (int64, error)) gin.HandlerFunc {
	var mu sync.Mutex
	var cached metrics.SystemStatus
	var expires time.Time
	var total *int64
	var countExpires time.Time
	return func(c *gin.Context) {
		mu.Lock()
		defer mu.Unlock()
		if c.Request.Context().Err() != nil {
			return
		}
		if time.Now().After(expires) {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
			defer cancel()
			type countResult struct {
				value int64
				err   error
			}
			refreshCount := time.Now().After(countExpires)
			counts := make(chan countResult, 1)
			if refreshCount {
				go func() {
					value, err := count(ctx)
					counts <- countResult{value, err}
				}()
			}
			crawler, err := fetch(ctx)
			if err != nil {
				crawler = nil
			}
			nextTotal := total
			if refreshCount {
				nextTotal = nil
				select {
				case result := <-counts:
					if result.err == nil {
						nextTotal = &result.value
					}
				case <-ctx.Done():
				}
			}
			if c.Request.Context().Err() != nil {
				return
			}
			if refreshCount {
				total = nextTotal
				countExpires = time.Now().Add(time.Minute)
				if total == nil {
					countExpires = time.Now().Add(10 * time.Second)
				}
			}
			cached = metrics.NewSystemStatus(crawler, total)
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
