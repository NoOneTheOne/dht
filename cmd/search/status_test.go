package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/metrics"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
	"github.com/gin-gonic/gin"
)

func TestStatusHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, unavailable := range []bool{false, true} {
		calls := 0
		handler := statusHandler(func(context.Context) (*metrics.CrawlerStatus, error) {
			if unavailable {
				return nil, errors.New("private backend details")
			}
			return &metrics.CrawlerStatus{StartedAt: time.Now(), Ready: true, Reachability: "reachable", NAT: "manual"}, nil
		}, func(context.Context, time.Time) (mysql.ResourceCounts, error) {
			calls++
			if unavailable {
				return mysql.ResourceCounts{}, errors.New("private database details")
			}
			return mysql.ResourceCounts{Total: 12, WithFiles: 9, ThisRunTotal: 4, ThisRunWithFiles: 1}, nil
		})
		router := gin.New()
		router.GET("/api/status", handler)
		for i := 0; i < 2; i++ {
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/status", nil))
			var status metrics.SystemStatus
			if recorder.Code != 200 || json.Unmarshal(recorder.Body.Bytes(), &status) != nil {
				t.Fatalf("bad response: %s", recorder.Body)
			}
			want := "operational"
			if unavailable {
				want = "outage"
			}
			if status.Status != want || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("unexpected status: %+v", status)
			}
			if unavailable && (status.Crawler != nil || status.TotalCollected != nil || status.Ingestion.AllTime != nil || status.Ingestion.ThisRun != nil) {
				t.Fatal("unavailable metrics must remain unknown")
			}
			if !unavailable {
				all, run := status.Ingestion.AllTime, status.Ingestion.ThisRun
				if all == nil || run == nil || all.Total != 12 || all.WithFiles != 9 || all.WithoutFiles != 3 ||
					run.Total != 4 || run.WithFiles != 1 || run.WithoutFiles != 3 ||
					all.WithFilesPercent == nil || *all.WithFilesPercent != 75 || run.WithFilesPercent == nil || *run.WithFilesPercent != 25 {
					t.Fatalf("incorrect ingestion coverage: %+v", status.Ingestion)
				}
			}
		}
		if calls != 1 {
			t.Fatal("status requests must reuse short cache")
		}
	}
}

func TestStatusCountTimeoutPreservesCrawler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	original := statusCountTimeout
	statusCountTimeout = 20 * time.Millisecond
	t.Cleanup(func() { statusCountTimeout = original })
	handler := statusHandler(func(context.Context) (*metrics.CrawlerStatus, error) {
		return &metrics.CrawlerStatus{StartedAt: time.Now(), Ready: true, Reachability: "reachable", NAT: "manual"}, nil
	}, func(ctx context.Context, _ time.Time) (mysql.ResourceCounts, error) {
		<-ctx.Done()
		return mysql.ResourceCounts{}, ctx.Err()
	})
	router := gin.New()
	router.GET("/api/status", handler)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	var status metrics.SystemStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Crawler == nil || status.TotalCollected != nil || status.Status != "outage" {
		t.Fatalf("count timeout discarded crawler status: %+v", status)
	}
}

func TestStatusCancellationDoesNotPoisonCache(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	calls := 0
	handler := statusHandler(func(fetchCtx context.Context) (*metrics.CrawlerStatus, error) {
		calls++
		cancel() // The requesting client disconnects mid-refresh.
		<-release
		if fetchCtx.Err() != nil {
			return nil, fetchCtx.Err()
		}
		return &metrics.CrawlerStatus{StartedAt: time.Now(), Ready: true, Reachability: "reachable", NAT: "manual"}, nil
	}, func(context.Context, time.Time) (mysql.ResourceCounts, error) {
		return mysql.ResourceCounts{Total: 12, WithFiles: 9, ThisRunTotal: 4, ThisRunWithFiles: 1}, nil
	})
	router := gin.New()
	router.GET("/api/status", handler)
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/status", nil).WithContext(ctx))
	close(release)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	var status metrics.SystemStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || status.Status != "operational" {
		t.Fatalf("canceled request poisoned cache: %+v, calls=%d", status, calls)
	}
}

func TestStatusConcurrentRequestsShareOneRefresh(t *testing.T) {
	gin.SetMode(gin.TestMode)
	release := make(chan struct{})
	var calls atomic.Int32
	handler := statusHandler(func(context.Context) (*metrics.CrawlerStatus, error) {
		calls.Add(1)
		<-release
		return &metrics.CrawlerStatus{StartedAt: time.Now(), Ready: true, Reachability: "reachable", NAT: "manual"}, nil
	}, func(context.Context, time.Time) (mysql.ResourceCounts, error) {
		return mysql.ResourceCounts{Total: 1}, nil
	})
	router := gin.New()
	router.GET("/api/status", handler)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/status", nil))
			if recorder.Code != http.StatusOK {
				t.Errorf("status %d", recorder.Code)
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("%d refreshes for concurrent requests, want 1", calls.Load())
	}
}

func TestStaticHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("INDEX"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(webDir, "assets"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "assets", "app.js"), []byte("APP"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(webDir), "secret.txt"), []byte("SECRET"), 0o600); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.NoRoute(staticHandler(webDir))
	for _, tc := range []struct {
		method, path string
		code         int
		body         string
	}{
		{http.MethodGet, "/", 200, "INDEX"},
		{http.MethodGet, "/assets/app.js", 200, "APP"},
		{http.MethodGet, "/assets/", 200, "INDEX"},
		{http.MethodGet, "/trending", 200, "INDEX"},
		{http.MethodGet, "/../secret.txt", 400, ""}, // http.ServeFile rejects "..".
		{http.MethodGet, "/api/unknown", 404, ""},
		{http.MethodPost, "/", 404, ""},
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
		body := recorder.Body.String()
		if recorder.Code != tc.code || tc.body != "" && body != tc.body || strings.Contains(body, "SECRET") || strings.Contains(body, "<pre>") {
			t.Errorf("%s %s: %d %q", tc.method, tc.path, recorder.Code, body)
		}
	}
}

func TestFetchCrawlerStatus(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		code       int
		valid      bool
	}{
		{"ok", `{"started_at":"2026-11-15T00:00:00Z"}`, 200, true},
		{"missing timestamp", `{}`, 200, false}, {"malformed", `not json`, 200, false}, {"unavailable", `{}`, 503, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.code); _, _ = w.Write([]byte(tc.body)) }))
			defer server.Close()
			_, err := fetchCrawlerStatus(context.Background(), server.Client(), server.URL)
			if (err == nil) != tc.valid {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
