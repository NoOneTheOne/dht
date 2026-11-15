package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
	calls := 0
	handler := statusHandler(func(ctx context.Context) (*metrics.CrawlerStatus, error) {
		calls++
		if calls == 1 {
			cancel()
			return nil, ctx.Err()
		}
		return &metrics.CrawlerStatus{StartedAt: time.Now(), Ready: true, Reachability: "reachable", NAT: "manual"}, nil
	}, func(context.Context, time.Time) (mysql.ResourceCounts, error) {
		return mysql.ResourceCounts{Total: 12, WithFiles: 9, ThisRunTotal: 4, ThisRunWithFiles: 1}, nil
	})
	router := gin.New()
	router.GET("/api/status", handler)
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/status", nil).WithContext(ctx))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	var status metrics.SystemStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || status.Status != "operational" {
		t.Fatalf("canceled request poisoned cache: %+v, calls=%d", status, calls)
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
