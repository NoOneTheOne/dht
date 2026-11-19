package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	"github.com/NoOneTheOne/dht/internal/logging"
	"github.com/NoOneTheOne/dht/internal/metrics"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gorm.io/gorm"
)

const maxQueryLen = 200

var startTime = time.Now()

func main() {
	defer logging.Init("dht-search")()
	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()
	gin.DefaultWriter = logging.Writer(slog.LevelInfo)
	gin.DefaultErrorWriter = logging.Writer(slog.LevelError)
	// Fail at startup, not on the first request: GetConfig exits on error.
	config.RequireMySQL(config.GetConfig())
	router := gin.New()
	// Recovery is innermost so access logs and metrics still see the 500
	// produced for a panicking handler.
	router.Use(accessLog())
	router.Use(metrics.Middleware())
	router.Use(recoveryLog())

	// Set up MySQL stats getter for metrics
	metrics.SetMySQLStatsGetter(func() (open, inUse, idle int) {
		return mysqlStats(mysql.GetDBContext)
	})

	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	router.GET("/search", searchHandler)
	router.GET("/api/status", statusHandler(crawlerStatusGetter(), mysql.CountResources, mysql.Ping))
	trending := newTrendingCache(mysql.Trending)
	router.GET("/api/trending", trendingHandler(trending.lookup))
	router.GET("/api/recent", recentHandler(mysql.Recent))
	router.GET("/api/torrents/:infohash", detailHandler(mysql.Detail))

	router.GET("/health", func(c *gin.Context) {
		health := gin.H{
			"status":       "ok",
			"uptime":       time.Since(startTime).String(),
			"goroutines":   runtime.NumGoroutine(),
			"memory":       getMemoryUsage(),
			"config":       "loaded",
			"storage_type": "mysql",
		}

		// Check MySQL connection
		checkMySQLHealth(c.Request.Context(), health, mysql.GetDBContext)

		statusCode := http.StatusOK
		if health["status"] == "unhealthy" {
			statusCode = http.StatusServiceUnavailable
		}

		c.JSON(statusCode, health)
	})

	executable, err := os.Executable()
	if err != nil {
		logging.Fatalln(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		logging.Fatalln(err)
	}
	webDir := filepath.Join(filepath.Dir(executable), "web")
	if _, err := os.Stat(webDir); err == nil {
		router.NoRoute(staticHandler(webDir))
	} else {
		slog.Warn("web directory unavailable; serving API only", "path", webDir)
	}

	go buildSearchIndexes()
	go trending.warm()

	port := os.Getenv("SEARCH_API_PORT")
	if port == "" {
		port = "30080"
	}
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	err = server.ListenAndServe()
	if err != nil {
		logging.Fatalln(err)
	}
}

// staticHandler serves the built frontend. Unknown paths fall back to
// index.html for client-side routing; directories never produce listings.
func staticHandler(webDir string) gin.HandlerFunc {
	index := filepath.Join(webDir, "index.html")
	return func(c *gin.Context) {
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		path := c.Request.URL.Path
		if path == "/api" || strings.HasPrefix(path, "/api/") {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		// Cleaning a rooted path removes every "..", so the join stays in webDir.
		filePath := filepath.Join(webDir, filepath.Clean("/"+path))
		if info, err := os.Stat(filePath); err == nil && info.Mode().IsRegular() {
			c.File(filePath)
			return
		}
		c.File(index)
	}
}

// buildSearchIndexes creates the full-text search schema in the background,
// retrying until it succeeds; searches degrade gracefully meanwhile.
func buildSearchIndexes() {
	for delay := time.Minute; ; delay = min(2*delay, 30*time.Minute) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		err := mysql.EnsureSearchIndexes(ctx)
		cancel()
		if err == nil {
			slog.Info("search indexes ready", "component", "mysql", "event", "search_indexes_ready")
			return
		}
		slog.Warn("search index build failed; retrying", "component", "mysql", "event", "search_indexes_failed", "retry_in", delay.String(), "error", err)
		time.Sleep(delay)
	}
}

func searchHandler(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		metrics.RecordSearchError("missing_parameter")
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing required parameter: name"})
		return
	}
	if len(name) > maxQueryLen {
		metrics.RecordSearchError("query_too_long")
		c.JSON(http.StatusBadRequest, gin.H{"error": "query too long"})
		return
	}
	rawPage := "1"
	if values, present := c.Request.URL.Query()["page"]; present {
		rawPage = values[0]
	}
	page, err := strconv.ParseUint(rawPage, 10, 32)
	if err != nil || page == 0 || page > mysql.SearchPageMax {
		metrics.RecordSearchError("invalid_page")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page"})
		return
	}

	start := time.Now()
	result, err := mysql.Search(c.Request.Context(), name, int(page))
	metrics.RecordMySQLQuery("search", time.Since(start), err)
	if errors.Is(err, mysql.ErrSearchTooBroad) {
		metrics.RecordSearchError("too_broad")
		slog.WarnContext(c.Request.Context(), "search too broad", "component", "mysql", "event", "search_too_broad", "page", page, "error", err)
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "search too broad"})
		return
	}
	if err != nil {
		metrics.RecordSearchError("mysql_error")
		slog.ErrorContext(c.Request.Context(), "search failed", "component", "mysql", "event", "search_failed", "operation", "search", "page", page, "page_size", mysql.SearchPageSize, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if result.Items == nil {
		result.Items = []mysql.Data{}
	}
	metrics.RecordSearch(len(result.Items))
	c.JSON(http.StatusOK, gin.H{
		"total": result.Total, "items": result.Items,
		"page": page, "page_size": mysql.SearchPageSize,
		"files_searched": result.FilesSearched, "total_capped": result.TotalCapped,
	})
}

// trendingHandler serves rankings; lookup returns them with their computation time.
func trendingHandler(lookup func(context.Context, time.Duration, int) ([]mysql.Data, time.Time, error)) gin.HandlerFunc {
	return func(c *gin.Context) {
		period := c.DefaultQuery("window", "1h")
		windows := map[string]time.Duration{"1h": time.Hour, "3h": 3 * time.Hour, "6h": 6 * time.Hour, "12h": 12 * time.Hour, "24h": 24 * time.Hour}
		window, ok := windows[period]
		limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
		if !ok || err != nil || limit < 1 || limit > trendingLimitMax {
			c.JSON(http.StatusBadRequest, gin.H{"error": "window must be 1h, 3h, 6h, 12h or 24h; limit must be 1..100"})
			return
		}
		start := time.Now()
		items, until, err := lookup(c.Request.Context(), window, limit)
		metrics.RecordMySQLQuery("trending", time.Since(start), err)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "trending lookup failed", "component", "mysql", "event", "trending_failed", "window", period, "limit", limit, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		if items == nil {
			items = []mysql.Data{}
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "window": period, "limit": limit, "since": until.Add(-window), "until": until})
	}
}

// recentHandler lists the newest collected resources; limit is 1..100, default 10.
func recentHandler(lookup func(context.Context, int) ([]mysql.Data, error)) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
		if err != nil || limit < 1 || limit > mysql.RecentLimitMax {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be 1..100"})
			return
		}
		start := time.Now()
		items, err := lookup(c.Request.Context(), limit)
		metrics.RecordMySQLQuery("recent", time.Since(start), err)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "recent lookup failed", "component", "mysql", "event", "recent_failed", "limit", limit, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		if items == nil {
			items = []mysql.Data{}
		}
		c.Header("Cache-Control", "no-store")
		c.JSON(http.StatusOK, gin.H{"items": items, "limit": limit})
	}
}

func detailHandler(lookup func(context.Context, string) (*mysql.Data, error)) gin.HandlerFunc {
	return func(c *gin.Context) {
		hash := c.Param("infohash")
		decoded, err := hex.DecodeString(hash)
		if err != nil || len(decoded) != 20 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid infohash"})
			return
		}
		start := time.Now()
		result, err := lookup(c.Request.Context(), strings.ToLower(hash))
		metrics.RecordMySQLQuery("detail", time.Since(start), err)
		if errors.Is(err, gorm.ErrRecordNotFound) || err == nil && (result == nil || len(result.Files) == 0) {
			c.JSON(http.StatusNotFound, gin.H{"error": "file list not found"})
			return
		}
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "detail lookup failed", "component", "mysql", "event", "detail_failed", "operation", "detail", "infohash", hash, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func recoveryLog() gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(nil, func(c *gin.Context, _ interface{}) {
		slog.ErrorContext(c.Request.Context(), "HTTP handler panic", "component", "http", "event", "http_panic",
			"method", c.Request.Method, "path", c.Request.URL.Path, "stack", string(debug.Stack()))
		c.AbortWithStatus(http.StatusInternalServerError)
	})
}

func accessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		requestID := rand.Text()
		c.Request = c.Request.WithContext(logging.WithRequestID(c.Request.Context(), requestID))
		c.Header("X-Request-ID", requestID)
		c.Next()
		status := c.Writer.Status()
		level := slog.LevelInfo
		if status >= http.StatusInternalServerError {
			level = slog.LevelError
		} else if status >= http.StatusBadRequest {
			level = slog.LevelWarn
		}
		slog.Log(c.Request.Context(), level, "HTTP request", "component", "http", "event", "http_request",
			"method", c.Request.Method, "path", c.Request.URL.Path, "route", c.FullPath(),
			"protocol", c.Request.Proto, "request_bytes", c.Request.ContentLength,
			"status", status, "duration_ms", float64(time.Since(start))/float64(time.Millisecond),
			"response_bytes", c.Writer.Size(), "client_ip", c.ClientIP())
	}
}

func mysqlStats(getDB func(context.Context) (*gorm.DB, error)) (open, inUse, idle int) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	db, err := getDB(ctx)
	if err != nil {
		return 0, 0, 0
	}
	pool, err := db.DB()
	if err != nil {
		return 0, 0, 0
	}
	stats := pool.Stats()
	return stats.OpenConnections, stats.InUse, stats.Idle
}

func checkMySQLHealth(parent context.Context, health gin.H, getDB func(context.Context) (*gorm.DB, error)) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	status := health["status"]
	health["status"] = "unhealthy"
	health["mysql"] = "unavailable"
	db, err := getDB(ctx)
	if err != nil {
		return
	}
	pool, err := db.DB()
	if err != nil {
		return
	}
	if err := pool.PingContext(ctx); err != nil {
		return
	}
	health["status"] = status
	health["mysql"] = "connected"
	stats := pool.Stats()
	health["mysql_pool"] = gin.H{
		"open_connections": stats.OpenConnections,
		"in_use":           stats.InUse,
		"idle":             stats.Idle,
	}
}

func getMemoryUsage() gin.H {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return gin.H{
		"alloc_mb":       m.Alloc / 1024 / 1024,
		"total_alloc_mb": m.TotalAlloc / 1024 / 1024,
		"sys_mb":         m.Sys / 1024 / 1024,
		"num_gc":         m.NumGC,
	}
}
