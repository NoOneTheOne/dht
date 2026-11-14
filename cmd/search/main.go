package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
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
	router := gin.New()
	router.Use(accessLog())
	router.Use(recoveryLog())
	router.Use(metrics.Middleware())

	// Set up MySQL stats getter for metrics
	metrics.SetMySQLStatsGetter(func() (open, inUse, idle int) {
		return mysqlStats(mysql.GetDB)
	})

	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	router.GET("/search", searchHandler)

	router.GET("/health", func(c *gin.Context) {
		health := gin.H{
			"status":     "ok",
			"uptime":     time.Since(startTime).String(),
			"goroutines": runtime.NumGoroutine(),
			"memory":     getMemoryUsage(),
		}

		// Check config
		cfg := config.GetConfig()
		if cfg == nil {
			health["status"] = "degraded"
			health["config"] = "not loaded"
		} else {
			health["config"] = "loaded"
			health["storage_type"] = cfg.Storage.Type
		}

		// Check MySQL connection
		checkMySQLHealth(c.Request.Context(), health, mysql.GetDB)

		statusCode := http.StatusOK
		if health["status"] == "unhealthy" {
			statusCode = http.StatusServiceUnavailable
		} else if health["status"] == "degraded" {
			statusCode = http.StatusPartialContent
		}

		c.JSON(statusCode, health)
	})

	webDir := filepath.Join(filepath.Dir(os.Args[0]), "web")
	if _, err := os.Stat(webDir); err == nil {
		router.NoRoute(func(c *gin.Context) {
			path := c.Request.URL.Path
			if path == "/" {
				path = "/index.html"
			}
			filePath := filepath.Join(webDir, filepath.Clean(path))
			// Prevent path traversal: ensure resolved path is under webDir
			rel, err := filepath.Rel(webDir, filePath)
			if err != nil || len(rel) >= 2 && rel[:2] == ".." {
				c.Status(http.StatusForbidden)
				return
			}
			if abs, err := filepath.Abs(filePath); err == nil {
				if absAbs, err := filepath.Abs(webDir); err == nil {
					if len(abs) < len(absAbs) || abs[:len(absAbs)] != absAbs {
						c.Status(http.StatusForbidden)
						return
					}
				}
			}
			if _, err := os.Stat(filePath); err == nil {
				c.File(filePath)
			} else {
				c.File(filepath.Join(webDir, "index.html"))
			}
		})
	} else {
		slog.Warn("web directory unavailable; serving API only", "path", webDir)
	}

	port := os.Getenv("SEARCH_API_PORT")
	if port == "" {
		port = "30080"
	}
	err := router.Run(":" + port)
	if err != nil {
		logging.Fatalln(err)
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
	if err != nil || page == 0 || page > uint64(int(^uint(0)>>1)/mysql.SearchPageSize) {
		metrics.RecordSearchError("invalid_page")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page"})
		return
	}

	start := time.Now()
	results, total, err := mysql.Search(c.Request.Context(), name, int(page))
	metrics.RecordMySQLQuery("search", time.Since(start), err)
	if err != nil {
		metrics.RecordSearchError("mysql_error")
		slog.ErrorContext(c.Request.Context(), "search failed", "component", "mysql", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	metrics.RecordSearch(len(results))
	c.JSON(http.StatusOK, gin.H{
		"total": total, "items": results,
		"page": page, "page_size": mysql.SearchPageSize,
	})
}

func recoveryLog() gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(nil, func(c *gin.Context, _ interface{}) {
		slog.ErrorContext(c.Request.Context(), "HTTP handler panic", "component", "http",
			"method", c.Request.Method, "path", c.Request.URL.Path, "stack", string(debug.Stack()))
		c.AbortWithStatus(http.StatusInternalServerError)
	})
}

func accessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		status := c.Writer.Status()
		level := slog.LevelInfo
		if status >= http.StatusInternalServerError {
			level = slog.LevelError
		} else if status >= http.StatusBadRequest {
			level = slog.LevelWarn
		}
		slog.Log(c.Request.Context(), level, "HTTP request", "component", "http",
			"method", c.Request.Method, "path", c.Request.URL.Path,
			"status", status, "duration_ms", float64(time.Since(start))/float64(time.Millisecond),
			"response_bytes", c.Writer.Size(), "client_ip", c.ClientIP())
	}
}

func mysqlStats(getDB func() (*gorm.DB, error)) (open, inUse, idle int) {
	db, err := getDB()
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

func checkMySQLHealth(parent context.Context, health gin.H, getDB func() (*gorm.DB, error)) {
	status := health["status"]
	health["status"] = "unhealthy"
	health["mysql"] = "unavailable"
	db, err := getDB()
	if err != nil {
		return
	}
	pool, err := db.DB()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
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
