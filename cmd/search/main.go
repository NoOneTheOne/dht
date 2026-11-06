package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	"github.com/NoOneTheOne/dht/internal/metrics"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const maxQueryLen = 200

var startTime = time.Now()

func main() {
	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()
	gin.DefaultWriter = io.MultiWriter(os.Stdout)
	router := gin.New()
	router.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("{\"@timestamp\":\"%s\",\"remoteAddr\":\"%s\",\"request_method\":\"%s\",\"request_uri\":\"%s\",\"protocol\":\"%s\",\"status\":\"%d\",\"requestTime\":\"%s\",\"httpUserAgent\":\"%s\",\"Errormessage\":\"%s\"}\n",
			param.TimeStamp.Format(time.RFC3339),
			param.ClientIP,
			param.Method,
			param.Path,
			param.Request.Proto,
			param.StatusCode,
			param.Latency,
			param.Request.UserAgent(),
			param.ErrorMessage,
		)
	}))
	router.Use(gin.Recovery())
	router.Use(metrics.Middleware())

	// Set up MySQL stats getter for metrics
	metrics.SetMySQLStatsGetter(func() (open, inUse, idle int) {
		db, err := mysql.GetDB().DB()
		if err != nil {
			return 0, 0, 0
		}
		stats := db.Stats()
		return stats.OpenConnections, stats.InUse, stats.Idle
	})

	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	router.GET("/search", func(c *gin.Context) {
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

		start := time.Now()
		results, total, err := mysql.Search(name)
		duration := time.Since(start)
		metrics.RecordMySQLQuery("search", duration, err)

		if err != nil {
			metrics.RecordSearchError("mysql_error")
			log.Printf("search error: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}

		metrics.RecordSearch(len(results))
		c.JSON(http.StatusOK, gin.H{
			"total": total,
			"items": results,
		})
	})

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
		db, err := mysql.GetDB().DB()
		if err != nil {
			health["status"] = "unhealthy"
			health["mysql"] = "error: " + err.Error()
		} else if err := db.Ping(); err != nil {
			health["status"] = "unhealthy"
			health["mysql"] = "ping failed: " + err.Error()
		} else {
			health["mysql"] = "connected"
			stats := db.Stats()
			health["mysql_pool"] = gin.H{
				"open_connections": stats.OpenConnections,
				"in_use":           stats.InUse,
				"idle":             stats.Idle,
			}
		}

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
		log.Printf("web directory not found at %s, serving API only", webDir)
	}

	port := os.Getenv("SEARCH_API_PORT")
	if port == "" {
		port = "30080"
	}
	err := router.Run(":" + port)
	if err != nil {
		log.Fatalln(err)
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
