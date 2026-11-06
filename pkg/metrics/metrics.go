package metrics

import (
	"runtime"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTP metrics
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dht_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dht_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	httpRequestSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dht_http_request_size_bytes",
			Help:    "HTTP request size in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 7),
		},
		[]string{"method", "path"},
	)

	httpResponseSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dht_http_response_size_bytes",
			Help:    "HTTP response size in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 7),
		},
		[]string{"method", "path"},
	)

	// Search metrics
	searchTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dht_search_total",
			Help: "Total number of search requests",
		},
	)

	searchResultCount = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dht_search_result_count",
			Help:    "Number of results per search",
			Buckets: []float64{0, 1, 5, 10, 25, 50, 100, 500, 1000},
		},
	)

	searchErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dht_search_errors_total",
			Help: "Total number of search errors",
		},
		[]string{"type"},
	)

	// MySQL metrics
	mysqlQueryDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dht_mysql_query_duration_seconds",
			Help:    "MySQL query duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	mysqlQueryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dht_mysql_query_total",
			Help: "Total number of MySQL queries",
		},
		[]string{"operation", "status"},
	)

	mysqlConnectionsOpen = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_mysql_connections_open",
			Help: "Number of open MySQL connections",
		},
		getMySQLOpenConnections,
	)

	mysqlConnectionsInUse = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_mysql_connections_in_use",
			Help: "Number of in-use MySQL connections",
		},
		getMySQLInUseConnections,
	)

	mysqlConnectionsIdle = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_mysql_connections_idle",
			Help: "Number of idle MySQL connections",
		},
		getMySQLIdleConnections,
	)

	// System metrics
	goroutinesGauge = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_goroutines",
			Help: "Number of goroutines",
		},
		func() float64 {
			return float64(runtime.NumGoroutine())
		},
	)

	memoryAllocGauge = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_memory_alloc_bytes",
			Help: "Current memory allocation in bytes",
		},
		func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.Alloc)
		},
	)

	memorySysGauge = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_memory_sys_bytes",
			Help: "Total memory obtained from system in bytes",
		},
		func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.Sys)
		},
	)

	gcCountGauge = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_gc_count_total",
			Help: "Total number of GC runs",
		},
		func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.NumGC)
		},
	)

	gcPauseGauge = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_gc_pause_seconds",
			Help: "Last GC pause duration in seconds",
		},
		func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.NumGC > 0 {
				return float64(m.PauseNs[(m.NumGC+255)%256]) / 1e9
			}
			return 0
		},
	)

	// Uptime
	startTime = time.Now()
	uptimeGauge = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dht_uptime_seconds",
			Help: "Service uptime in seconds",
		},
		func() float64 {
			return time.Since(startTime).Seconds()
		},
	)
)

// MySQL stats getters (will be set by mysql package)
var (
	mysqlStatsGetter func() (open, inUse, idle int)
)

// SetMySQLStatsGetter sets the function to get MySQL connection stats
func SetMySQLStatsGetter(getter func() (open, inUse, idle int)) {
	mysqlStatsGetter = getter
}

func getMySQLOpenConnections() float64 {
	if mysqlStatsGetter != nil {
		open, _, _ := mysqlStatsGetter()
		return float64(open)
	}
	return 0
}

func getMySQLInUseConnections() float64 {
	if mysqlStatsGetter != nil {
		_, inUse, _ := mysqlStatsGetter()
		return float64(inUse)
	}
	return 0
}

func getMySQLIdleConnections() float64 {
	if mysqlStatsGetter != nil {
		_, _, idle := mysqlStatsGetter()
		return float64(idle)
	}
	return 0
}

// Middleware returns a Gin middleware that records HTTP metrics
func Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath()
		if path == "" {
			path = "unknown"
		}

		// Record request size
		reqSize := float64(c.Request.ContentLength)
		if reqSize > 0 {
			httpRequestSize.WithLabelValues(c.Request.Method, path).Observe(reqSize)
		}

		c.Next()

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())

		httpRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		httpRequestDuration.WithLabelValues(c.Request.Method, path).Observe(duration)
		httpResponseSize.WithLabelValues(c.Request.Method, path).Observe(float64(c.Writer.Size()))
	}
}

// RecordSearch increments the search counter and records result count
func RecordSearch(resultCount int) {
	searchTotal.Inc()
	searchResultCount.Observe(float64(resultCount))
}

// RecordSearchError increments the search error counter
func RecordSearchError(errType string) {
	searchErrors.WithLabelValues(errType).Inc()
}

// RecordMySQLQuery records a MySQL query duration and status
func RecordMySQLQuery(operation string, duration time.Duration, err error) {
	mysqlQueryDuration.Observe(duration.Seconds())
	status := "success"
	if err != nil {
		status = "error"
	}
	mysqlQueryTotal.WithLabelValues(operation, status).Inc()
}
