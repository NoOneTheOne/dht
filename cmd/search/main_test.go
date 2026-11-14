package main

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/logging"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestSearchHandlerInvalidParameters(t *testing.T) {
	router := gin.New()
	router.GET("/search", searchHandler)
	queries := []string{
		"", "name=" + strings.Repeat("a", maxQueryLen+1),
		"name=test&page=", "name=test&page=0", "name=test&page=-1",
		"name=test&page=abc", "name=test&page=1.5", "name=test&page=%2B1",
		"name=test&page=%201", "name=test&page=4294967296",
		"name=test&page=18446744073709551616",
		"name=test&page=" + strconv.FormatUint(uint64(int(^uint(0)>>1)/50)+1, 10),
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/search?"+query, nil))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
			}
			var body map[string]interface{}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["error"] == nil {
				t.Fatalf("invalid error response: %s", response.Body.String())
			}
		})
	}
}

func TestDetailHandler(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hash   string
		data   *mysql.Data
		err    error
		status int
	}{
		{"invalid", "invalid", nil, nil, http.StatusBadRequest},
		{"invalid hex", strings.Repeat("z", 40), nil, nil, http.StatusBadRequest},
		{"missing", strings.Repeat("a", 40), nil, gorm.ErrRecordNotFound, http.StatusNotFound},
		{"no files", strings.Repeat("a", 40), &mysql.Data{Name: "legacy"}, nil, http.StatusNotFound},
		{"database failure", strings.Repeat("a", 40), nil, errors.New("backend unavailable"), http.StatusInternalServerError},
		{"files", strings.Repeat("A", 40), &mysql.Data{InfoHash: strings.Repeat("a", 40), Name: "resource", Files: []mysql.File{{Path: []string{"folder", "empty.txt"}, Length: 0}}}, nil, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			type contextKey struct{}
			router := gin.New()
			router.GET("/api/torrents/:infohash", detailHandler(func(ctx context.Context, hash string) (*mysql.Data, error) {
				calls++
				if hash != strings.ToLower(tc.hash) || ctx.Value(contextKey{}) != "request" {
					t.Fatal("lookup lost hash normalization or request context")
				}
				return tc.data, tc.err
			}))
			request := httptest.NewRequest(http.MethodGet, "/api/torrents/"+tc.hash, nil)
			request = request.WithContext(context.WithValue(request.Context(), contextKey{}, "request"))
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			wantCalls := 1
			if tc.status == http.StatusBadRequest {
				wantCalls = 0
			}
			if response.Code != tc.status || calls != wantCalls {
				t.Fatalf("status=%d calls=%d body=%s", response.Code, calls, response.Body.String())
			}
			if tc.status == http.StatusOK {
				var body mysql.Data
				if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || len(body.Files) != 1 || strings.Join(body.Files[0].Path, "/") != "folder/empty.txt" {
					t.Fatalf("invalid detail response: %s", response.Body.String())
				}
			} else if strings.Contains(response.Body.String(), "backend unavailable") || strings.Contains(response.Body.String(), "legacy") {
				t.Fatal("error response exposed metadata or backend error")
			}
		})
	}
}

func TestHTTPJSONLogs(t *testing.T) {
	previous := slog.Default()
	output, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	mode := gin.Mode()
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(output)
		log.SetFlags(flags)
		log.SetPrefix(prefix)
		gin.SetMode(mode)
	})
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(accessLog(), recoveryLog())
	router.GET("/quoted\"path", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	router.GET("/panic", func(*gin.Context) { panic("private-panic-value") })
	router.GET("/bad", func(c *gin.Context) { c.Status(http.StatusBadRequest) })
	var requestIDs []string
	for _, path := range []string{"/quoted%22path?token=private-query", "/panic", "/bad"} {
		request := httptest.NewRequest(http.MethodGet, path, strings.NewReader("private-body"))
		request.Header.Set("Authorization", "private-auth")
		request.Header.Set("User-Agent", "private-agent")
		request.Header.Set("X-Request-ID", "untrusted-request-id")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		requestID := response.Header().Get("X-Request-ID")
		if requestID == "" || requestID == "untrusted-request-id" {
			t.Fatalf("invalid generated request ID: %q", requestID)
		}
		for _, previousID := range requestIDs {
			if previousID == requestID {
				t.Fatal("request IDs must be unique")
			}
		}
		requestIDs = append(requestIDs, requestID)
		if path == "/panic" && response.Code != http.StatusInternalServerError {
			t.Fatalf("panic status = %d", response.Code)
		}
	}
	var records []map[string]interface{}
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte{'\n'}) {
		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("invalid log JSON: %v", err)
		}
		records = append(records, record)
	}
	if len(records) != 4 || records[0]["path"] != "/quoted\"path" || records[0]["status"] != float64(204) ||
		records[1]["level"] != "ERROR" || records[1]["stack"] == nil || records[1]["stack"] == "" ||
		records[2]["status"] != float64(500) || records[3]["level"] != "WARN" {
		t.Fatalf("unexpected HTTP logs: %v", records)
	}
	if records[0]["event"] != "http_request" || records[0]["route"] != "/quoted\"path" ||
		records[0]["protocol"] != "HTTP/1.1" || records[0]["request_bytes"] != float64(len("private-body")) {
		t.Fatalf("missing HTTP fields: %v", records[0])
	}
	if _, ok := records[0]["duration_ms"].(float64); !ok {
		t.Fatal("duration must be numeric")
	}
	for _, secret := range []string{"private-query", "private-body", "private-auth", "private-agent", "private-panic-value"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("HTTP logs exposed %s", secret)
		}
	}
}

func TestHTTPRequestLogCorrelation(t *testing.T) {
	previous := slog.Default()
	output, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	mode := gin.Mode()
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(output)
		log.SetFlags(flags)
		log.SetPrefix(prefix)
		gin.SetMode(mode)
	})
	path := filepath.Join(t.TempDir(), "requests.jsonl")
	t.Setenv("LOG_FILE_PATH", path)
	closeLog := logging.Init("dht-search")
	t.Cleanup(closeLog)
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(accessLog(), recoveryLog())
	router.GET("/failure", func(c *gin.Context) {
		slog.ErrorContext(c.Request.Context(), "database failure", "component", "mysql")
		panic("private-value")
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/failure", nil))
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(content), []byte{'\n'})
	if len(lines) != 3 || response.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected response/logs: %d %s", response.Code, content)
	}
	for _, line := range lines {
		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record["request_id"] != response.Header().Get("X-Request-ID") || record["request_id"] == "" {
			t.Fatalf("request ID lost: %v", record)
		}
	}
}

func TestMySQLInitializationFailure(t *testing.T) {
	getDB := func() (*gorm.DB, error) {
		return nil, errors.New("synthetic private backend details")
	}
	health := gin.H{"status": "ok"}
	checkMySQLHealth(context.Background(), health, getDB)
	if health["status"] != "unhealthy" || health["mysql"] != "unavailable" {
		t.Fatalf("unexpected health response: %v", health)
	}
	if _, ok := health["mysql_pool"]; ok {
		t.Fatal("failed initialization exposed pool stats")
	}
	if open, inUse, idle := mysqlStats(getDB); open != 0 || inUse != 0 || idle != 0 {
		t.Fatalf("failed initialization stats = %d, %d, %d", open, inUse, idle)
	}
}

func TestMySQLPoolAccessFailure(t *testing.T) {
	getDB := func() (*gorm.DB, error) {
		return &gorm.DB{Config: &gorm.Config{}}, nil
	}
	health := gin.H{"status": "ok"}
	checkMySQLHealth(context.Background(), health, getDB)
	if health["status"] != "unhealthy" || health["mysql"] != "unavailable" {
		t.Fatalf("unexpected health response: %v", health)
	}
	if open, inUse, idle := mysqlStats(getDB); open != 0 || inUse != 0 || idle != 0 {
		t.Fatalf("pool access failure stats = %d, %d, %d", open, inUse, idle)
	}
}

func TestMySQLHealthPing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pingErr error
		status  string
		mysql   string
	}{
		{"success", nil, "degraded", "connected"},
		{"failure", errors.New("synthetic private ping details"), "unhealthy", "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &healthConn{pingErr: tc.pingErr}
			pool := sql.OpenDB(healthConnector{conn})
			t.Cleanup(func() { _ = pool.Close() })
			getDB := func() (*gorm.DB, error) {
				return &gorm.DB{Config: &gorm.Config{ConnPool: pool}}, nil
			}
			health := gin.H{"status": "degraded"}
			before := time.Now()
			checkMySQLHealth(context.Background(), health, getDB)
			if health["status"] != tc.status || health["mysql"] != tc.mysql {
				t.Fatalf("unexpected health response: %v", health)
			}
			if conn.pingContext == nil {
				t.Fatal("health did not call PingContext")
			}
			deadline, ok := conn.pingContext.Deadline()
			if !ok || deadline.Before(before.Add(time.Second)) || deadline.After(time.Now().Add(2*time.Second)) {
				t.Fatal("health ping must have a two-second deadline")
			}
			if !errors.Is(conn.pingContext.Err(), context.Canceled) {
				t.Fatal("health ping context was not canceled after use")
			}
			if tc.pingErr == nil {
				if _, ok := health["mysql_pool"]; !ok {
					t.Fatal("healthy connection omitted pool stats")
				}
				if open, inUse, idle := mysqlStats(getDB); open != 1 || inUse != 0 || idle != 1 {
					t.Fatalf("healthy pool stats = %d, %d, %d", open, inUse, idle)
				}
			} else if _, ok := health["mysql_pool"]; ok {
				t.Fatal("failed ping exposed pool stats")
			}
		})
	}
}

func TestMySQLHealthHonorsRequestCancellation(t *testing.T) {
	conn := &healthConn{}
	pool := sql.OpenDB(healthConnector{conn})
	t.Cleanup(func() { _ = pool.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	health := gin.H{"status": "ok"}
	checkMySQLHealth(ctx, health, func() (*gorm.DB, error) {
		return &gorm.DB{Config: &gorm.Config{ConnPool: pool}}, nil
	})
	if health["status"] != "unhealthy" || health["mysql"] != "unavailable" {
		t.Fatalf("canceled health response: %v", health)
	}
}

// Minimal database/sql fake: no external database or driver dependency.
type healthConnector struct{ conn *healthConn }

func (c healthConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c healthConnector) Driver() driver.Driver                        { return healthDriver{} }

type healthDriver struct{}

func (healthDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type healthConn struct {
	pingErr     error
	pingContext context.Context
}

func (c *healthConn) Ping(ctx context.Context) error {
	c.pingContext = ctx
	return c.pingErr
}
func (*healthConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (*healthConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (*healthConn) Close() error                        { return nil }
