package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type gormLogContextKey struct{}

type gormContextHandler struct {
	slog.Handler
}

func (h gormContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if value, ok := ctx.Value(gormLogContextKey{}).(string); ok {
		record.AddAttrs(slog.String("request_id", value))
	}
	return h.Handler.Handle(ctx, record)
}

// These tests mutate slog's global default and must not run in parallel.
func captureGORMLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	previous := slog.Default()
	handler := slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(gormContextHandler{Handler: handler}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &output
}

func decodeGORMRecord(t *testing.T, output *bytes.Buffer) map[string]interface{} {
	t.Helper()
	var record map[string]interface{}
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode JSON log: %v; output=%q", err, output.String())
	}
	return record
}

func TestGORMLoggerTrace(t *testing.T) {
	failure := errors.New("database unavailable")
	for _, tt := range []struct {
		name      string
		mode      logger.LogLevel
		elapsed   time.Duration
		err       error
		wantLevel string
	}{
		{name: "error", elapsed: time.Millisecond, err: failure, wantLevel: "ERROR"},
		{name: "slow", elapsed: time.Second, wantLevel: "WARN"},
		{name: "slow error prefers error", elapsed: time.Second, err: failure, wantLevel: "ERROR"},
		{name: "fast", elapsed: time.Millisecond},
		{name: "silent error", mode: logger.Silent, elapsed: time.Second, err: failure},
		{name: "silent slow", mode: logger.Silent, elapsed: time.Second},
		{name: "record not found", elapsed: time.Millisecond, err: gorm.ErrRecordNotFound},
		{name: "slow wrapped record not found", elapsed: time.Second, err: fmt.Errorf("lookup: %w", gorm.ErrRecordNotFound)},
		{name: "error mode suppresses slow", mode: logger.Error, elapsed: time.Second},
		{name: "error mode logs errors", mode: logger.Error, elapsed: time.Millisecond, err: failure, wantLevel: "ERROR"},
		{name: "info mode suppresses fast", mode: logger.Info, elapsed: time.Millisecond},
		{name: "info mode logs slow", mode: logger.Info, elapsed: time.Second, wantLevel: "WARN"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			output := captureGORMLogs(t)
			var log logger.Interface = newGORMLogger()
			if tt.mode != 0 {
				log = log.LogMode(tt.mode)
			}
			ctx := context.WithValue(context.Background(), gormLogContextKey{}, "trace-request")
			calls := 0
			const sql = "SELECT secret_column FROM private_table WHERE token = 'secret-parameter'"
			log.Trace(ctx, time.Now().Add(-tt.elapsed), func() (string, int64) {
				calls++
				return sql, 7
			}, tt.err)

			if tt.wantLevel == "" {
				if output.Len() != 0 || calls != 0 {
					t.Fatalf("suppressed trace: callback calls=%d, log=%q", calls, output.String())
				}
				return
			}
			if calls != 1 {
				t.Fatalf("callback calls=%d, want 1", calls)
			}
			record := decodeGORMRecord(t, output)
			if record["level"] != tt.wantLevel || record["component"] != "mysql" || record["request_id"] != "trace-request" {
				t.Fatalf("incorrect level, component or context: %v", record)
			}
			wantEvent := "mysql_slow_query"
			if tt.err != nil {
				wantEvent = "mysql_query_failed"
			}
			if record["event"] != wantEvent {
				t.Fatalf("event=%v, want %s", record["event"], wantEvent)
			}
			duration, ok := record["duration_ms"].(float64)
			if !ok || duration < float64(tt.elapsed)/float64(time.Millisecond) {
				t.Fatalf("duration_ms=%v, want elapsed milliseconds", record["duration_ms"])
			}
			if record["rows"] != float64(7) {
				t.Fatalf("rows=%v, want 7", record["rows"])
			}
			loggedError, exists := record["error"]
			if !exists || (tt.err == nil && loggedError != nil) || (tt.err != nil && loggedError != tt.err.Error()) {
				t.Fatalf("error=%v (exists=%v), want %v", loggedError, exists, tt.err)
			}
			for _, secret := range []string{sql, "SELECT", "secret_column", "private_table", "secret-parameter"} {
				if strings.Contains(output.String(), secret) {
					t.Fatalf("SQL or parameter leaked: %q", output.String())
				}
			}
			for _, key := range []string{"sql", "params", "parameters"} {
				if _, exists := record[key]; exists {
					t.Fatalf("unexpected SQL/parameter field %q", key)
				}
			}
		})
	}
}

func TestGORMLoggerMessagesAndLogMode(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode logger.LogLevel
		want []string
	}{
		{name: "silent", mode: logger.Silent},
		{name: "error", mode: logger.Error, want: []string{"ERROR"}},
		{name: "warn", mode: logger.Warn, want: []string{"WARN", "ERROR"}},
		{name: "info", mode: logger.Info, want: []string{"INFO", "WARN", "ERROR"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			output := captureGORMLogs(t)
			base := newGORMLogger()
			log := base.LogMode(tt.mode)
			if base.level != logger.Warn {
				t.Fatalf("LogMode mutated original logger: %v", base.level)
			}
			ctx := context.WithValue(context.Background(), gormLogContextKey{}, "message-request")
			log.Info(ctx, "message %s %d", "info", 1)
			log.Warn(ctx, "message %s %d", "warn", 2)
			log.Error(ctx, "message %s %d", "error", 3)
			decoder := json.NewDecoder(output)
			for _, level := range tt.want {
				var record map[string]interface{}
				if err := decoder.Decode(&record); err != nil {
					t.Fatalf("decode %s record: %v", level, err)
				}
				if record["level"] != level || record["component"] != "mysql" || record["request_id"] != "message-request" {
					t.Fatalf("incorrect level, component or context: %v", record)
				}
				wantMessage := map[string]string{"INFO": "message info 1", "WARN": "message warn 2", "ERROR": "message error 3"}[level]
				if record["msg"] != wantMessage {
					t.Fatalf("msg=%v, want %q", record["msg"], wantMessage)
				}
			}
			if decoder.More() {
				t.Fatal("unexpected extra log record")
			}
		})
	}
}

func TestGORMLoggerUsesCurrentDefault(t *testing.T) {
	log := newGORMLogger()
	first := captureGORMLogs(t)
	log.Warn(context.Background(), "first")
	second := captureGORMLogs(t)
	log.Warn(context.Background(), "second")
	if record := decodeGORMRecord(t, first); record["msg"] != "first" {
		t.Fatalf("first default received %v", record)
	}
	if record := decodeGORMRecord(t, second); record["msg"] != "second" {
		t.Fatalf("new default received %v", record)
	}
}
