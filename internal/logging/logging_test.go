package logging

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

func preserveLogger(t *testing.T) {
	t.Helper()
	previous := slog.Default()
	output, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(output)
		log.SetFlags(flags)
		log.SetPrefix(prefix)
	})
}

func decodeRecords(t *testing.T, input io.Reader) []map[string]interface{} {
	t.Helper()
	var records []map[string]interface{}
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		var record map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("invalid JSON log: %v", err)
		}
		if _, err := time.Parse(time.RFC3339Nano, record["@timestamp"].(string)); err != nil {
			t.Fatal(err)
		}
		if record["service"] != "test" || record["time"] != nil || record["msg"] != nil {
			t.Fatalf("unexpected log schema: %v", record)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

func TestJSONLogsAndStandardLogBridge(t *testing.T) {
	preserveLogger(t)
	var output bytes.Buffer
	setDefaultLogger(newLogger(&output, "test"))
	message := "quoted \"value\"\nsecond line\t\\path"
	_, file, line, _ := runtime.Caller(0)
	log.Print(message)
	slog.Error(message, "status", 500, "duration_ms", 1.5)
	if _, err := Writer(slog.LevelWarn).Write([]byte(message)); err != nil {
		t.Fatal(err)
	}
	records := decodeRecords(t, &output)
	if len(records) != 3 {
		t.Fatalf("records = %d, want 3", len(records))
	}
	for i, level := range []string{"INFO", "ERROR", "WARN"} {
		if records[i]["message"] != message || records[i]["level"] != level {
			t.Fatalf("record %d lost message or level: %v", i, records[i])
		}
	}
	assertSource(t, records[0], file, line+1, ".TestJSONLogsAndStandardLogBridge")
	assertSource(t, records[1], file, line+2, ".TestJSONLogsAndStandardLogBridge")
	if records[1]["status"] != float64(500) || records[1]["duration_ms"] != 1.5 {
		t.Fatal("numeric fields were converted to strings")
	}
}

func TestConcurrentJSONLogs(t *testing.T) {
	preserveLogger(t)
	var output bytes.Buffer
	setDefaultLogger(newLogger(&output, "test"))
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			log.Print("multiple\nlines")
			slog.Info("structured", "count", 1)
		}()
	}
	workers.Wait()
	if records := decodeRecords(t, &output); len(records) != 64 {
		t.Fatalf("records = %d, want 64", len(records))
	}
}

func assertSource(t *testing.T, record map[string]interface{}, file string, line int, functionSuffix string) {
	t.Helper()
	source, ok := record["source"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing structured source: %v", record)
	}
	function, ok := source["function"].(string)
	if !ok || !strings.HasSuffix(function, functionSuffix) || source["file"] != file || source["line"] != float64(line) {
		t.Fatalf("source = %v, want %s:%d (%s)", source, file, line, functionSuffix)
	}
}

func TestCommonMetadata(t *testing.T) {
	previousVersion, previousRevision := Version, Revision
	t.Cleanup(func() { Version, Revision = previousVersion, previousRevision })
	Version, Revision = "test-version", "test-revision"
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	for _, environment := range []string{"", "staging"} {
		t.Run("environment="+environment, func(t *testing.T) {
			t.Setenv("APP_ENV", environment)
			var output bytes.Buffer
			newLogger(&output, "test").Info("metadata")
			records := decodeRecords(t, &output)
			if len(records) != 1 {
				t.Fatalf("records = %d, want 1", len(records))
			}
			wantEnvironment := environment
			if wantEnvironment == "" {
				wantEnvironment = "unknown"
			}
			for key, want := range map[string]interface{}{
				"host": host, "pid": float64(os.Getpid()), "environment": wantEnvironment,
				"version": "test-version", "revision": "test-revision", "go_version": runtime.Version(),
			} {
				if got := records[0][key]; got != want {
					t.Errorf("%s = %#v (%T), want %#v (%T)", key, got, got, want, want)
				}
			}
		})
	}
}

func TestReleaseEnvironment(t *testing.T) {
	previous := Environment
	t.Cleanup(func() { Environment = previous })
	for _, tt := range []struct{ build, override, want string }{
		{"unknown", "", "unknown"},
		{"prod", "", "prod"},
		{"prod", "staging", "staging"},
	} {
		t.Run(tt.build+"/"+tt.override, func(t *testing.T) {
			Environment = tt.build
			t.Setenv("APP_ENV", tt.override)
			var output bytes.Buffer
			newLogger(&output, "test").Info("environment")
			records := decodeRecords(t, &output)
			if len(records) != 1 || records[0]["environment"] != tt.want {
				t.Fatalf("environment should be %q: %v", tt.want, records)
			}
		})
	}
}

func TestUnknownRevisionUsesBuildInfo(t *testing.T) {
	previous := Revision
	t.Cleanup(func() { Revision = previous })
	Revision = "unknown"
	want := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				want = setting.Value
				break
			}
		}
	}
	var output bytes.Buffer
	newLogger(&output, "test").Info("revision")
	records := decodeRecords(t, &output)
	if len(records) != 1 || records[0]["revision"] != want {
		t.Fatalf("revision should be %q: %v", want, records)
	}
}

func TestRequestIDContext(t *testing.T) {
	base := context.Background()
	if RequestID(base) != "" || RequestID(nil) != "" {
		t.Fatal("absent request ID should be empty")
	}
	ctx := WithRequestID(base, "first")
	if RequestID(ctx) != "first" || RequestID(base) != "" {
		t.Fatal("request ID must not change the parent context")
	}
	if RequestID(WithRequestID(ctx, "second")) != "second" || RequestID(ctx) != "first" {
		t.Fatal("request ID override must not change the parent context")
	}
	if RequestID(WithRequestID(ctx, "")) != "" {
		t.Fatal("empty request ID should clear an inherited ID")
	}
}

func TestRequestIDWithDerivedAndGroupedLoggers(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(&output, "test")
	ctx := WithRequestID(context.Background(), "req-123")
	derived := logger.With("component", "api")
	grouped := derived.WithGroup("").WithGroup("http").With("method", "GET")
	nested := grouped.WithGroup("response").With("status", 200)
	sibling := derived.WithGroup("job").With("attempt", 1)
	logger.InfoContext(ctx, "root")
	derived.InfoContext(ctx, "derived")
	nested.InfoContext(ctx, "nested", "bytes", 12)
	sibling.InfoContext(ctx, "sibling")
	nested.Info("without request")
	nested.InfoContext(WithRequestID(ctx, "req-456"), "new request")
	logger.InfoContext(WithRequestID(ctx, ""), "empty request")
	records := decodeRecords(t, &output)
	if len(records) != 7 {
		t.Fatalf("records = %d, want 7", len(records))
	}
	for i, record := range records {
		want := "req-123"
		if i == 5 {
			want = "req-456"
		}
		if i == 4 || i == 6 {
			if _, exists := record["request_id"]; exists {
				t.Errorf("record %d retained request ID: %v", i, record)
			}
		} else if record["request_id"] != want {
			t.Errorf("record %d top-level request_id = %v, want %s", i, record["request_id"], want)
		}
		if i > 0 && i < 6 && record["component"] != "api" {
			t.Errorf("record %d lost derived attributes: %v", i, record)
		}
	}
	for _, index := range []int{2, 4, 5} {
		http, ok := records[index]["http"].(map[string]interface{})
		if !ok || http["method"] != "GET" || http["request_id"] != nil {
			t.Fatalf("incorrect http group: %v", records[index])
		}
		response, ok := http["response"].(map[string]interface{})
		if !ok || response["status"] != float64(200) || response["request_id"] != nil {
			t.Fatalf("incorrect response group: %v", http)
		}
	}
	if records[2]["http"].(map[string]interface{})["response"].(map[string]interface{})["bytes"] != float64(12) {
		t.Fatal("record attributes were lost")
	}
	job, ok := records[3]["job"].(map[string]interface{})
	if !ok || job["attempt"] != float64(1) || job["request_id"] != nil || records[3]["http"] != nil {
		t.Fatalf("sibling logger inherited another branch: %v", records[3])
	}
}

type countingLogValue struct {
	calls *int
}

func (v countingLogValue) LogValue() slog.Value {
	(*v.calls)++
	return slog.IntValue(*v.calls)
}

func TestDerivedAttributesResolveOnce(t *testing.T) {
	var output bytes.Buffer
	calls := 0
	logger := newLogger(&output, "test").WithGroup("details").With(
		slog.Group("snapshot", slog.Any("value", countingLogValue{calls: &calls})),
	)
	if calls != 1 {
		t.Fatalf("LogValuer calls after With = %d, want 1", calls)
	}
	for _, id := range []string{"first", "second", ""} {
		logger.InfoContext(WithRequestID(context.Background(), id), "snapshot")
	}
	if calls != 1 {
		t.Fatalf("request injection reevaluated LogValuer: %d calls", calls)
	}
	for _, record := range decodeRecords(t, &output) {
		details := record["details"].(map[string]interface{})
		snapshot := details["snapshot"].(map[string]interface{})
		if snapshot["value"] != float64(1) {
			t.Fatalf("derived value changed: %v", record)
		}
	}
}

// Match the production Fatal wrappers' stack depth without calling os.Exit.
func fatalWithoutExit(message string) {
	logFatal(message)
}

func TestFatalSource(t *testing.T) {
	preserveLogger(t)
	var output bytes.Buffer
	setDefaultLogger(newLogger(&output, "test"))
	_, file, line, _ := runtime.Caller(0)
	fatalWithoutExit("fatal message")
	records := decodeRecords(t, &output)
	if len(records) != 1 || records[0]["level"] != "ERROR" || records[0]["message"] != "fatal message" {
		t.Fatalf("incorrect fatal record: %v", records)
	}
	assertSource(t, records[0], file, line+1, ".TestFatalSource")
}

func TestInitStandardLogSource(t *testing.T) {
	preserveLogger(t)
	path := filepath.Join(t.TempDir(), "standard.jsonl")
	t.Setenv("LOG_FILE_PATH", path)
	closeLog := Init("test")
	t.Cleanup(closeLog)
	_, file, line, _ := runtime.Caller(0)
	log.Print("original caller")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	records := decodeRecords(t, bytes.NewReader(content))
	if len(records) != 1 || records[0]["message"] != "original caller" {
		t.Fatalf("incorrect standard log record: %v", records)
	}
	assertSource(t, records[0], file, line+1, ".TestInitStandardLogSource")
}

func TestLogFileAppend(t *testing.T) {
	preserveLogger(t)
	path := filepath.Join(t.TempDir(), "service.jsonl")
	t.Setenv("LOG_FILE_PATH", path)
	for i := 0; i < 2; i++ {
		closeLog := Init("test")
		slog.Info("append", "index", i)
		closeLog()
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if records := decodeRecords(t, strings.NewReader(string(content))); len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
}
