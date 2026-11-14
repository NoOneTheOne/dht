package logging

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
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
	slog.SetDefault(newLogger(&output, "test"))
	message := "quoted \"value\"\nsecond line\t\\path"
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
	if records[1]["status"] != float64(500) || records[1]["duration_ms"] != 1.5 {
		t.Fatal("numeric fields were converted to strings")
	}
}

func TestConcurrentJSONLogs(t *testing.T) {
	preserveLogger(t)
	var output bytes.Buffer
	slog.SetDefault(newLogger(&output, "test"))
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
