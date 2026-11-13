package es

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v7"
)

const testInfohash = "0123456789abcdef0123456789abcdef01234567"

// All HTTP traffic, including the client's product check, stays in this transport.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testClient(t *testing.T, update roundTripFunc) *elasticsearch.Client {
	t.Helper()
	c, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    []string{"http://es.invalid"},
		DisableRetry: true,
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodGet && r.URL.Path == "/" {
				return testResponse(http.StatusOK, io.NopCloser(strings.NewReader(`{}`))), nil
			}
			return update(r)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func testResponse(status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
		Body:       body,
	}
}

type updatePayload struct {
	ScriptedUpsert bool `json:"scripted_upsert"`
	Script         struct {
		Lang   string `json:"lang"`
		Source string `json:"source"`
	} `json:"script"`
	Upsert map[string]json.RawMessage `json:"upsert"`
	Doc    json.RawMessage            `json:"doc"`
}

func checkUpdate(t *testing.T, r *http.Request) updatePayload {
	t.Helper()
	if r.Method != http.MethodPost || r.URL.Path != "/metadata/torrent/"+testInfohash+"/_update" {
		t.Fatalf("unexpected update: %s %s", r.Method, r.URL)
	}
	if got := r.URL.Query().Get("retry_on_conflict"); got != "3" {
		t.Fatalf("retry_on_conflict = %q, want 3", got)
	}
	if got := r.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	deadline, ok := r.Context().Deadline()
	if !ok || deadline.IsZero() {
		t.Fatal("update has no deadline")
	}
	var p updatePayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if !p.ScriptedUpsert || p.Doc != nil || p.Script.Lang != "painless" {
		t.Fatalf("not a script-only scripted upsert: %+v", p)
	}
	// Pin the complete script: it must handle absent/null count and must not
	// copy incoming metadata (particularly timestamp) over existing fields.
	wantScript := "if (!ctx._source.containsKey('count') || ctx._source.count == null) { " +
		"ctx._source.count = 0; } ctx._source.count += 1;"
	if p.Script.Source != wantScript {
		t.Fatalf("script = %q, want %q", p.Script.Source, wantScript)
	}
	if string(p.Upsert["count"]) != "0" || string(p.Upsert["infohash"]) != `"`+testInfohash+`"` {
		t.Fatalf("unexpected upsert: %s", p.Upsert)
	}
	return p
}

// This models only the pinned script's effects, not a Painless interpreter.
// Executing Painless and real version-conflict handling requires a real ES cluster.
func applyUpdate(t *testing.T, existing map[string]json.RawMessage, p updatePayload) map[string]json.RawMessage {
	t.Helper()
	if existing == nil {
		existing = p.Upsert
	}
	count := 0
	if raw, ok := existing["count"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &count); err != nil {
			t.Fatal(err)
		}
	}
	existing["count"] = json.RawMessage(fmt.Sprint(count + 1))
	return existing
}

func TestWriteSameInfohashPreservesFirstMetadata(t *testing.T) {
	var stored map[string]json.RawMessage
	calls := 0
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		calls++
		stored = applyUpdate(t, stored, checkUpdate(t, r))
		return testResponse(http.StatusOK, io.NopCloser(strings.NewReader(`{"result":"updated"}`))), nil
	})
	first := `{"infohash":"` + strings.ToUpper(testInfohash) + `","name":"first","timestamp":123,"size":9007199254740993,"count":999,"files":[{"path":"first.bin"}]}`
	if result, err := write(c, "metadata", "torrent", []byte(first)); err != nil || result != `[200 OK] {"result":"updated"}` {
		t.Fatalf("first write = %q, %v", result, err)
	}
	if string(stored["count"]) != "1" {
		t.Fatalf("initial count = %s, want 1", stored["count"])
	}
	firstMetadata := make(map[string]json.RawMessage)
	for key, value := range stored {
		if key != "count" {
			firstMetadata[key] = value
		}
	}
	second := `{"infohash":"` + testInfohash + `","name":"replacement","timestamp":456,"count":-42}`
	if _, err := write(c, "metadata", "torrent", []byte(second)); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || string(stored["count"]) != "2" {
		t.Fatalf("calls = %d, count = %s", calls, stored["count"])
	}
	delete(stored, "count")
	if !reflect.DeepEqual(stored, firstMetadata) {
		t.Fatalf("first metadata changed: got %s, want %s", stored, firstMetadata)
	}
	if string(stored["size"]) != "9007199254740993" {
		t.Fatal("large JSON integer lost precision")
	}
}

func TestWriteExistingCount(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count string
		want  string
	}{
		{name: "missing", want: "1"},
		{name: "null", count: "null", want: "1"},
		{name: "zero", count: "0", want: "1"},
		{name: "existing", count: "7", want: "8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stored := map[string]json.RawMessage{"timestamp": json.RawMessage("123"), "name": json.RawMessage(`"original"`)}
			if tc.count != "" {
				stored["count"] = json.RawMessage(tc.count)
			}
			c := testClient(t, func(r *http.Request) (*http.Response, error) {
				stored = applyUpdate(t, stored, checkUpdate(t, r))
				return testResponse(http.StatusOK, io.NopCloser(strings.NewReader(`{}`))), nil
			})
			data := `{"infohash":"` + testInfohash + `","timestamp":456,"count":99}`
			if _, err := write(c, "metadata", "torrent", []byte(data)); err != nil {
				t.Fatal(err)
			}
			if string(stored["count"]) != tc.want || string(stored["timestamp"]) != "123" || string(stored["name"]) != `"original"` {
				t.Fatalf("unexpected stored document: %s", stored)
			}
		})
	}
}

func TestWriteInvalidInputWithoutConfigOrHTTP(t *testing.T) {
	for _, data := range []string{
		``, `{`, `{} {}`, `null`, `[]`, `"text"`, `{}`,
		`{"infohash":null}`, `{"infohash":123}`, `{"infohash":{}}`,
		`{"infohash":""}`, `{"infohash":"` + testInfohash[:39] + `"}`,
		`{"infohash":"` + testInfohash + `0"}`,
		`{"infohash":"g` + testInfohash[1:] + `"}`,
		`{"infohash":" ` + testInfohash[1:] + `"}`,
	} {
		t.Run(data, func(t *testing.T) {
			// Public Write must reject these before resolving getClient/config.
			if result, err := Write("metadata", "torrent", []byte(data)); err == nil || result != "" {
				t.Fatalf("invalid input accepted: %q, %v", result, err)
			}
		})
	}
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestWriteErrorsAreNotRetried(t *testing.T) {
	lostResponse := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("response lost after commit")}
	readErr := errors.New("truncated response body")
	for _, tc := range []struct {
		name   string
		status int
		err    error
		reader io.Reader
		want   string
	}{
		{name: "bad request", status: 400, want: "status=400"},
		{name: "conflict exhausted", status: 409, want: "status=409"},
		{name: "rate limit", status: 429, want: "status=429"},
		{name: "bad gateway", status: 502, want: "status=502"},
		{name: "unavailable", status: 503, want: "status=503"},
		{name: "gateway timeout", status: 504, want: "status=504"},
		{name: "transport unknown result", err: lostResponse, want: lostResponse.Error()},
		{name: "body unknown result", status: 200, reader: errorReader{readErr}, want: readErr.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			body := &trackedBody{Reader: strings.NewReader(`{"error":"failed"}`)}
			if tc.reader != nil {
				body.Reader = tc.reader
			}
			c := testClient(t, func(r *http.Request) (*http.Response, error) {
				checkUpdate(t, r)
				calls++
				if tc.err != nil {
					return nil, tc.err
				}
				return testResponse(tc.status, body), nil
			})
			data := `{"infohash":"` + testInfohash + `"}`
			result, err := write(c, "metadata", "torrent", []byte(data))
			if err == nil || !strings.Contains(err.Error(), tc.want) || result != "" {
				t.Fatalf("write = %q, %v; want error containing %q", result, err, tc.want)
			}
			if calls != 1 {
				t.Fatalf("update sent %d times, want exactly one", calls)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("transport error lost: %v", err)
			}
			if tc.reader != nil && !errors.Is(err, readErr) {
				t.Fatalf("body error lost: %v", err)
			}
			if tc.err == nil && !body.closed {
				t.Fatal("response body not closed")
			}
		})
	}
}
