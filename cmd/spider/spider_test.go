package main

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

func TestWriteMySQLBatches(t *testing.T) {
	for _, n := range []int{0, 1, 100, 205} {
		input := make(chan mysql.Data, n)
		for i := 0; i < n; i++ {
			input <- mysql.Data{Length: int64(i)}
		}
		close(input)
		var batches, seen int
		writeMySQLBatches(input, nil, func(items []mysql.Data) error {
			batches++
			if len(items) == 0 || len(items) > 100 {
				t.Fatalf("invalid batch length %d", len(items))
			}
			for _, item := range items {
				if item.Length != int64(seen) {
					t.Fatal("observation lost or repeated")
				}
				seen++
			}
			return nil
		})
		if seen != n || batches != (n+99)/100 {
			t.Fatalf("n=%d: saw %d items in %d batches", n, seen, batches)
		}
	}
}

func TestWriteMySQLBatchTickAndFailure(t *testing.T) {
	input := make(chan mysql.Data)
	ticks := make(chan time.Time)
	written := make(chan int, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		writeMySQLBatches(input, ticks, func(items []mysql.Data) error {
			written <- len(items)
			return errors.New("uncertain commit")
		})
	}()
	input <- mysql.Data{}
	ticks <- time.Now()
	if n := <-written; n != 1 {
		t.Fatalf("tick flushed %d rows", n)
	}
	input <- mysql.Data{}
	close(input)
	wg.Wait()
	if n := <-written; n != 1 || len(written) != 0 {
		t.Fatal("failed write retried or prevented subsequent flush")
	}
}

func TestTorrentFiles(t *testing.T) {
	entry := func(length interface{}) interface{} {
		return map[string]interface{}{"length": length, "path": []interface{}{"file"}}
	}
	for _, tc := range []struct {
		name   string
		info   map[string]interface{}
		length int64
		count  int
		valid  bool
	}{
		{"single", map[string]interface{}{"name": "single", "length": 7}, 7, 1, true},
		{"single int64", map[string]interface{}{"name": "single", "length": int64(7)}, 7, 1, true},
		{"empty single", map[string]interface{}{"name": "empty", "length": 0}, 0, 1, true},
		{"multiple", map[string]interface{}{"name": "multi", "files": []interface{}{entry(2), entry(3)}}, 5, 2, true},
		{"nested path", map[string]interface{}{"name": "multi", "files": []interface{}{map[string]interface{}{"length": 0, "path": []interface{}{"folder", "empty.txt"}}}}, 0, 1, true},
		{"empty files", map[string]interface{}{"name": "multi", "files": []interface{}{}}, 0, 0, false},
		{"empty path", map[string]interface{}{"name": "multi", "files": []interface{}{map[string]interface{}{"length": 2, "path": []interface{}{}}}}, 0, 0, false},
		{"missing", map[string]interface{}{}, 0, 0, false},
		{"negative single", map[string]interface{}{"name": "single", "length": -1}, 0, 0, false},
		{"negative file", map[string]interface{}{"name": "multi", "files": []interface{}{entry(-1)}}, 0, 0, false},
		{"overflow", map[string]interface{}{"name": "multi", "files": []interface{}{entry(int64(math.MaxInt64)), entry(1)}}, 0, 0, false},
		{"bad length", map[string]interface{}{"name": "multi", "files": []interface{}{entry("2")}}, 0, 0, false},
		{"bad files", map[string]interface{}{"name": "multi", "files": "invalid"}, 0, 0, false},
		{"bad file", map[string]interface{}{"name": "multi", "files": []interface{}{2}}, 0, 0, false},
		{"bad path", map[string]interface{}{"name": "multi", "files": []interface{}{map[string]interface{}{"length": 2, "path": []interface{}{2}}}}, 0, 0, false},
		{"conflicting", map[string]interface{}{"name": "multi", "length": 5, "files": []interface{}{entry(5)}}, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files, length, valid := torrentFiles(tc.info)
			if valid != tc.valid || length != tc.length || len(files) != tc.count {
				t.Fatalf("got %d files, length %d, valid %v", len(files), length, valid)
			}
			if !valid {
				return
			}
			mysqlData := mysql.Data{Length: length, Files: files}
			mysqlJSON, err := json.Marshal(mysqlData)
			if err != nil {
				t.Fatal(err)
			}
			var mysqlDecoded mysql.Data
			if err := json.Unmarshal(mysqlJSON, &mysqlDecoded); err != nil || !reflect.DeepEqual(mysqlDecoded.Files, files) {
				t.Fatal("MySQL file list did not survive JSON serialization")
			}
			esData, err := json.Marshal(bitTorrent{Files: files, Length: length})
			if err != nil {
				t.Fatal(err)
			}
			var decoded bitTorrent
			if err := json.Unmarshal(esData, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.Length != mysqlData.Length || !reflect.DeepEqual(decoded.Files, mysqlData.Files) {
				t.Fatal("backend totals or file lists differ")
			}
		})
	}
}
