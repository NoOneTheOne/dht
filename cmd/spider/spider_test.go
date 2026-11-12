package main

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

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
		{"single", map[string]interface{}{"length": 7}, 7, 0, true},
		{"empty single", map[string]interface{}{"length": 0}, 0, 0, true},
		{"multiple", map[string]interface{}{"files": []interface{}{entry(2), entry(3)}}, 5, 2, true},
		{"missing", map[string]interface{}{}, 0, 0, false},
		{"negative single", map[string]interface{}{"length": -1}, 0, 0, false},
		{"negative file", map[string]interface{}{"files": []interface{}{entry(-1)}}, 0, 0, false},
		{"overflow", map[string]interface{}{"files": []interface{}{entry(int(math.MaxInt64)), entry(1)}}, 0, 0, false},
		{"bad length", map[string]interface{}{"files": []interface{}{entry("2")}}, 0, 0, false},
		{"bad files", map[string]interface{}{"files": "invalid"}, 0, 0, false},
		{"bad file", map[string]interface{}{"files": []interface{}{2}}, 0, 0, false},
		{"bad path", map[string]interface{}{"files": []interface{}{map[string]interface{}{"length": 2, "path": []interface{}{2}}}}, 0, 0, false},
		{"conflicting", map[string]interface{}{"length": 5, "files": []interface{}{entry(5)}}, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files, length, valid := torrentFiles(tc.info)
			if valid != tc.valid || length != tc.length || len(files) != tc.count {
				t.Fatalf("got %d files, length %d, valid %v", len(files), length, valid)
			}
			if !valid {
				return
			}
			mysqlData := mysql.Data{Length: length}
			esData, err := json.Marshal(bitTorrent{Files: files, Length: length})
			if err != nil {
				t.Fatal(err)
			}
			var decoded bitTorrent
			if err := json.Unmarshal(esData, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.Length != mysqlData.Length {
				t.Fatal("backend totals differ")
			}
		})
	}
}
