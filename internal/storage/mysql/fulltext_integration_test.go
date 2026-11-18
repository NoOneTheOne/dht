package mysql

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
)

// TestFulltextSearchAgainstMySQL runs only with DHT_TEST_MYSQL_ADDR set, e.g.
//
//	docker run --rm -d -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=ftscheck -p 127.0.0.1:33306:3306 mysql:8.4
//	DHT_TEST_MYSQL_ADDR=127.0.0.1:33306 go test -run Fulltext ./internal/storage/mysql/
//
// It checks behaviour that DryRun tests cannot: building the schema, ngram
// matching of names and file names, stopwords, ranking and fallbacks.
func TestFulltextSearchAgainstMySQL(t *testing.T) {
	addr := os.Getenv("DHT_TEST_MYSQL_ADDR")
	if addr == "" {
		t.Skip("DHT_TEST_MYSQL_ADDR not set")
	}
	cfg := &config.Config{}
	cfg.MySQL.Addresses, cfg.MySQL.Username, cfg.MySQL.Database = addr, "root", "ftscheck"
	cfg.MySQL.Table = "fts_" + time.Now().Format("150405")
	original := getCfg
	getCfg = func() *config.Config { return cfg }
	t.Cleanup(func() { getCfg = original })
	stubInitialization(t, openDB)

	base := time.Now().Add(-time.Hour)
	type row struct {
		name  string
		files [][]string
	}
	rows := []row{
		{"Ubuntu 26.04 LTS Desktop amd64", [][]string{{"casper", "vmlinuz"}, {"README.diskdefines"}}},
		{"intel graphics driver pack", [][]string{{"setup.exe"}}},
		{"赘婿之吉兴高照 2021 WEB-DL", [][]string{{"赘婿", "第一集.mp4"}, {"赘婿", "第二集.mp4"}}},
		{"Metropolis 1927 restored", [][]string{{"Metropolis.1927.mkv"}, {"Release Notes.TXT"}}},
		{"To be or not to be", [][]string{{"hamlet.epub"}}},
		{"release candidate build", [][]string{{"build.log"}}},
	}
	items := make([]Data, len(rows))
	for i, r := range rows {
		files := make([]File, len(r.files))
		for j, path := range r.files {
			files[j] = File{Path: path, Length: 1}
		}
		items[i] = Data{Name: r.name, InfoHash: strings.Repeat(string(rune('a'+i)), 40), Time: base.Add(time.Duration(i) * time.Minute), Files: files}
	}
	// "Metropolis" only matches "release" by a file name, yet is the newest row.
	items[3].Time = base.Add(time.Hour)
	if err := WriteBatch(items); err != nil {
		t.Fatal(err)
	}
	db, err := GetDBContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS `" + cfg.MySQL.Table + "`, `" + cfg.MySQL.Table + "_observations`")
	})

	search := func(query string) ([]string, []bool) {
		t.Helper()
		result, err := Search(context.Background(), query, 1)
		results, total := result.Items, result.Total
		if err != nil {
			t.Fatalf("%q: %v", query, err)
		}
		if int(total) != len(results) {
			t.Fatalf("%q: total %d for %d rows", query, total, len(results))
		}
		var names []string
		var fileHits []bool
		for _, r := range results {
			names = append(names, r.Name)
			fileHits = append(fileHits, r.MatchedFiles)
		}
		return names, fileHits
	}

	// Applied observations keep only what idempotency and trending need.
	var observations []struct {
		Applied   bool
		FilesNull bool
		Count     int64
	}
	if err := db.Raw("SELECT applied, files IS NULL AS files_null, count FROM `" + cfg.MySQL.Table + "_observations`").Scan(&observations).Error; err != nil {
		t.Fatal(err)
	}
	if len(observations) != len(items) {
		t.Fatalf("%d observations for %d items", len(observations), len(items))
	}
	for _, o := range observations {
		if !o.Applied || !o.FilesNull || o.Count != 1 {
			t.Fatalf("applied observation kept its file list or lost its count: %+v", o)
		}
	}
	// Replaying an applied batch (an ambiguous commit) must not count twice.
	const replayID = "0123456789abcdef0123456789abcdef"
	replay := []Data{{Name: "replayed", InfoHash: strings.Repeat("e", 40), Time: base, Files: []File{{Path: []string{"r.bin"}, Length: 1}}}}
	for i := 0; i < 2; i++ {
		if err := WriteBatchWithID(replayID, replay); err != nil {
			t.Fatal(err)
		}
	}
	var replayed int64
	db.Raw("SELECT count FROM `"+cfg.MySQL.Table+"` WHERE infohash = ?", strings.Repeat("e", 40)).Scan(&replayed)
	if replayed != 2 { // One from the initial batch, one from this batch, none from its replay.
		t.Fatalf("count after replay = %d, want 2", replayed)
	}
	trending, err := Trending(context.Background(), time.Hour, 10, time.Now().Add(time.Minute))
	if err != nil || len(trending) == 0 {
		t.Fatalf("trending after clearing observation files: %v %v", trending, err)
	}

	// Emulate rows written by an earlier version, before file_names existed.
	if err := db.Exec("UPDATE `" + cfg.MySQL.Table + "` SET file_names = NULL").Error; err != nil {
		t.Fatal(err)
	}
	// Before the schema exists, search still works (LIKE on names).
	if got, _ := search("ubuntu"); len(got) != 1 {
		t.Fatalf("search before indexes: %q", got)
	}

	ctx := context.Background()
	if err := EnsureSearchIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSearchIndexes(ctx); err != nil { // Idempotent.
		t.Fatalf("second run: %v", err)
	}
	for _, index := range []string{nameFulltextIndex, searchFulltextIndex} {
		if !db.Migrator().HasIndex(&Data{}, index) {
			t.Fatalf("index %s missing", index)
		}
	}

	for _, tc := range []struct {
		query    string
		want     []string
		fileHits []bool
	}{
		{"ubuntu", []string{rows[0].name}, []bool{false}},                      // case-insensitive name
		{"vmlinuz", []string{rows[0].name}, []bool{true}},                      // only in a file name
		{"readme", []string{rows[0].name}, []bool{true}},                       // file names are case-insensitive too
		{"ubuntu vmlinuz", []string{rows[0].name}, []bool{true}},               // words split across name and files
		{"第一集", []string{rows[2].name}, []bool{true}},                          // CJK file name
		{"吉兴", []string{rows[2].name}, []bool{false}},                          // CJK infix of the name
		{"intel", []string{rows[1].name}, []bool{false}},                       // starts with the stopword "in"
		{"to be", []string{rows[4].name}, []bool{false}},                       // only stopwords
		{"hamlet", []string{rows[4].name}, []bool{true}},                       // file name
		{"tropo", []string{rows[3].name}, []bool{false}},                       // infix of a Latin word
		{"vmlinuz hamlet", nil, nil},                                           // files of different rows
		{"第 赘婿", []string{rows[2].name}, []bool{true}},                         // "第" is only in file names
		{"release", []string{rows[5].name, rows[3].name}, []bool{false, true}}, // name hit ranks above a newer file hit
	} {
		got, fileHits := search(tc.query)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") || len(fileHits) != len(tc.fileHits) {
			t.Errorf("%q: got %q, want %q", tc.query, got, tc.want)
			continue
		}
		for i := range fileHits {
			if fileHits[i] != tc.fileHits[i] {
				t.Errorf("%q: matched_files %v, want %v", tc.query, fileHits, tc.fileHits)
				break
			}
		}
	}

	// New writes populate the generated column automatically.
	if err := WriteBatch([]Data{{Name: "new arrival", InfoHash: strings.Repeat("0", 40), Time: time.Now(), Files: []File{{Path: []string{"zzqx-unique.bin"}, Length: 1}}}}); err != nil {
		t.Fatal(err)
	}
	if got, _ := search("zzqx-unique"); len(got) != 1 {
		t.Fatalf("file name of a new row not searchable: %q", got)
	}

	// Regression: a new torrent whose infohash sorts before a re-observed one
	// in the same batch. A multi-row upsert loses the new row's document.
	reobserved := items[5] // infohash "fff..."; the new one below sorts first.
	reobserved.Files = nil // As the scheduler emits re-observations.
	if err := WriteBatch([]Data{
		{Name: "brand new qwvx release", InfoHash: strings.Repeat("1", 40), Time: time.Now(), Files: []File{{Path: []string{"n.bin"}, Length: 1}}},
		reobserved,
	}); err != nil {
		t.Fatal(err)
	}
	if got, _ := search("qwvx"); len(got) != 1 {
		t.Fatalf("new row written before a re-observed row is not searchable: %q", got)
	}

	// Indexes carry the current generation tag.
	var comments []string
	db.Raw("SELECT DISTINCT INDEX_COMMENT FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_TYPE = 'FULLTEXT'", cfg.MySQL.Table).Scan(&comments)
	if len(comments) != 1 || comments[0] != searchIndexGeneration {
		t.Fatalf("index generation tags = %q, want %q", comments, searchIndexGeneration)
	}

	// An outdated generation is rebuilt, recovering documents its writers lost.
	// Lose one on purpose with the multi-row statement order that triggers it.
	if err := db.Exec("INSERT INTO `"+cfg.MySQL.Table+"` (name, infohash, time, count) VALUES (?, ?, ?, 1), (?, ?, ?, 1) "+
		"ON DUPLICATE KEY UPDATE count = count + 1", "lost by old writer zkqp", strings.Repeat("2", 40), time.Now(),
		items[5].Name, items[5].InfoHash, time.Now()).Error; err != nil {
		t.Fatal(err)
	}
	if got, _ := search("zkqp"); len(got) != 0 {
		t.Logf("note: this MySQL indexed the multi-row upsert correctly; recovery is still exercised")
	}
	for _, index := range []string{nameFulltextIndex, searchFulltextIndex} {
		columns := "`name`"
		if index == searchFulltextIndex {
			columns = "`name`, `file_names`"
		}
		if err := db.Exec("DROP INDEX `" + index + "` ON `" + cfg.MySQL.Table + "`").Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("CREATE FULLTEXT INDEX `" + index + "` ON `" + cfg.MySQL.Table + "` (" + columns + ") WITH PARSER ngram COMMENT 'dht-search-v1'").Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := EnsureSearchIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	comments = nil
	db.Raw("SELECT DISTINCT INDEX_COMMENT FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_TYPE = 'FULLTEXT'", cfg.MySQL.Table).Scan(&comments)
	if len(comments) != 1 || comments[0] != searchIndexGeneration {
		t.Fatalf("outdated indexes not rebuilt: tags %q", comments)
	}
	if got, _ := search("zkqp"); len(got) != 1 {
		t.Fatalf("rebuild did not recover the lost document: %q", got)
	}

	// Without the indexes, searches step down instead of failing.
	for _, index := range []string{searchFulltextIndex, nameFulltextIndex} {
		if err := db.Exec("DROP INDEX `" + index + "` ON `" + cfg.MySQL.Table + "`").Error; err != nil {
			t.Fatal(err)
		}
		if got, _ := search("Ubuntu 26.04"); len(got) != 1 {
			t.Fatalf("fallback after dropping %s: %q", index, got)
		}
	}
}
