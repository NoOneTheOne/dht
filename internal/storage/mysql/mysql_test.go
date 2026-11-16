package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	mysqlDriver "github.com/go-sql-driver/mysql"
	gormMySQL "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Tests replace initialization, never loading config or contacting MySQL.
func stubInitialization(t *testing.T, init func(context.Context) (*gorm.DB, error)) {
	t.Helper()
	dbMu.Lock()
	previousDB, previousInit, previousRound := db, initDB, dbStarting
	db, initDB, dbStarting = nil, init, nil
	dbMu.Unlock()
	t.Cleanup(func() {
		dbMu.Lock()
		db, initDB, dbStarting = previousDB, previousInit, previousRound
		dbMu.Unlock()
	})
}

func TestWriteBatchSQLUsesProductionUpsert(t *testing.T) {
	cfg := &config.Config{}
	cfg.MySQL.Table = "torrent_test"
	original := getCfg
	getCfg = func() *config.Config { return cfg }
	t.Cleanup(func() { getCfg = original })
	candidate, err := gorm.Open(gormMySQL.New(gormMySQL.Config{SkipInitializeWithVersion: true}), &gorm.Config{DryRun: true, DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	var query string
	candidate.Callback().Create().After("gorm:create").Register("capture_production_upsert", func(tx *gorm.DB) { query = tx.Statement.SQL.String() })
	rows := []Data{{InfoHash: strings.Repeat("a", 40), Count: 2, Files: []File{{Path: []string{"file"}, Length: 7}}}}
	if err := upsertRows(candidate.Session(&gorm.Session{SkipDefaultTransaction: true}), rows); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"COALESCE(`count`, 0) + VALUES(`count`)",
		"`length`=IF((`files` IS NULL OR JSON_LENGTH(`files`) = 0 OR JSON_TYPE(`files`) = 'NULL') AND JSON_LENGTH(VALUES(`files`)) > 0, VALUES(`length`), `length`)",
		"`files`=IF(`files` IS NULL OR JSON_LENGTH(`files`) = 0 OR JSON_TYPE(`files`) = 'NULL', VALUES(`files`), `files`)",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("missing %s in %s", fragment, query)
		}
	}
	if strings.Index(query, "`length`=IF") > strings.Index(query, "`files`=IF") {
		t.Fatal("length must be enriched before files are replaced")
	}
}

func TestRecoveryReplayRetainsFailuresAndDamagedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.jsonl")
	store := NewRecoveryStore(path)
	for _, id := range []string{"first", "second"} {
		if err := store.Append(RecoveryBatch{ID: id, Items: []Data{{Name: id}}}); err != nil {
			t.Fatal(err)
		}
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	file.WriteString(`{"id":"partial`)
	file.Close()
	calls := 0
	if err := store.Replay(func(id string, _ []Data) error {
		calls++
		if id == "second" {
			return errors.New("offline")
		}
		return nil
	}); err == nil {
		t.Fatal("damaged tail not reported")
	}
	if calls != 2 {
		t.Fatal("valid prefix not replayed")
	}
	backups, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".dht-recovery-damaged-*"))
	if len(backups) != 1 {
		t.Fatal("damaged original not preserved")
	}
	if err := store.Append(RecoveryBatch{ID: "third"}); err != nil {
		t.Fatal(err)
	}
	var replayed []string
	if err := store.Replay(func(id string, _ []Data) error { replayed = append(replayed, id); return nil }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(replayed, ",") != "second,third" {
		t.Fatalf("replayed %v", replayed)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("completed spool not removed")
	}
}

func TestGetDBRetriesFailureAndCachesSuccess(t *testing.T) {
	failure := errors.New("temporary initialization failure")
	candidate := &gorm.DB{}
	calls := 0
	stubInitialization(t, func(context.Context) (*gorm.DB, error) {
		calls++
		if calls == 1 {
			// Even a partial handle must not be published on failure.
			return candidate, failure
		}
		return candidate, nil
	})

	if got, err := GetDBContext(context.Background()); got != nil || !errors.Is(err, failure) {
		t.Fatalf("failed initialization = (%p, %v), want nil and failure", got, err)
	}
	if db != nil {
		t.Fatal("failed initialization was published")
	}
	for i := 0; i < 2; i++ {
		if got, err := GetDBContext(context.Background()); got != candidate || err != nil {
			t.Fatalf("successful initialization = (%p, %v), want cached handle", got, err)
		}
	}
	if calls != 2 {
		t.Fatalf("initializations = %d, want 2", calls)
	}
}

func TestGetDBConcurrentRetryInitializesOnce(t *testing.T) {
	candidate := &gorm.DB{}
	failure := errors.New("temporary initialization failure")
	calls := 0
	stubInitialization(t, func(context.Context) (*gorm.DB, error) {
		calls++ // GetDB must serialize all calls to the initializer.
		if calls == 1 {
			return nil, failure
		}
		return candidate, nil
	})
	if _, err := GetDBContext(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("first initialization error = %v", err)
	}

	const workers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if got, err := GetDBContext(context.Background()); got != candidate || err != nil {
				t.Errorf("concurrent GetDB = (%p, %v), want shared handle", got, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls != 2 {
		t.Fatalf("initializations = %d, want one failure and one success", calls)
	}
}

// Done is observed only after GetDBContext has selected the in-flight round.
// This makes concurrent failure tests independent of goroutine scheduling.
type initializationWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *initializationWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestGetDBContextSharesRoundResult(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure_%t", fail), func(t *testing.T) {
			candidate := &gorm.DB{}
			failure := errors.New("initialization failed after opening a handle")
			started, release := make(chan struct{}), make(chan struct{})
			calls := 0
			stubInitialization(t, func(context.Context) (*gorm.DB, error) {
				calls++
				if calls == 1 {
					close(started)
					<-release
					if fail {
						return candidate, failure
					}
				}
				return candidate, nil
			})
			var wg sync.WaitGroup
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer func() { unblock(); wg.Wait() }()
			check := func() {
				defer wg.Done()
				got, err := GetDBContext(context.Background())
				if fail {
					if got != nil || err != failure {
						t.Errorf("failed round = (%p, %v), want nil and shared failure", got, err)
					}
				} else if got != candidate || err != nil {
					t.Errorf("successful round = (%p, %v), want shared handle", got, err)
				}
			}
			wg.Add(1)
			go check()
			<-started
			for i := 0; i < 32; i++ {
				ctx := &initializationWaitContext{Context: context.Background(), waiting: make(chan struct{})}
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, err := GetDBContext(ctx)
					if fail {
						if got != nil || err != failure {
							t.Errorf("waiter = (%p, %v), want nil and shared failure", got, err)
						}
					} else if got != candidate || err != nil {
						t.Errorf("waiter = (%p, %v), want shared handle", got, err)
					}
				}()
				select {
				case <-ctx.waiting:
				case <-time.After(2 * time.Second):
					t.Fatal("waiter blocked acquiring initialization lock")
				}
			}
			unblock()
			wg.Wait()
			if calls != 1 {
				t.Fatalf("initializations = %d, want one for the entire round", calls)
			}
			if fail && db != nil {
				t.Fatal("partial handle was cached")
			}
			if got, err := GetDBContext(context.Background()); got != candidate || err != nil {
				t.Fatalf("next request = (%p, %v), want successful handle", got, err)
			}
			wantCalls := 1
			if fail {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("initializations = %d, want %d", calls, wantCalls)
			}
		})
	}
}

func TestGetDBContextWaitCanCancel(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline_%t", deadline), func(t *testing.T) {
			started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			candidate := &gorm.DB{}
			calls := 0
			stubInitialization(t, func(context.Context) (*gorm.DB, error) {
				calls++
				close(started)
				<-release
				return candidate, nil
			})
			defer func() { close(release); <-finished }()
			go func() {
				defer close(finished)
				if got, err := GetDBContext(context.Background()); got != candidate || err != nil {
					t.Errorf("initializer = (%p, %v)", got, err)
				}
			}()
			<-started
			parent, cancel := context.WithCancel(context.Background())
			wantErr := context.Canceled
			if deadline {
				cancel()
				parent, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
				wantErr = context.DeadlineExceeded
			}
			defer cancel()
			ctx := &initializationWaitContext{Context: parent, waiting: make(chan struct{})}
			result := make(chan error, 1)
			go func() {
				got, err := GetDBContext(ctx)
				if got != nil {
					t.Errorf("canceled waiter received handle %p", got)
				}
				result <- err
			}()
			select {
			case <-ctx.waiting:
			case <-time.After(2 * time.Second):
				t.Fatal("waiter blocked acquiring initialization lock")
			}
			if !deadline {
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, wantErr) {
					t.Fatalf("wait error = %v, want %v", err, wantErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancellation did not interrupt initialization wait")
			}
			select {
			case <-finished:
				t.Fatal("waiter canceled the initializer")
			default:
			}
		})
	}
}

func TestOperationsPropagateInitializationError(t *testing.T) {
	failure := errors.New("backend unavailable")
	stubInitialization(t, func(context.Context) (*gorm.DB, error) { return nil, failure })
	if err := Write(&Data{Name: "test", InfoHash: strings.Repeat("a", 40)}); !errors.Is(err, failure) {
		t.Fatalf("Write error = %v, want initialization failure", err)
	}
	if results, total, err := Search(context.Background(), "test", 1); results != nil || total != 0 || !errors.Is(err, failure) {
		t.Fatalf("Search = (%v, %d, %v), want nil, zero and initialization failure", results, total, err)
	}
	if result, err := Detail(context.Background(), strings.Repeat("a", 40)); result != nil || !errors.Is(err, failure) {
		t.Fatalf("Detail = (%v, %v), want nil and initialization failure", result, err)
	}
}

func TestOperationInitializationHasTimeout(t *testing.T) {
	for _, tt := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"write", func(context.Context) error {
			return WriteBatchWithID("test", []Data{{InfoHash: strings.Repeat("a", 40)}})
		}},
		{"search", func(ctx context.Context) error { _, _, err := Search(ctx, "test", 1); return err }},
		{"trending", func(ctx context.Context) error {
			_, err := Trending(ctx, time.Hour, 10, time.Now())
			return err
		}},
		{"counts", func(ctx context.Context) error { _, err := CountResources(ctx, time.Now()); return err }},
		{"detail", func(ctx context.Context) error { _, err := Detail(ctx, strings.Repeat("a", 40)); return err }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			failure := errors.New("backend unavailable")
			calls := 0
			type contextKey struct{}
			stubInitialization(t, func(ctx context.Context) (*gorm.DB, error) {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > operationTimeout {
					t.Error("initialization has no operation timeout")
				}
				if tt.name != "write" && ctx.Value(contextKey{}) != "request" {
					t.Error("initialization lost request context")
				}
				return &gorm.DB{}, failure
			})
			parent := context.WithValue(context.Background(), contextKey{}, "request")
			if err := tt.run(parent); err != failure || calls != 1 {
				t.Fatalf("error=%v initializations=%d, want shared failure and no internal initialization retry", err, calls)
			}
			if db != nil {
				t.Fatal("operation cached partial handle")
			}
			if tt.name == "write" {
				return // The write API has no parent context.
			}
			ctx, cancel := context.WithCancel(parent)
			cancel()
			if err := tt.run(ctx); !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("canceled operation: error=%v initializations=%d", err, calls)
			}
		})
	}
}

func TestGetDBContextInitializerCancellationDoesNotCacheHandle(t *testing.T) {
	candidate := &gorm.DB{}
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stubInitialization(t, func(got context.Context) (*gorm.DB, error) {
		calls++
		if calls == 1 {
			if got != ctx {
				t.Fatal("initializer did not receive caller context")
			}
			cancel()
			return candidate, got.Err()
		}
		return candidate, nil
	})
	if got, err := GetDBContext(ctx); got != nil || !errors.Is(err, context.Canceled) || db != nil {
		t.Fatalf("canceled initialization = (%p, %v), cached=%p", got, err, db)
	}
	if got, err := GetDBContext(context.Background()); got != candidate || err != nil || calls != 2 {
		t.Fatalf("retry = (%p, %v), calls=%d", got, err, calls)
	}
}

func TestRetryUncommitted(t *testing.T) {
	failure := errors.New("write failed")
	for _, tt := range []struct {
		name      string
		retryable bool
		succeedAt int
		wantCalls int
	}{
		{"success", false, 1, 1},
		{"unknown commit", false, 0, 1},
		{"rollback then success", true, 2, 2},
		{"bounded retries", true, 0, 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := retryUncommitted(context.Background(), func() (bool, error) {
				calls++
				if calls == tt.succeedAt {
					return false, nil
				}
				return tt.retryable, failure
			})
			if calls != tt.wantCalls || (err == nil) != (tt.succeedAt > 0) {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := retryUncommitted(ctx, func() (bool, error) {
		t.Fatal("canceled retry invoked write/initialization")
		return true, failure
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry = %v", err)
	}
}

func TestAggregateWrites(t *testing.T) {
	first := time.Unix(100, 0)
	files := []File{{Path: []string{"folder", "file"}, Length: 7}}
	items := []Data{
		{InfoHash: strings.Repeat("B", 40), Name: "first\xff", Time: first, Count: 999},
		{InfoHash: strings.Repeat("b", 40), Name: "later", Time: first.Add(time.Hour), Length: 7, Files: files},
		{InfoHash: strings.Repeat("a", 40), Name: "other"},
	}
	rows, err := aggregateWrites(items)
	if err != nil || len(rows) != 2 {
		t.Fatalf("aggregate = %v, %v", rows, err)
	}
	if rows[0].InfoHash != strings.Repeat("a", 40) || rows[0].Count != 1 ||
		rows[1].InfoHash != strings.Repeat("b", 40) || rows[1].Count != 2 ||
		rows[1].Name != "first" || rows[1].Time != first || rows[1].Length != 7 {
		t.Fatalf("incorrect grouped counts or first metadata: %+v", rows)
	}
	if len(rows[1].Files) != 1 || rows[1].Files[0].Length != 7 || len(items[0].Files) != 0 {
		t.Fatal("duplicate file-list enrichment lost data or mutated the caller")
	}
	preserved, err := aggregateWrites([]Data{
		{InfoHash: strings.Repeat("a", 40), Length: 7, Files: files},
		{InfoHash: strings.Repeat("a", 40), Length: 8, Files: []File{{Path: []string{"later"}, Length: 8}}},
		{InfoHash: strings.Repeat("a", 40)},
	})
	if err != nil || len(preserved[0].Files) != 1 || preserved[0].Files[0].Length != 7 || preserved[0].Length != 7 || preserved[0].Count != 3 {
		t.Fatal("duplicate overwrote the first file list")
	}
	if items[0].Count != 999 || items[0].Name != "first\xff" {
		t.Fatal("write mutated caller data")
	}
	for _, hash := range []string{"", "abc", strings.Repeat("z", 40), strings.Repeat("a", 42)} {
		if err := WriteBatch([]Data{{InfoHash: hash}}); err == nil {
			t.Fatalf("accepted invalid hash %q", hash)
		}
	}
	longName, err := aggregateWrites([]Data{{InfoHash: strings.Repeat("c", 40), Name: strings.Repeat("中", 513)}})
	if err != nil || len([]rune(longName[0].Name)) != 512 {
		t.Fatal("name was not bounded to varchar(512) by characters")
	}
	if err := WriteBatch(nil); err != nil {
		t.Fatal(err)
	}
}

func TestWriteBatchAtomicCountSQL(t *testing.T) {
	cfg := &config.Config{}
	cfg.MySQL.Table = "torrent_test"
	original := getCfg
	getCfg = func() *config.Config { return cfg }
	t.Cleanup(func() { getCfg = original })
	candidate, err := gorm.Open(gormMySQL.New(gormMySQL.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DryRun: true, DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var query string
	stubInitialization(t, func(context.Context) (*gorm.DB, error) { return candidate, nil })
	hash := strings.Repeat("a", 40)
	rows, err := aggregateWrites([]Data{{InfoHash: hash, Name: "first", Files: []File{{Path: []string{"folder", "file"}, Length: 7}}}, {InfoHash: hash, Name: "second"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Count != 2 || len(rows[0].Files) != 1 || rows[0].Files[0].Length != 7 {
		t.Fatalf("unexpected grouped rows: %+v", rows)
	}
	query = candidate.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "infohash"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"count": gorm.Expr("COALESCE(`count`, 0) + VALUES(`count`)"),
				"files": gorm.Expr("IF(`files` IS NULL OR JSON_LENGTH(`files`) = 0 OR JSON_TYPE(`files`) = 'NULL', VALUES(`files`), `files`)"),
			}),
		}).Create(&rows)
	})
	if query == "" {
		t.Fatal("failed to generate upsert SQL")
	}
	if !strings.Contains(query, "ON DUPLICATE KEY UPDATE `count`=COALESCE(`count`, 0) + VALUES(`count`)") {
		t.Fatalf("count must be incremented atomically by grouped delta: %s", query)
	}
	if strings.Contains(query, "`name`=") || strings.Contains(query, "`time`=") {
		t.Fatalf("duplicate overwrites original metadata: %s", query)
	}
}

func BenchmarkAggregateWrites(b *testing.B) {
	for _, unique := range []int{1, 100} {
		b.Run(fmt.Sprint(unique), func(b *testing.B) {
			items := make([]Data, 100)
			for i := range items {
				items[i] = Data{InfoHash: fmt.Sprintf("%040X", i%unique), Name: "example torrent 中文"}
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := aggregateWrites(items); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestTrendingQuery(t *testing.T) {
	cfg := &config.Config{}
	cfg.MySQL.Table = "torrent_test"
	original := getCfg
	getCfg = func() *config.Config { return cfg }
	t.Cleanup(func() { getCfg = original })
	candidate, err := gorm.Open(gormMySQL.New(gormMySQL.Config{SkipInitializeWithVersion: true}), &gorm.Config{DryRun: true, DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	until := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	calls := 0
	if err := candidate.Callback().Query().After("gorm:query").Register("capture_trending", func(tx *gorm.DB) {
		calls++
		sql := tx.Statement.SQL.String()
		if !strings.Contains(sql, "JOIN (") {
			calls--
			return
		}
		for _, fragment := range []string{"SUM(`count`) AS hits", "JOIN (SELECT", "GROUP BY `infohash`", "applied = ? AND time >= ? AND time < ?", "ranked.hits AS count", "ORDER BY hits DESC, infohash ASC LIMIT ?", "ranked.hits DESC, resources.infohash ASC", "AS has_files"} {
			if !strings.Contains(sql, fragment) {
				t.Fatalf("missing %q in %s", fragment, sql)
			}
		}
		vars := tx.Statement.Vars
		if len(vars) != 4 || vars[0] != true || vars[1] != until.Add(-time.Hour) || vars[2] != until || vars[3] != 10 {
			t.Fatalf("unexpected window vars: %v", vars)
		}
		if _, ok := tx.Statement.Context.Deadline(); !ok {
			t.Fatal("missing timeout")
		}
	}); err != nil {
		t.Fatal(err)
	}
	stubInitialization(t, func(context.Context) (*gorm.DB, error) { return candidate, nil })
	items, err := Trending(context.Background(), time.Hour, 10, until)
	if err != nil || items == nil || calls != 1 {
		t.Fatalf("items=%v calls=%d err=%v", items, calls, err)
	}
	for _, tc := range []struct {
		window time.Duration
		limit  int
	}{{2 * time.Hour, 10}, {time.Hour, 0}, {time.Hour, 101}} {
		if _, err := Trending(context.Background(), tc.window, tc.limit, until); err == nil {
			t.Fatal("invalid parameters accepted")
		}
	}
	statement := &gorm.Statement{DB: candidate}
	if err := statement.Parse(&Observation{}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, index := range statement.Schema.ParseIndexes() {
		if index.Name == "idx_observation_window" && len(index.Fields) == 2 && index.Fields[0].DBName == "applied" && index.Fields[1].DBName == "time" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing observation window index")
	}
}

func TestSearchQueries(t *testing.T) {
	for _, tt := range []struct{ page, rows int }{
		{1, 0}, {1, 1}, {1, 49}, {1, 50},
		{2, 50}, {3, 23}, {4, 0},
	} {
		t.Run(fmt.Sprintf("page_%d_rows_%d", tt.page, tt.rows), func(t *testing.T) {
			n := tt.rows
			cfg := &config.Config{}
			cfg.MySQL.Table = "torrent_test"
			original := getCfg
			getCfg = func() *config.Config { return cfg }
			t.Cleanup(func() { getCfg = original })
			candidate, err := gorm.Open(gormMySQL.New(gormMySQL.Config{SkipInitializeWithVersion: true}), &gorm.Config{
				DryRun: true, DisableAutomaticPing: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			type contextKey struct{}
			parent := context.WithValue(context.Background(), contextKey{}, "request")
			queries := 0
			if err := candidate.Callback().Query().After("gorm:query").Register("capture_search", func(tx *gorm.DB) {
				queries++
				if tx.Statement.Context.Value(contextKey{}) != "request" {
					t.Fatal("request context was lost")
				}
				if _, ok := tx.Statement.Context.Deadline(); !ok {
					t.Fatal("query has no timeout")
				}
				if got := tx.Statement.Vars; len(got) == 0 || got[0] != "%a|%|_||%" {
					t.Fatalf("LIKE input was not escaped: %v", got)
				}
				sql := tx.Statement.SQL.String()
				switch dest := tx.Statement.Dest.(type) {
				case *[]Data:
					if strings.Contains(sql, "SELECT *") || !strings.Contains(sql, "AS has_files") {
						t.Fatalf("search must return availability without the file list: %s", sql)
					}
					if !strings.Contains(sql, "ORDER BY `time` DESC, `infohash` DESC LIMIT ?") {
						t.Fatalf("missing bounded deterministic sort: %s", sql)
					}
					vars := tx.Statement.Vars
					if len(vars) < 2 || vars[1] != SearchPageSize {
						t.Fatalf("incorrect page limit: %v", vars)
					}
					if tt.page > 1 {
						if !strings.Contains(sql, "OFFSET ?") || len(vars) != 3 || vars[2] != (tt.page-1)*SearchPageSize {
							t.Fatalf("incorrect page offset: %s %v", sql, vars)
						}
					} else if strings.Contains(sql, "OFFSET") {
						t.Fatalf("first page has offset: %s", sql)
					}
					*dest = make([]Data, n)
				case *int64:
					if strings.Contains(sql, "LIMIT") || strings.Contains(sql, "OFFSET") || strings.Contains(sql, "ORDER BY") {
						t.Fatalf("count inherited page clauses: %s", sql)
					}
					*dest = 123
					tx.RowsAffected = 1
				default:
					t.Fatalf("unexpected query destination %T", dest)
				}
			}); err != nil {
				t.Fatal(err)
			}
			stubInitialization(t, func(context.Context) (*gorm.DB, error) { return candidate, nil })
			rows, total, err := Search(parent, "a%_|", tt.page)
			wantTotal, wantQueries := int64(n), 1
			if tt.page > 1 || n == SearchPageSize {
				wantTotal, wantQueries = 123, 2
			}
			if err != nil || len(rows) != n || total != wantTotal || queries != wantQueries {
				t.Fatalf("rows=%d total=%d queries=%d err=%v", len(rows), total, queries, err)
			}
			statement := &gorm.Statement{DB: candidate}
			if err := statement.Parse(&Data{}); err != nil {
				t.Fatal(err)
			}
			hasFiles := statement.Schema.LookUpField("has_files")
			if hasFiles == nil || !hasFiles.IgnoreMigration || hasFiles.Creatable || hasFiles.Updatable || !hasFiles.Readable {
				t.Fatal("availability must be a read-only projection, not a physical column")
			}
			files := statement.Schema.LookUpField("files")
			if files == nil || files.Serializer == nil || string(files.DataType) != "json" {
				t.Fatal("file lists require a JSON serializer and JSON column")
			}
			indexes := statement.Schema.ParseIndexes()
			if len(indexes) != 1 || len(indexes[0].Fields) != 1 || indexes[0].Fields[0].DBName != "time" {
				t.Fatalf("missing time index: %+v", indexes)
			}
		})
	}
}

func TestSearchCanceledRequest(t *testing.T) {
	cfg := &config.Config{}
	cfg.MySQL.Table = "torrent_test"
	original := getCfg
	getCfg = func() *config.Config { return cfg }
	t.Cleanup(func() { getCfg = original })
	candidate, err := gorm.Open(gormMySQL.New(gormMySQL.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DryRun: true, DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := candidate.Callback().Query().Before("gorm:query").Register("canceled_request", func(tx *gorm.DB) {
		calls++
		tx.AddError(tx.Statement.Context.Err())
	}); err != nil {
		t.Fatal(err)
	}
	initializations := 0
	stubInitialization(t, func(context.Context) (*gorm.DB, error) {
		initializations++
		return candidate, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows, total, err := Search(ctx, "test", 1)
	if !errors.Is(err, context.Canceled) || rows != nil || total != 0 || calls != 0 || initializations != 0 {
		t.Fatalf("canceled search = %v, %d, %v; queries=%d initializations=%d", rows, total, err, calls, initializations)
	}
}

func TestSearchItemJSON(t *testing.T) {
	for _, available := range []bool{false, true} {
		encoded, err := json.Marshal(Data{Name: "resource", HasFiles: available})
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		if _, exists := fields["files"]; exists || string(fields["has_files"]) != fmt.Sprint(available) {
			t.Fatalf("search JSON must expose availability without a file payload: %s", encoded)
		}
	}
}

func TestCountResources(t *testing.T) {
	cfg := &config.Config{}
	cfg.MySQL.Table = "torrent_test"
	original := getCfg
	getCfg = func() *config.Config { return cfg }
	t.Cleanup(func() { getCfg = original })
	candidate, err := gorm.Open(gormMySQL.New(gormMySQL.Config{SkipInitializeWithVersion: true}), &gorm.Config{DryRun: true, DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	queries := 0
	if err := candidate.Callback().Query().After("gorm:query").Register("capture_coverage", func(tx *gorm.DB) {
		if err := tx.Statement.Context.Err(); err != nil {
			tx.AddError(err)
			return
		}
		queries++
		sql := tx.Statement.SQL.String()
		for _, fragment := range []string{"COUNT(*) AS total", "JSON_TYPE(`files`) = 'ARRAY' AND JSON_LENGTH(`files`) > 0", "AS with_files", "AS this_run_total", "AS this_run_with_files", "FROM `torrent_test`"} {
			if !strings.Contains(sql, fragment) {
				t.Fatalf("missing %s in %s", fragment, sql)
			}
		}
		if len(tx.Statement.Vars) != 2 || tx.Statement.Vars[0] != startedAt || tx.Statement.Vars[1] != startedAt {
			t.Fatalf("run start not bound: %v", tx.Statement.Vars)
		}
		if _, ok := tx.Statement.Context.Deadline(); !ok {
			t.Fatal("coverage query has no timeout")
		}
		*tx.Statement.Dest.(*ResourceCounts) = ResourceCounts{Total: 12, WithFiles: 9, ThisRunTotal: 4, ThisRunWithFiles: 1}
	}); err != nil {
		t.Fatal(err)
	}
	stubInitialization(t, func(context.Context) (*gorm.DB, error) { return candidate, nil })
	counts, err := CountResources(context.Background(), startedAt)
	if err != nil || queries != 1 || counts.Total != 12 || counts.WithFiles != 9 || counts.ThisRunTotal != 4 || counts.ThisRunWithFiles != 1 {
		t.Fatalf("incorrect coverage: %+v, queries=%d, error=%v", counts, queries, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CountResources(ctx, startedAt); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled coverage query returned %v", err)
	}
}

func TestDetailQuery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		files    []File
		queryErr error
		wantErr  error
	}{
		{"files", []File{{Path: []string{"folder", "file"}, Length: 7}}, nil, nil},
		{"missing", nil, gorm.ErrRecordNotFound, gorm.ErrRecordNotFound},
		{"empty", nil, nil, gorm.ErrRecordNotFound},
		{"canceled", nil, context.Canceled, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.MySQL.Table = "torrent_test"
			original := getCfg
			getCfg = func() *config.Config { return cfg }
			t.Cleanup(func() { getCfg = original })
			candidate, err := gorm.Open(gormMySQL.New(gormMySQL.Config{SkipInitializeWithVersion: true}), &gorm.Config{DryRun: true, DisableAutomaticPing: true})
			if err != nil {
				t.Fatal(err)
			}
			type contextKey struct{}
			parent := context.WithValue(context.Background(), contextKey{}, "request")
			hash := strings.Repeat("a", 40)
			if err := candidate.Callback().Query().After("gorm:query").Register("capture_detail", func(tx *gorm.DB) {
				sql := tx.Statement.SQL.String()
				if !strings.Contains(sql, "infohash = ? AND JSON_TYPE(`files`) = 'ARRAY' AND JSON_LENGTH(`files`) > 0") || !strings.Contains(sql, "LIMIT ?") || tx.Statement.Vars[0] != hash {
					t.Fatalf("unbounded or unfiltered detail lookup: %s", sql)
				}
				if _, ok := tx.Statement.Context.Deadline(); !ok || tx.Statement.Context.Value(contextKey{}) != "request" {
					t.Fatal("request context or deadline lost")
				}
				if tc.queryErr != nil {
					tx.AddError(tc.queryErr)
					return
				}
				result := tx.Statement.Dest.(*Data)
				result.InfoHash, result.Files = hash, tc.files
			}); err != nil {
				t.Fatal(err)
			}
			stubInitialization(t, func(context.Context) (*gorm.DB, error) { return candidate, nil })
			result, err := Detail(parent, hash)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("detail error=%v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && (result == nil || !result.HasFiles || len(result.Files) != 1) {
				t.Fatalf("invalid detail: %+v", result)
			}
			if tc.wantErr != nil && result != nil {
				t.Fatal("returned detail on error")
			}
		})
	}
}

type versionConnector struct {
	version string
	query   func(context.Context, string) error
}

func (c versionConnector) Connect(context.Context) (driver.Conn, error) {
	return versionConnection{connector: c}, nil
}

func (c versionConnector) Driver() driver.Driver { return nil } // sql.OpenDB uses Connect.

type versionConnection struct {
	connector versionConnector
}

func (c versionConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c versionConnection) Close() error              { return nil }
func (c versionConnection) Begin() (driver.Tx, error) { return nil, errors.New("unexpected Begin") }
func (c versionConnection) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := c.connector.query(ctx, query); err != nil {
		return nil, err
	}
	return &versionRows{version: c.connector.version}, nil
}

type versionRows struct {
	version string
	read    bool
}

func (r *versionRows) Columns() []string { return []string{"VERSION()"} }
func (r *versionRows) Close() error      { return nil }
func (r *versionRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	dest[0] = r.version
	return nil
}

func TestInitializationVersionQueryContextAndCompatibility(t *testing.T) {
	for _, version := range []string{"5.7.44", "8.0.36", "10.5.23-MariaDB", "8.0.11-TiDB-v7.5.0"} {
		t.Run(version, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			queries := 0
			pool := sql.OpenDB(versionConnector{version: version, query: func(got context.Context, query string) error {
				queries++
				if got != ctx || query != "SELECT VERSION()" {
					t.Errorf("version query context=%v query=%q", got, query)
				}
				return nil
			}})
			defer pool.Close()
			dialector := gormMySQL.New(gormMySQL.Config{Conn: initializationPool{ConnPool: pool, ctx: ctx}})
			candidate, err := gorm.Open(dialector, &gorm.Config{DisableAutomaticPing: true})
			if err != nil {
				t.Fatal(err)
			}
			settings := candidate.Dialector.(*gormMySQL.Dialector)
			if queries != 1 || settings.ServerVersion != version {
				t.Fatalf("queries=%d version=%q", queries, settings.ServerVersion)
			}
			if strings.HasPrefix(version, "5.7.") && (!settings.DontSupportRenameColumn || !settings.DontSupportDropConstraint) {
				t.Fatal("MySQL 5.7 migration compatibility flags lost")
			}
			if strings.Contains(version, "MariaDB") && (!settings.DontSupportRenameIndex || !settings.DontSupportRenameColumn || !settings.DontSupportNullAsDefaultValue) {
				t.Fatal("MariaDB migration compatibility flags lost")
			}
			if strings.Contains(version, "TiDB") && !settings.DontSupportRenameColumnUnique {
				t.Fatal("TiDB migration compatibility flag lost")
			}
		})
	}
}

func TestInitializationVersionQueryCanCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := sql.OpenDB(versionConnector{query: func(got context.Context, _ string) error {
		if got != ctx {
			return errors.New("version query lost initialization context")
		}
		cancel()
		<-got.Done()
		return got.Err()
	}})
	defer pool.Close()
	_, err := gorm.Open(gormMySQL.New(gormMySQL.Config{Conn: initializationPool{ConnPool: pool, ctx: ctx}}), &gorm.Config{DisableAutomaticPing: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("version initialization error = %v, want canceled", err)
	}
}

func TestMySQLDSN(t *testing.T) {
	cfg := &config.Config{}
	cfg.MySQL.Username = "test-user"
	cfg.MySQL.Password = "synthetic:@/?&password"
	cfg.MySQL.Addresses = "[::1]:3306"
	cfg.MySQL.Database = "test database/with?chars"

	parsed, err := mysqlDriver.ParseDSN(mysqlDSN(cfg))
	if err != nil {
		t.Fatal("generated DSN could not be parsed")
	}
	if parsed.User != cfg.MySQL.Username || parsed.Passwd != cfg.MySQL.Password ||
		parsed.Addr != cfg.MySQL.Addresses || parsed.DBName != cfg.MySQL.Database || parsed.Net != "tcp" {
		t.Fatal("DSN did not preserve connection settings")
	}
	if parsed.Timeout != 2*time.Second || parsed.ReadTimeout != 5*time.Second || parsed.WriteTimeout != 5*time.Second {
		t.Fatal("DSN does not have bounded connect/read/write timeouts")
	}
	if !parsed.ParseTime || parsed.Loc != time.Local || parsed.Params["charset"] != "utf8mb4" {
		t.Fatal("DSN did not preserve charset and time settings")
	}
}
