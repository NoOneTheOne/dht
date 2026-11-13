package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	mysqlDriver "github.com/go-sql-driver/mysql"
	gormMySQL "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Tests replace initialization, never loading config or contacting MySQL.
func stubInitialization(t *testing.T, init func() (*gorm.DB, error)) {
	t.Helper()
	previousDB, previousInit := db, initDB
	db, initDB = nil, init
	t.Cleanup(func() {
		db, initDB = previousDB, previousInit
	})
}

func TestGetDBRetriesFailureAndCachesSuccess(t *testing.T) {
	failure := errors.New("temporary initialization failure")
	candidate := &gorm.DB{}
	calls := 0
	stubInitialization(t, func() (*gorm.DB, error) {
		calls++
		if calls == 1 {
			// Even a partial handle must not be published on failure.
			return candidate, failure
		}
		return candidate, nil
	})

	if got, err := GetDB(); got != nil || !errors.Is(err, failure) {
		t.Fatalf("failed initialization = (%p, %v), want nil and failure", got, err)
	}
	if db != nil {
		t.Fatal("failed initialization was published")
	}
	for i := 0; i < 2; i++ {
		if got, err := GetDB(); got != candidate || err != nil {
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
	stubInitialization(t, func() (*gorm.DB, error) {
		calls++ // GetDB must serialize all calls to the initializer.
		if calls == 1 {
			return nil, failure
		}
		return candidate, nil
	})
	if _, err := GetDB(); !errors.Is(err, failure) {
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
			if got, err := GetDB(); got != candidate || err != nil {
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

func TestOperationsPropagateInitializationError(t *testing.T) {
	failure := errors.New("backend unavailable")
	stubInitialization(t, func() (*gorm.DB, error) { return nil, failure })
	if err := Write(&Data{Name: "test", InfoHash: strings.Repeat("a", 40)}); !errors.Is(err, failure) {
		t.Fatalf("Write error = %v, want initialization failure", err)
	}
	if results, total, err := Search(context.Background(), "test"); results != nil || total != 0 || !errors.Is(err, failure) {
		t.Fatalf("Search = (%v, %d, %v), want nil, zero and initialization failure", results, total, err)
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
	if err := retryUncommitted(ctx, func() (bool, error) { return true, failure }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry = %v", err)
	}
}

func TestAggregateWrites(t *testing.T) {
	first := time.Unix(100, 0)
	items := []Data{
		{InfoHash: strings.Repeat("B", 40), Name: "first\xff", Time: first, Length: 7, Count: 999},
		{InfoHash: strings.Repeat("b", 40), Name: "later", Time: first.Add(time.Hour), Length: 8},
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
	var values []interface{}
	if err := candidate.Callback().Create().After("gorm:create").Register("capture", func(tx *gorm.DB) {
		query = tx.Statement.SQL.String()
		values = append(values, tx.Statement.Vars...)
	}); err != nil {
		t.Fatal(err)
	}
	stubInitialization(t, func() (*gorm.DB, error) { return candidate, nil })
	hash := strings.Repeat("a", 40)
	if err := WriteBatch([]Data{{InfoHash: hash, Name: "first"}, {InfoHash: hash, Name: "second"}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "ON DUPLICATE KEY UPDATE `count`=COALESCE(`count`, 0) + VALUES(`count`)") {
		t.Fatalf("count must be incremented atomically by grouped delta: %s", query)
	}
	if len(values) != 5 || values[4] != int64(2) {
		t.Fatalf("expected one row with delta 2, got %v", values)
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

func TestSearchQueries(t *testing.T) {
	for _, n := range []int{0, 1, 49, 50} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
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
					if !strings.Contains(sql, "ORDER BY `time` DESC, `infohash` DESC LIMIT ?") {
						t.Fatalf("missing bounded deterministic sort: %s", sql)
					}
					*dest = make([]Data, n)
				case *int64:
					if strings.Contains(sql, "LIMIT") || strings.Contains(sql, "ORDER BY") {
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
			stubInitialization(t, func() (*gorm.DB, error) { return candidate, nil })
			rows, total, err := Search(parent, "a%_|")
			wantTotal, wantQueries := int64(n), 1
			if n == 50 {
				wantTotal, wantQueries = 123, 2
			}
			if err != nil || len(rows) != n || total != wantTotal || queries != wantQueries {
				t.Fatalf("rows=%d total=%d queries=%d err=%v", len(rows), total, queries, err)
			}
			statement := &gorm.Statement{DB: candidate}
			if err := statement.Parse(&Data{}); err != nil {
				t.Fatal(err)
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
	stubInitialization(t, func() (*gorm.DB, error) { return candidate, nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows, total, err := Search(ctx, "test")
	if !errors.Is(err, context.Canceled) || rows != nil || total != 0 || calls != 1 {
		t.Fatalf("canceled search = %v, %d, %v; queries=%d", rows, total, err, calls)
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
