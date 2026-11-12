package mysql

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	mysqlDriver "github.com/go-sql-driver/mysql"
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
	if err := Write(&Data{Name: "test"}); !errors.Is(err, failure) {
		t.Fatalf("Write error = %v, want initialization failure", err)
	}
	if results, total, err := Search("test"); results != nil || total != 0 || !errors.Is(err, failure) {
		t.Fatalf("Search = (%v, %d, %v), want nil, zero and initialization failure", results, total, err)
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
