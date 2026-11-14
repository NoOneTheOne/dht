package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Data struct {
	Name     string    `gorm:"column:name;type:varchar(512);not null" json:"name"`
	InfoHash string    `gorm:"column:infohash;type:varchar(64);primaryKey" json:"infohash"`
	Time     time.Time `gorm:"column:time;not null;index" json:"time"`
	Length   int64     `gorm:"column:length;default:0" json:"length"`
	Count    int64     `gorm:"column:count;default:1" json:"count"`
}

func (Data) TableName() string {
	return getCfg().MySQL.Table
}

const (
	operationTimeout  = 10 * time.Second
	connectionTimeout = 2 * time.Second
	ioTimeout         = 5 * time.Second
)

var (
	db     *gorm.DB
	dbMu   sync.RWMutex
	initDB = openDB
)

var getCfg = config.GetConfig

func mysqlDSN(c *config.Config) string {
	dsn := mysqlDriver.NewConfig()
	dsn.User = c.MySQL.Username
	dsn.Passwd = c.MySQL.Password
	dsn.Net = "tcp"
	dsn.Addr = c.MySQL.Addresses
	dsn.DBName = c.MySQL.Database
	dsn.Params = map[string]string{"charset": "utf8mb4"}
	dsn.ParseTime = true
	dsn.Loc = time.Local
	dsn.Timeout = connectionTimeout
	dsn.ReadTimeout = ioTimeout
	dsn.WriteTimeout = ioTimeout
	return dsn.FormatDSN()
}

func GetDB() (*gorm.DB, error) {
	dbMu.RLock()
	cached := db
	dbMu.RUnlock()
	if cached != nil {
		return cached, nil
	}
	dbMu.Lock()
	defer dbMu.Unlock()
	if db != nil {
		return db, nil
	}

	candidate, err := initDB()
	if err != nil {
		return nil, err
	}
	db = candidate
	return db, nil
}

func openDB() (*gorm.DB, error) {
	pool, err := sql.Open("mysql", mysqlDSN(getCfg()))
	if err != nil {
		return nil, fmt.Errorf("open MySQL pool: %w", err)
	}
	initialized := false
	defer func() {
		if !initialized {
			_ = pool.Close()
		}
	}()
	pool.SetMaxIdleConns(10)
	pool.SetMaxOpenConns(100)
	pool.SetConnMaxLifetime(time.Hour)

	candidate, err := gorm.Open(mysql.New(mysql.Config{Conn: pool}), &gorm.Config{Logger: newGORMLogger()})
	if err != nil {
		return nil, fmt.Errorf("connect MySQL: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	if err := candidate.WithContext(ctx).AutoMigrate(&Data{}); err != nil {
		return nil, fmt.Errorf("migrate MySQL: %w", err)
	}
	initialized = true
	return candidate, nil
}

func Write(d *Data) error {
	return WriteBatch([]Data{*d})
}

func aggregateWrites(items []Data) ([]Data, error) {
	grouped := make(map[string]Data, len(items))
	for _, item := range items {
		var hash [20]byte
		if len(item.InfoHash) != 40 {
			return nil, fmt.Errorf("infohash must be 40 hexadecimal characters")
		}
		if _, err := hex.Decode(hash[:], []byte(item.InfoHash)); err != nil {
			return nil, fmt.Errorf("infohash must be 40 hexadecimal characters")
		}
		item.InfoHash = strings.ToLower(item.InfoHash)
		if existing, ok := grouped[item.InfoHash]; ok {
			existing.Count++
			grouped[item.InfoHash] = existing
			continue
		}
		item.Name = strings.ToValidUTF8(item.Name, "")
		characters := 0
		for offset := range item.Name {
			if characters == 512 {
				item.Name = item.Name[:offset]
				break
			}
			characters++
		}
		item.Count = 1
		grouped[item.InfoHash] = item
	}
	rows := make([]Data, 0, len(grouped))
	for _, item := range grouped {
		rows = append(rows, item)
	}
	// Consistent primary-key lock order reduces deadlocks between concurrent batches.
	sort.Slice(rows, func(i, j int) bool { return rows[i].InfoHash < rows[j].InfoHash })
	return rows, nil
}

func retryUncommitted(ctx context.Context, write func() (bool, error)) error {
	for attempt := 0; ; attempt++ {
		retryable, err := write()
		if err == nil || !retryable || attempt == 2 {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func WriteBatch(items []Data) error {
	if len(items) == 0 {
		return nil
	}
	rows, err := aggregateWrites(items)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	return retryUncommitted(ctx, func() (bool, error) {
		db, err := GetDB()
		if err != nil {
			return true, err
		}
		err = db.WithContext(ctx).Session(&gorm.Session{SkipDefaultTransaction: true}).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "infohash"}},
			DoUpdates: clause.Assignments(map[string]interface{}{"count": gorm.Expr("COALESCE(`count`, 0) + VALUES(`count`)")}),
		}).Create(&rows).Error
		var mysqlErr *mysqlDriver.MySQLError
		return errors.As(err, &mysqlErr) && mysqlErr.Number == 1213, err
	})
}

const SearchPageSize = 50

func Search(parent context.Context, name string, page int) ([]Data, int64, error) {
	if page < 1 || page > int(^uint(0)>>1)/SearchPageSize {
		return nil, 0, fmt.Errorf("invalid search page")
	}
	results := make([]Data, 0, SearchPageSize)

	db, err := GetDB()
	if err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	escaped := escapeLike(name)
	query := db.WithContext(ctx).Model(&Data{}).Where("name LIKE ? ESCAPE '|'", "%"+escaped+"%")

	if err := query.Session(&gorm.Session{}).Order("`time` DESC, `infohash` DESC").Limit(SearchPageSize).Offset((page - 1) * SearchPageSize).Find(&results).Error; err != nil {
		return nil, 0, err
	}
	total := int64(len(results))
	if page > 1 || len(results) == SearchPageSize {
		if err := query.Count(&total).Error; err != nil {
			return nil, 0, err
		}
	}
	return results, total, nil
}

// escapeLike escapes special LIKE pattern characters (%, _, |)
// using '|' as the ESCAPE character to avoid backslash parsing issues.
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `|`, `||`)
	s = strings.ReplaceAll(s, `%`, `|%`)
	s = strings.ReplaceAll(s, `_`, `|_`)
	return s
}
