package mysql

import (
	"context"
	"database/sql"
	"fmt"
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
	Time     time.Time `gorm:"column:time;not null" json:"time"`
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
	dbMu   sync.Mutex
	initDB = openDB
)

func getCfg() *config.Config {
	return config.GetConfig()
}

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

	candidate, err := gorm.Open(mysql.New(mysql.Config{Conn: pool}), &gorm.Config{})
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
	db, err := GetDB()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	d.Name = strings.ToValidUTF8(d.Name, "")
	return db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "infohash"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"count": gorm.Expr("`count` + 1")}),
	}).Create(d).Error
}

func Search(name string) ([]Data, int64, error) {
	var results []Data
	var total int64

	db, err := GetDB()
	if err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	escaped := escapeLike(name)
	query := db.WithContext(ctx).Model(&Data{}).Where("name LIKE ? ESCAPE '|'", "%"+escaped+"%")

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if err := query.Order("`time` DESC").Limit(50).Find(&results).Error; err != nil {
		return nil, 0, err
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
