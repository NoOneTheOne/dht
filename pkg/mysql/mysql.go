package mysql

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/NoOneTheOne/dht/pkg/config"
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

var (
	db   *gorm.DB
	once sync.Once
)

func getCfg() *config.Config {
	return config.GetConfig()
}

func GetDB() *gorm.DB {
	once.Do(func() {
		c := getCfg()
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			c.MySQL.Username, c.MySQL.Password, c.MySQL.Addresses, c.MySQL.Database)
		var err error
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			log.Fatalf("failed to connect MySQL at %s: %v", c.MySQL.Addresses, err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			log.Fatalf("failed to get underlying sql.DB: %v", err)
		}
		sqlDB.SetMaxIdleConns(10)
		sqlDB.SetMaxOpenConns(100)
		sqlDB.SetConnMaxLifetime(time.Hour)

		if err := db.AutoMigrate(&Data{}); err != nil {
			log.Fatalf("failed to auto-migrate table: %v", err)
		}
	})
	return db
}

func Write(d *Data) error {
	d.Name = strings.ToValidUTF8(d.Name, "")
	return GetDB().Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "infohash"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"count": gorm.Expr("`count` + 1")}),
	}).Create(d).Error
}

func Search(name string) ([]Data, int64, error) {
	var results []Data
	var total int64

	// Escape LIKE special characters in user input
	escaped := escapeLike(name)
	query := GetDB().Model(&Data{}).Where("name LIKE ? ESCAPE '\\'", "%"+escaped+"%")

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if err := query.Order("`time` DESC").Limit(50).Find(&results).Error; err != nil {
		return nil, 0, err
	}
	return results, total, nil
}

// escapeLike escapes special LIKE pattern characters (%, _, \)
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}
