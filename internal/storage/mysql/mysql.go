package mysql

import (
	"context"
	"crypto/rand"
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

type Observation struct {
	BatchID  string    `gorm:"column:batch_id;type:char(32);primaryKey"`
	InfoHash string    `gorm:"column:infohash;type:varchar(64);primaryKey"`
	Name     string    `gorm:"column:name;type:varchar(512);not null"`
	Time     time.Time `gorm:"column:time;not null;index:idx_observation_window,priority:2"`
	Length   int64     `gorm:"column:length;default:0"`
	Count    int64     `gorm:"column:count;default:1"`
	Files    []File    `gorm:"column:files;type:json;serializer:json"`
	Applied  bool      `gorm:"column:applied;default:false;index;index:idx_observation_window,priority:1"`
}

func (Observation) TableName() string {
	return getCfg().MySQL.Table + "_observations"
}

type File struct {
	Path   []string `json:"path"`
	Length int64    `json:"length"`
}

type Data struct {
	Name     string    `gorm:"column:name;type:varchar(512);not null" json:"name"`
	InfoHash string    `gorm:"column:infohash;type:varchar(64);primaryKey" json:"infohash"`
	Time     time.Time `gorm:"column:time;not null;index" json:"time"`
	Length   int64     `gorm:"column:length;default:0" json:"length"`
	Count    int64     `gorm:"column:count;default:1" json:"count"`
	Files    []File    `gorm:"column:files;type:json;serializer:json" json:"files,omitempty"`
	HasFiles bool      `gorm:"column:has_files;->;-:migration" json:"has_files"`
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
	db         *gorm.DB
	dbMu       sync.Mutex
	dbStarting *dbInitialization
	initDB     = openDB
)

// Each waiter retains its round's result, even if a later request starts a retry.
type dbInitialization struct {
	done chan struct{}
	db   *gorm.DB
	err  error
}

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
	// UTC in every process: trending windows and per-run counts compare
	// DATETIMEs written by the spider with times computed by the search API.
	dsn.Loc = time.UTC
	dsn.Timeout = connectionTimeout
	dsn.ReadTimeout = ioTimeout
	dsn.WriteTimeout = ioTimeout
	return dsn.FormatDSN()
}

// Waiting callers can cancel independently of the caller performing initialization.
func GetDBContext(ctx context.Context) (*gorm.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dbMu.Lock()
	if err := ctx.Err(); err != nil {
		dbMu.Unlock()
		return nil, err
	}
	if db != nil {
		cached := db
		dbMu.Unlock()
		return cached, nil
	}
	if round := dbStarting; round != nil {
		dbMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-round.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return round.db, round.err
		}
	}
	round := &dbInitialization{done: make(chan struct{})}
	dbStarting = round
	dbMu.Unlock()

	candidate, err := initDB(ctx)
	dbMu.Lock()
	if err == nil {
		db = candidate
		round.db = candidate
	}
	round.err = err
	dbStarting = nil
	close(round.done)
	dbMu.Unlock()
	return round.db, round.err
}

// Bound the dialector's background-context version query without retaining the initialization context.
type initializationPool struct {
	gorm.ConnPool
	ctx context.Context
}

func (p initializationPool) QueryRowContext(_ context.Context, query string, args ...interface{}) *sql.Row {
	return p.ConnPool.QueryRowContext(p.ctx, query, args...)
}

func openDB(parent context.Context) (*gorm.DB, error) {
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

	if err := pool.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping MySQL: %w", err)
	}
	dialector := mysql.New(mysql.Config{Conn: initializationPool{ConnPool: pool, ctx: ctx}})
	candidate, err := gorm.Open(dialector, &gorm.Config{Logger: newGORMLogger(), DisableAutomaticPing: true})
	if err != nil {
		return nil, fmt.Errorf("connect MySQL: %w", err)
	}
	// All subsequent queries use their own context, including migrations below.
	dialector.(*mysql.Dialector).Conn = pool
	candidate.ConnPool = pool
	candidate.Statement.ConnPool = pool
	if err := candidate.WithContext(ctx).AutoMigrate(&Data{}, &Observation{}); err != nil {
		return nil, fmt.Errorf("migrate MySQL: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	initialized = true
	return candidate, nil
}

func Write(d *Data) error {
	return WriteBatch([]Data{*d})
}

// ErrInvalidItem marks items rejected before reaching MySQL.
var ErrInvalidItem = errors.New("invalid metadata item")

// permanentErrorCodes are server rejections caused by the data itself, which
// replaying the same batch can never fix.
var permanentErrorCodes = map[uint16]bool{
	1048: true, // column cannot be null
	1153: true, // packet larger than max_allowed_packet
	1264: true, // out of range value
	1292: true, // incorrect datetime value
	1366: true, // incorrect string value
	1406: true, // data too long
	3140: true, // invalid JSON text
}

// IsPermanent reports whether err rejects the batch's data rather than
// signalling an unavailable or overloaded database.
func IsPermanent(err error) bool {
	if errors.Is(err, ErrInvalidItem) || errors.Is(err, mysqlDriver.ErrPktTooLarge) {
		return true
	}
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && permanentErrorCodes[mysqlErr.Number]
}

func aggregateWrites(items []Data) ([]Data, error) {
	grouped := make(map[string]Data, len(items))
	for _, item := range items {
		var hash [20]byte
		if len(item.InfoHash) != 40 {
			return nil, fmt.Errorf("%w: infohash must be 40 hexadecimal characters", ErrInvalidItem)
		}
		if _, err := hex.Decode(hash[:], []byte(item.InfoHash)); err != nil {
			return nil, fmt.Errorf("%w: infohash must be 40 hexadecimal characters", ErrInvalidItem)
		}
		item.InfoHash = strings.ToLower(item.InfoHash)
		if existing, ok := grouped[item.InfoHash]; ok {
			existing.Count++
			if len(existing.Files) == 0 && len(item.Files) > 0 {
				existing.Files = item.Files
				existing.Length = item.Length
			}
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
		if err := ctx.Err(); err != nil {
			return err
		}
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

func NewBatchID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func WriteBatch(items []Data) error {
	batchID, err := NewBatchID()
	if err != nil {
		return err
	}
	return WriteBatchWithID(batchID, items)
}

func upsertRows(tx *gorm.DB, values []Data) error {
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "infohash"}},
		// MySQL evaluates assignments left to right; inspect the old files before replacing them.
		DoUpdates: clause.Set{
			{Column: clause.Column{Name: "count"}, Value: gorm.Expr("COALESCE(`count`, 0) + VALUES(`count`)")},
			{Column: clause.Column{Name: "length"}, Value: gorm.Expr("IF((`files` IS NULL OR JSON_LENGTH(`files`) = 0 OR JSON_TYPE(`files`) = 'NULL') AND JSON_LENGTH(VALUES(`files`)) > 0, VALUES(`length`), `length`)")},
			{Column: clause.Column{Name: "files"}, Value: gorm.Expr("IF(`files` IS NULL OR JSON_LENGTH(`files`) = 0 OR JSON_TYPE(`files`) = 'NULL', VALUES(`files`), `files`)")},
		},
	}).Create(&values).Error
}

func WriteBatchWithID(batchID string, items []Data) error {
	if len(items) == 0 {
		return nil
	}
	rows, err := aggregateWrites(items)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	db, err := GetDBContext(ctx)
	if err != nil {
		return err
	}
	return retryUncommitted(ctx, func() (bool, error) {
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			observations := make([]Observation, len(rows))
			for i, row := range rows {
				observations[i] = Observation{BatchID: batchID, InfoHash: row.InfoHash, Name: row.Name, Time: row.Time, Length: row.Length, Count: row.Count, Files: row.Files}
			}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&observations).Error; err != nil {
				return err
			}
			var pending []Observation
			if err := tx.Where("batch_id = ? AND applied = ?", batchID, false).Order("infohash ASC").Find(&pending).Error; err != nil {
				return err
			}
			pendingRows := make([]Data, len(pending))
			for i, observation := range pending {
				pendingRows[i] = Data{Name: observation.Name, InfoHash: observation.InfoHash, Time: observation.Time, Length: observation.Length, Count: observation.Count, Files: observation.Files}
			}
			if len(pendingRows) == 0 {
				return nil
			}
			if err := upsertRows(tx, pendingRows); err != nil {
				return err
			}
			return tx.Model(&Observation{}).Where("batch_id = ? AND applied = ?", batchID, false).Update("applied", true).Error
		})
		var mysqlErr *mysqlDriver.MySQLError
		return errors.As(err, &mysqlErr) && (mysqlErr.Number == 1213 || mysqlErr.Number == 1205), err
	})
}

// ObservationRetention keeps every row the longest trending window (7d) can
// read, plus a day of margin for recovery batches replayed late.
const ObservationRetention = 8 * 24 * time.Hour

const pruneChunkSize = 5000

// PruneObservations deletes applied observations older than before in small
// chunks, so the cleanup never holds long locks against concurrent writers.
// Unapplied rows are kept because WriteBatchWithID still needs them.
//
// Callers must not prune while the recovery spool holds batches: a spooled
// batch may already be applied (ambiguous commit), and its observation rows
// are what make the replay a no-op. Fresh batches are never old enough to be
// pruned, so an empty spool makes pruning by discovery time safe.
func PruneObservations(parent context.Context, before time.Time) (int64, error) {
	var total int64
	for {
		ctx, cancel := context.WithTimeout(parent, operationTimeout)
		db, err := GetDBContext(ctx)
		if err != nil {
			cancel()
			return total, err
		}
		// A single DELETE is atomic; skip GORM's BEGIN/COMMIT round trips.
		result := db.WithContext(ctx).Session(&gorm.Session{SkipDefaultTransaction: true}).
			Where("applied = ? AND time < ?", true, before).
			Limit(pruneChunkSize).Delete(&Observation{})
		cancel()
		if result.Error != nil {
			return total, result.Error
		}
		total += result.RowsAffected
		if result.RowsAffected < pruneChunkSize {
			return total, nil
		}
		if err := parent.Err(); err != nil {
			return total, err
		}
	}
}

const SearchPageSize = 50

func Search(parent context.Context, name string, page int) ([]Data, int64, error) {
	if page < 1 || page > int(^uint(0)>>1)/SearchPageSize {
		return nil, 0, fmt.Errorf("invalid search page")
	}
	results := make([]Data, 0, SearchPageSize)

	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	db, err := GetDBContext(ctx)
	if err != nil {
		return nil, 0, err
	}
	escaped := escapeLike(name)
	query := db.WithContext(ctx).Model(&Data{}).Where("name LIKE ? ESCAPE '|'", "%"+escaped+"%")

	if err := query.Session(&gorm.Session{}).Select("`name`, `infohash`, `time`, `length`, `count`, COALESCE(JSON_TYPE(`files`) = 'ARRAY' AND JSON_LENGTH(`files`) > 0, FALSE) AS has_files").Order("`time` DESC, `infohash` DESC").Limit(SearchPageSize).Offset((page - 1) * SearchPageSize).Find(&results).Error; err != nil {
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

func Trending(parent context.Context, window time.Duration, limit int, until time.Time) ([]Data, error) {
	if (window != time.Hour && window != 24*time.Hour && window != 7*24*time.Hour) || limit < 1 || limit > 100 {
		return nil, fmt.Errorf("invalid trending parameters")
	}
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	db, err := GetDBContext(ctx)
	if err != nil {
		return nil, err
	}
	ranked := db.WithContext(ctx).Model(&Observation{}).
		Select("`infohash`, SUM(`count`) AS hits").
		Where("applied = ? AND time >= ? AND time < ?", true, until.Add(-window), until).
		Group("infohash").Order("hits DESC, infohash ASC").Limit(limit)
	items := make([]Data, 0, limit)
	err = db.WithContext(ctx).Model(&Data{}).
		Table("? AS resources", clause.Table{Name: (Data{}).TableName()}).
		Select("resources.name, resources.infohash, resources.time, resources.length, ranked.hits AS count, COALESCE(JSON_TYPE(resources.files) = 'ARRAY' AND JSON_LENGTH(resources.files) > 0, FALSE) AS has_files").
		Joins("JOIN (?) AS ranked ON ranked.infohash = resources.infohash", ranked).
		Order("ranked.hits DESC, resources.infohash ASC").Find(&items).Error
	return items, err
}

// RecentLimitMax bounds the recent-resources list.
const RecentLimitMax = 100

// Recent returns the most recently collected resources, newest first. `time`
// is the first-seen time and is indexed, so this is an ordered index scan.
func Recent(parent context.Context, limit int) ([]Data, error) {
	if limit < 1 || limit > RecentLimitMax {
		return nil, fmt.Errorf("invalid recent limit")
	}
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	db, err := GetDBContext(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]Data, 0, limit)
	err = db.WithContext(ctx).Model(&Data{}).
		Select("`name`, `infohash`, `time`, `length`, `count`, COALESCE(JSON_TYPE(`files`) = 'ARRAY' AND JSON_LENGTH(`files`) > 0, FALSE) AS has_files").
		Order("`time` DESC, `infohash` DESC").Limit(limit).Find(&items).Error
	return items, err
}

type ResourceCounts struct {
	Total            int64
	WithFiles        int64
	ThisRunTotal     int64
	ThisRunWithFiles int64
}

func CountResources(parent context.Context, startedAt time.Time) (ResourceCounts, error) {
	var counts ResourceCounts
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	db, err := GetDBContext(ctx)
	if err != nil {
		return counts, err
	}
	hasFiles := "COALESCE(JSON_TYPE(`files`) = 'ARRAY' AND JSON_LENGTH(`files`) > 0, FALSE)"
	err = db.WithContext(ctx).Model(&Data{}).Select(
		"COUNT(*) AS total, COALESCE(SUM("+hasFiles+"), 0) AS with_files, "+
			"COALESCE(SUM(`time` >= ?), 0) AS this_run_total, "+
			"COALESCE(SUM(`time` >= ? AND "+hasFiles+"), 0) AS this_run_with_files",
		startedAt, startedAt).Find(&counts).Error
	return counts, err
}

func Detail(parent context.Context, infoHash string) (*Data, error) {
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	db, err := GetDBContext(ctx)
	if err != nil {
		return nil, err
	}
	var result Data
	if err := db.WithContext(ctx).Where("infohash = ? AND JSON_TYPE(`files`) = 'ARRAY' AND JSON_LENGTH(`files`) > 0", infoHash).Take(&result).Error; err != nil {
		return nil, err
	}
	if len(result.Files) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	result.HasFiles = true
	return &result, nil
}

// escapeLike escapes special LIKE pattern characters (%, _, |)
// using '|' as the ESCAPE character to avoid backslash parsing issues.
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `|`, `||`)
	s = strings.ReplaceAll(s, `%`, `|%`)
	s = strings.ReplaceAll(s, `_`, `|_`)
	return s
}
