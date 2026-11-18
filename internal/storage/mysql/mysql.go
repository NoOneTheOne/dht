package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
	// FileNames is the searchable text of Files, see fileNamesText.
	FileNames *string `gorm:"column:file_names;type:mediumtext" json:"-"`
	HasFiles  bool    `gorm:"column:has_files;->;-:migration" json:"has_files"`
	// MatchedFiles marks search hits found only in file names, not the name.
	MatchedFiles bool `gorm:"column:matched_files;->;-:migration" json:"matched_files,omitempty"`
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
	// With InnoDB's default stopwords, words that are themselves stopwords
	// ("to", "be", "the", "of", ...) never match a full-text search. The
	// index and every query use this session setting.
	dsn.Params = map[string]string{"charset": "utf8mb4", "innodb_ft_enable_stopword": "0"}
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

// upsertRows writes one INSERT ... ON DUPLICATE KEY UPDATE statement per row.
// Do not batch them: in a multi-row statement, InnoDB drops the full-text
// document of every newly inserted row that precedes a row taking the
// update path, so mixing new and re-observed torrents in one statement
// silently hides the new ones from search. One row per statement inside the
// caller's transaction keeps both the counts and the index correct.
func upsertRows(tx *gorm.DB, values []Data) error {
	upsert := clause.OnConflict{
		Columns: []clause.Column{{Name: "infohash"}},
		// MySQL evaluates assignments left to right; inspect the old files before replacing them.
		DoUpdates: clause.Set{
			{Column: clause.Column{Name: "count"}, Value: gorm.Expr("COALESCE(`count`, 0) + VALUES(`count`)")},
			{Column: clause.Column{Name: "length"}, Value: gorm.Expr("IF((`files` IS NULL OR JSON_LENGTH(`files`) = 0 OR JSON_TYPE(`files`) = 'NULL') AND JSON_LENGTH(VALUES(`files`)) > 0, VALUES(`length`), `length`)")},
			{Column: clause.Column{Name: "file_names"}, Value: gorm.Expr("IF((`files` IS NULL OR JSON_LENGTH(`files`) = 0 OR JSON_TYPE(`files`) = 'NULL') AND JSON_LENGTH(VALUES(`files`)) > 0, VALUES(`file_names`), `file_names`)")},
			{Column: clause.Column{Name: "files"}, Value: gorm.Expr("IF(`files` IS NULL OR JSON_LENGTH(`files`) = 0 OR JSON_TYPE(`files`) = 'NULL', VALUES(`files`), `files`)")},
		},
	}
	for i := range values {
		values[i].FileNames = fileNamesText(values[i].Files)
		if err := tx.Clauses(upsert).Create(&values[i]).Error; err != nil {
			return err
		}
	}
	return nil
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
			// Once applied, an observation only serves idempotency (batch_id,
			// infohash, applied) and trending (time, count); its file list was
			// copied to the resource above and would otherwise dominate the table.
			return tx.Model(&Observation{}).Where("batch_id = ? AND applied = ?", batchID, false).
				Updates(map[string]interface{}{"applied": true, "files": gorm.Expr("NULL")}).Error
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

// Full-text search schema, built by EnsureSearchIndexes.
const (
	nameFulltextIndex   = "idx_name_fulltext"   // (name): ranks name hits first
	searchFulltextIndex = "idx_search_fulltext" // (name, file_names): filters
	fileNamesColumn     = "file_names"
	// fileNamesMaxRunes bounds the file-list text indexed per torrent, so a
	// few huge episode packs cannot dominate index size and query cost.
	fileNamesMaxRunes = 2000
	backfillBatch     = 200
	searchIndexLock   = "dht_search_indexes"
	// searchIndexGeneration tags the full-text indexes (as their comment).
	// Bump it to make EnsureSearchIndexes rebuild them. v2: rebuilt after the
	// multi-row upsert bug left new rows out of the v1 indexes.
	searchIndexGeneration = "dht-search-v2"
)

// EnsureSearchIndexes builds the full-text search schema: an ngram index on
// name, file_names for rows written before the column existed, and an ngram
// index over both. Building an index blocks writes for a while (the spider
// spools and replays them), so call this in the background from one service.
// A MySQL named lock makes concurrent callers skip. Until it finishes,
// Search degrades gracefully.
func EnsureSearchIndexes(ctx context.Context) error {
	if _, err := GetDBContext(ctx); err != nil { // Runs AutoMigrate: the table must exist.
		return err
	}
	dsn, err := mysqlDriver.ParseDSN(mysqlDSN(getCfg()))
	if err != nil {
		return err
	}
	dsn.ReadTimeout, dsn.WriteTimeout = 0, 0 // DDL may run far longer than queries.
	pool, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return err
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1) // GET_LOCK is per connection.
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: pool}), &gorm.Config{Logger: newGORMLogger(), DisableAutomaticPing: true})
	if err != nil {
		return err
	}
	db = db.WithContext(ctx)
	var locked int
	if err := db.Raw("SELECT GET_LOCK(?, 0)", searchIndexLock).Scan(&locked).Error; err != nil {
		return err
	}
	if locked != 1 {
		slog.Info("search index build already running elsewhere", "component", "mysql")
		return nil
	}
	defer db.Exec("SELECT RELEASE_LOCK(?)", searchIndexLock)

	table := clause.Table{Name: (Data{}).TableName()}
	step := func(event, sql string, args ...interface{}) error {
		started := time.Now()
		if err := db.Exec(sql, args...).Error; err != nil {
			var mysqlErr *mysqlDriver.MySQLError
			// 1060/1061: column or index created concurrently; 1091: already dropped.
			if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1060 || mysqlErr.Number == 1061 || mysqlErr.Number == 1091) {
				return nil
			}
			return fmt.Errorf("%s: %w", event, err)
		}
		slog.Info("search index step finished", "component", "mysql", "event", event, "duration_ms", time.Since(started).Milliseconds())
		return nil
	}
	// ensureIndex creates a full-text index tagged with searchIndexGeneration,
	// rebuilding an existing index with an older tag: a rebuild re-tokenizes
	// every row, restoring documents an earlier writer failed to index.
	ensureIndex := func(index, columns, event string) error {
		var comments []string
		if err := db.Raw("SELECT INDEX_COMMENT FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
			table.Name, index).Scan(&comments).Error; err != nil {
			return err
		}
		if len(comments) > 0 && comments[0] == searchIndexGeneration {
			return nil
		}
		if len(comments) > 0 {
			if err := step(event+"_outdated_dropped", "DROP INDEX `"+index+"` ON ?", table); err != nil {
				return err
			}
		}
		return step(event, "CREATE FULLTEXT INDEX `"+index+"` ON ? ("+columns+") WITH PARSER ngram COMMENT '"+searchIndexGeneration+"'", table)
	}
	if err := ensureIndex(nameFulltextIndex, "`name`", "name_fulltext_created"); err != nil {
		return err
	}
	if err := backfillFileNames(ctx, db); err != nil {
		return err
	}
	return ensureIndex(searchFulltextIndex, "`name`, `"+fileNamesColumn+"`", "search_fulltext_created")
}

// fileNamesText renders a file list as searchable text: every distinct
// directory and file name once, one per line, capped at fileNamesMaxRunes.
// Deduplication matters: an episode pack repeats its directory per file.
func fileNamesText(files []File) *string {
	if len(files) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	var b strings.Builder
	runes := 0
	for _, file := range files {
		for _, segment := range file.Path {
			segment = strings.Join(strings.Fields(segment), " ") // Keep one name per line.
			if segment == "" || seen[segment] {
				continue
			}
			seen[segment] = true
			n := utf8.RuneCountInString(segment)
			if runes+n > fileNamesMaxRunes {
				text := b.String()
				return &text
			}
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(segment)
			runes += n + 1
		}
	}
	text := b.String()
	return &text
}

// backfillFileNames fills file_names for rows written before it existed.
func backfillFileNames(ctx context.Context, db *gorm.DB) error {
	started, filled := time.Now(), 0
	for {
		var rows []struct {
			InfoHash string `gorm:"column:infohash"`
			Files    []File `gorm:"column:files;serializer:json"`
		}
		if err := db.Model(&Data{}).Select("`infohash`, `files`").
			Where("`file_names` IS NULL AND JSON_TYPE(`files`) = 'ARRAY' AND JSON_LENGTH(`files`) > 0").
			Limit(backfillBatch).Find(&rows).Error; err != nil {
			return fmt.Errorf("backfill file names: %w", err)
		}
		for _, row := range rows {
			text := fileNamesText(row.Files)
			if text == nil {
				empty := ""
				text = &empty // Never revisit rows without usable names.
			}
			if err := db.Model(&Data{}).Where("`infohash` = ?", row.InfoHash).Update("file_names", *text).Error; err != nil {
				return fmt.Errorf("backfill file names: %w", err)
			}
		}
		filled += len(rows)
		if len(rows) < backfillBatch {
			if filled > 0 {
				slog.Info("search index step finished", "component", "mysql", "event", "file_names_backfilled", "rows", filled, "duration_ms", time.Since(started).Milliseconds())
			}
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// fulltextTerms splits a search into words for MATCH ... AGAINST, and the
// words shorter than an ngram token, which only LIKE can match.
func fulltextTerms(name string) (indexed, short []string) {
	for _, word := range strings.Fields(name) {
		word = strings.ReplaceAll(word, `"`, "") // Quotes delimit boolean-mode phrases.
		switch n := utf8.RuneCountInString(word); {
		case n == 0:
		case n < 2:
			short = append(short, word)
		default:
			indexed = append(indexed, word)
		}
	}
	return indexed, short
}

// booleanPhrases requires every word, each as an ngram phrase: +"a" +"b".
func booleanPhrases(words []string) string {
	var b strings.Builder
	for i, word := range words {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(`+"`)
		b.WriteString(word)
		b.WriteByte('"')
	}
	return b.String()
}

// searchMode is how much of the full-text schema a search relies on; Search
// steps down a mode when the schema is missing (not built yet, or MariaDB).
type searchMode int

const (
	searchNameAndFiles searchMode = iota // every word in the name or file names
	searchNameOnly                       // every word in the name
	searchLike                           // the whole input as a name substring
)

// searchClause is a SQL condition with its arguments.
type searchClause struct {
	sql  string
	args []interface{}
}

func (c *searchClause) and(sql string, args ...interface{}) {
	if c.sql != "" {
		c.sql += " AND "
	}
	c.sql += sql
	c.args = append(c.args, args...)
}

// searchPlan returns the row filter and, when file names are searched, the
// condition for "matched in the name" used to rank name hits first.
func searchPlan(name string, mode searchMode) (filter, nameHit searchClause) {
	indexed, short := fulltextTerms(name)
	if mode == searchLike || len(indexed) == 0 && mode == searchNameOnly {
		filter.and("name LIKE ? ESCAPE '|'", "%"+escapeLike(name)+"%")
		return filter, searchClause{}
	}
	files := mode == searchNameAndFiles
	if len(indexed) == 0 { // Only one-character words.
		like := "%" + escapeLike(name) + "%"
		filter.and("(name LIKE ? ESCAPE '|' OR `"+fileNamesColumn+"` LIKE ? ESCAPE '|')", like, like)
		nameHit.and("name LIKE ? ESCAPE '|'", like)
		return filter, nameHit
	}
	phrases := booleanPhrases(indexed)
	if files {
		filter.and("MATCH(`name`, `"+fileNamesColumn+"`) AGAINST (? IN BOOLEAN MODE)", phrases)
		nameHit.and("MATCH(`name`) AGAINST (? IN BOOLEAN MODE)", phrases)
	} else {
		filter.and("MATCH(`name`) AGAINST (? IN BOOLEAN MODE)", phrases)
	}
	for _, word := range short {
		like := "%" + escapeLike(word) + "%"
		if files {
			filter.and("(name LIKE ? ESCAPE '|' OR `"+fileNamesColumn+"` LIKE ? ESCAPE '|')", like, like)
			nameHit.and("name LIKE ? ESCAPE '|'", like)
		} else {
			filter.and("name LIKE ? ESCAPE '|'", like)
		}
	}
	if !files {
		return filter, searchClause{}
	}
	return filter, nameHit
}

// fileSearchBudget bounds each statement that searches file names. Words
// found in almost every file list ("mkv", "1080p") make ngram phrase search
// expensive; past the budget, Search answers from names only instead.
const fileSearchBudget = 1500 * time.Millisecond

// stepDown reports errors after which Search retries in a simpler mode: an
// absent full-text index (1191) or column (1054), or a file-name search that
// exceeded its execution budget (3024).
func stepDown(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == 1191 || mysqlErr.Number == 1054 || mysqlErr.Number == 3024)
}

// SearchResult is one page of search results.
type SearchResult struct {
	Items []Data
	Total int64
	// FilesSearched is false when only names were matched: the file-name
	// index is not built yet, or the words were too common to search in time.
	FilesSearched bool
}

func Search(parent context.Context, name string, page int) (SearchResult, error) {
	if page < 1 || page > int(^uint(0)>>1)/SearchPageSize {
		return SearchResult{}, fmt.Errorf("invalid search page")
	}
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	db, err := GetDBContext(ctx)
	if err != nil {
		return SearchResult{}, err
	}
	for mode := searchNameAndFiles; ; mode++ {
		results, total, err := searchWith(db.WithContext(ctx), name, page, mode)
		if mode == searchLike || !stepDown(err) {
			return SearchResult{Items: results, Total: total, FilesSearched: err == nil && mode == searchNameAndFiles}, err
		}
	}
}

func searchWith(db *gorm.DB, name string, page int, mode searchMode) ([]Data, int64, error) {
	results := make([]Data, 0, SearchPageSize)
	filter, nameHit := searchPlan(name, mode)
	query := db.Model(&Data{}).Where(filter.sql, filter.args...)
	columns := "`name`, `infohash`, `time`, `length`, `count`, COALESCE(JSON_TYPE(`files`) = 'ARRAY' AND JSON_LENGTH(`files`) > 0, FALSE) AS has_files"
	order := "`time` DESC, `infohash` DESC"
	hint := ""
	if mode == searchNameAndFiles {
		hint = fmt.Sprintf("/*+ MAX_EXECUTION_TIME(%d) */ ", fileSearchBudget.Milliseconds())
	}
	selection := query.Session(&gorm.Session{})
	if nameHit.sql != "" {
		// Name hits first; within each group, newest first.
		selection = selection.Select(hint+columns+", NOT ("+nameHit.sql+") AS matched_files", nameHit.args...)
		order = "matched_files ASC, " + order
	} else {
		selection = selection.Select(hint + columns)
	}
	if err := selection.Order(order).Limit(SearchPageSize).Offset((page - 1) * SearchPageSize).Find(&results).Error; err != nil {
		return nil, 0, err
	}
	total := int64(len(results))
	if page > 1 || len(results) == SearchPageSize {
		if err := query.Session(&gorm.Session{}).Select(hint + "COUNT(*)").Find(&total).Error; err != nil {
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
