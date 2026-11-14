package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const slowQueryThreshold = 200 * time.Millisecond

// Resolve the default logger at write time to follow application initialization.
type gormLogger struct {
	level logger.LogLevel
}

var _ logger.Interface = (*gormLogger)(nil)

func newGORMLogger() *gormLogger {
	return &gormLogger{level: logger.Warn}
}

func (l *gormLogger) LogMode(level logger.LogLevel) logger.Interface {
	copy := *l
	copy.level = level
	return &copy
}

func (l *gormLogger) Info(ctx context.Context, msg string, args ...interface{}) {
	if l.level >= logger.Info {
		slog.InfoContext(ctx, fmt.Sprintf(msg, args...), "component", "mysql")
	}
}

func (l *gormLogger) Warn(ctx context.Context, msg string, args ...interface{}) {
	if l.level >= logger.Warn {
		slog.WarnContext(ctx, fmt.Sprintf(msg, args...), "component", "mysql")
	}
}

func (l *gormLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	if l.level >= logger.Error {
		slog.ErrorContext(ctx, fmt.Sprintf(msg, args...), "component", "mysql")
	}
}

func (l *gormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.level <= logger.Silent || errors.Is(err, gorm.ErrRecordNotFound) {
		return
	}

	duration := time.Since(begin)
	var level slog.Level
	var message string
	switch {
	case err != nil && l.level >= logger.Error:
		level, message = slog.LevelError, "MySQL query failed"
	case duration > slowQueryThreshold && l.level >= logger.Warn:
		level, message = slog.LevelWarn, "MySQL slow query"
	default:
		// Even Info mode does not log successful, fast queries.
		return
	}

	log := slog.Default()
	if !log.Enabled(ctx, level) {
		return
	}
	// The callback also returns SQL, which is deliberately discarded.
	_, rows := fc()
	log.Log(ctx, level, message,
		"component", "mysql",
		"duration_ms", float64(duration)/float64(time.Millisecond),
		"rows", rows,
		"error", err,
	)
}
