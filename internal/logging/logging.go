package logging

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"time"
)

// Build metadata may be set at link time with -ldflags -X.
var (
	Version     = "dev"
	Revision    = "unknown"
	Environment = "unknown"
)

type requestIDKey struct{}

// WithRequestID returns a context carrying id for structured logging.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the request ID in ctx, or an empty string if absent.
func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// Replay derivations so request_id stays outside derived logger groups.
type requestHandler struct {
	slog.Handler
	root       slog.Handler
	operations []func(slog.Handler) slog.Handler
}

func (h *requestHandler) Handle(ctx context.Context, record slog.Record) error {
	handler := h.Handler
	if id := RequestID(ctx); id != "" {
		handler = h.root.WithAttrs([]slog.Attr{slog.String("request_id", id)})
		for _, operation := range h.operations {
			handler = operation(handler)
		}
	}
	return handler.Handle(ctx, record)
}

// Snapshot values once, as a normal handler does when deriving a logger.
func resolveAttrs(attrs []slog.Attr) []slog.Attr {
	resolved := make([]slog.Attr, len(attrs))
	for i, attr := range attrs {
		attr.Value = attr.Value.Resolve()
		if attr.Value.Kind() == slog.KindGroup {
			attr.Value = slog.GroupValue(resolveAttrs(attr.Value.Group())...)
		}
		resolved[i] = attr
	}
	return resolved
}

func (h *requestHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	attrs = resolveAttrs(attrs)
	return h.derive(func(handler slog.Handler) slog.Handler {
		return handler.WithAttrs(attrs)
	})
}

func (h *requestHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.derive(func(handler slog.Handler) slog.Handler {
		return handler.WithGroup(name)
	})
}

func (h *requestHandler) derive(operation func(slog.Handler) slog.Handler) slog.Handler {
	operations := make([]func(slog.Handler) slog.Handler, len(h.operations)+1)
	copy(operations, h.operations)
	operations[len(h.operations)] = operation
	return &requestHandler{
		Handler:    operation(h.Handler),
		root:       h.root,
		operations: operations,
	}
}

func newLogger(output io.Writer, service string) *slog.Logger {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	environment := os.Getenv("APP_ENV")
	if environment == "" {
		environment = Environment
	}
	revision := Revision
	if revision == "unknown" {
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, setting := range info.Settings {
				if setting.Key == "vcs.revision" && setting.Value != "" {
					revision = setting.Value
					break
				}
			}
		}
	}
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{
		AddSource: true,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 {
				switch attr.Key {
				case slog.TimeKey:
					return slog.Time("@timestamp", attr.Value.Time().UTC())
				case slog.MessageKey:
					attr.Key = "message"
				}
			}
			return attr
		},
	})
	root := handler.WithAttrs([]slog.Attr{
		slog.String("service", service),
		slog.String("host", host),
		slog.Int("pid", os.Getpid()),
		slog.String("environment", environment),
		slog.String("version", Version),
		slog.String("revision", revision),
		slog.String("go_version", runtime.Version()),
	})
	return slog.New(&requestHandler{Handler: root, root: root})
}

func setDefaultLogger(logger *slog.Logger) {
	// SetDefault snapshots caller-capture flags before clearing message prefixes.
	log.SetFlags(log.Lshortfile)
	slog.SetDefault(logger)
}

func Init(service string) func() {
	setDefaultLogger(newLogger(os.Stdout, service))
	path := os.Getenv("LOG_FILE_PATH")
	if path == "" {
		return func() {}
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		Fatalf("open log file: %v", err)
	}
	setDefaultLogger(newLogger(file, service))
	return func() { _ = file.Close() }
}

func Writer(level slog.Level) io.Writer {
	return slog.NewLogLogger(slog.Default().Handler(), level).Writer()
}

// logFatal skips itself and the Fatal wrapper to retain the user's call site.
func logFatal(message string) {
	logger := slog.Default()
	ctx := context.Background()
	if !logger.Enabled(ctx, slog.LevelError) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])
	record := slog.NewRecord(time.Now(), slog.LevelError, message, pcs[0])
	_ = logger.Handler().Handle(ctx, record)
}

func Fatal(values ...any) {
	logFatal(fmt.Sprint(values...))
	os.Exit(1)
}

func Fatalf(format string, values ...any) {
	logFatal(fmt.Sprintf(format, values...))
	os.Exit(1)
}

func Fatalln(values ...any) {
	logFatal(fmt.Sprintln(values...))
	os.Exit(1)
}
