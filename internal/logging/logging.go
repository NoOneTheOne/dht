package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
)

func newLogger(output io.Writer, service string) *slog.Logger {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{
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
	return slog.New(handler).With("service", service)
}

func Init(service string) func() {
	slog.SetDefault(newLogger(os.Stdout, service))
	path := os.Getenv("LOG_FILE_PATH")
	if path == "" {
		return func() {}
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		Fatalf("open log file: %v", err)
	}
	slog.SetDefault(newLogger(file, service))
	return func() { _ = file.Close() }
}

func Writer(level slog.Level) io.Writer {
	return slog.NewLogLogger(slog.Default().Handler(), level).Writer()
}

func Fatal(values ...any) {
	slog.Error(fmt.Sprint(values...))
	os.Exit(1)
}

func Fatalf(format string, values ...any) {
	slog.Error(fmt.Sprintf(format, values...))
	os.Exit(1)
}

func Fatalln(values ...any) {
	slog.Error(fmt.Sprintln(values...))
	os.Exit(1)
}
