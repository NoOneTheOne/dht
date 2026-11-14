package basic

import (
	"log/slog"
)

func CheckError(err error) {
	if err != nil {
		slog.Error("operation failed", "error", err)
	}
}
