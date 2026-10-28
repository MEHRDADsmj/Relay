package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// New builds a slog.Logger writing to stdout with the given level and format.
func New(level, format string) (*slog.Logger, error) {
	return NewWithWriter(level, format, os.Stdout)
}

// NewWithWriter is like New but writes to w (useful for tests).
func NewWithWriter(level, format string, w io.Writer) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	case "text":
		handler = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("logging: invalid format %q (want json|text)", format)
	}
	return slog.New(handler), nil
}

func parseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logging: invalid level %q (want debug|info|warn|error)", level)
	}
}
