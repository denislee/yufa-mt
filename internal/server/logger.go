package server

import (
	"log"
	"log/slog"
	"os"
	"strings"
)

// initLogger configures slog as the default logger. Format and level
// are read from LOG_FORMAT ("text" | "json", default "text") and
// LOG_LEVEL ("debug" | "info" | "warn" | "error", default "info").
//
// The stdlib log package is bridged to slog so existing log.Printf
// calls inherit the same destination and formatting. Per-call
// migration from log.Printf to slog.Info/Warn/Error happens
// incrementally; this bootstrap means it's a no-op from the user's
// perspective until each call is touched.
func initLogger() {
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.EqualFold(os.Getenv("LOG_FORMAT"), "json") {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Bridge stdlib log → slog so log.Printf inherits the configured
	// handler. Existing prefixed messages keep their shape until each
	// call site is migrated to slog.Info/Warn/Error.
	log.SetFlags(0)
	log.SetOutput(stdlogWriter{logger: logger})
}

// stdlogWriter pipes stdlib log output through slog at Info level. The
// existing log lines already carry [I]/[W]/[E] prefixes, so callers can
// migrate to slog gradually without changing visible output much.
type stdlogWriter struct{ logger *slog.Logger }

func (w stdlogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	w.logger.Info(msg)
	return len(p), nil
}
