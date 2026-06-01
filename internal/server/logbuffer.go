package server

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// logBufferCapacity is the number of most-recent log lines kept in memory
// for the admin log viewer. Older lines are overwritten in a ring.
const logBufferCapacity = 2000

// logRing is a fixed-size, thread-safe ring buffer of log lines. It
// implements io.Writer so it can be tee'd onto the slog handler's output
// (see initLogger): every line written to stderr is also captured here so
// the admin panel can surface recent server output without touching disk.
type logRing struct {
	mu      sync.RWMutex
	entries []string
	next    int  // index the next write lands at
	full    bool // whether the ring has wrapped at least once
}

// logBuffer is the process-wide capture of recent log output.
var logBuffer = &logRing{entries: make([]string, logBufferCapacity)}

// Write stores p as a single ring entry (slog handlers emit one record per
// Write, so this maps to one log line). The trailing newline is trimmed;
// the viewer re-joins entries with newlines.
func (r *logRing) Write(p []byte) (int, error) {
	line := strings.TrimRight(string(p), "\n")
	r.mu.Lock()
	r.entries[r.next] = line
	r.next = (r.next + 1) % len(r.entries)
	if r.next == 0 {
		r.full = true
	}
	r.mu.Unlock()
	return len(p), nil
}

// Lines returns the captured log lines in chronological order (oldest
// first). If tail > 0, only the last tail lines are returned.
func (r *logRing) Lines(tail int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var out []string
	if r.full {
		out = make([]string, 0, len(r.entries))
		out = append(out, r.entries[r.next:]...)
		out = append(out, r.entries[:r.next]...)
	} else {
		out = make([]string, r.next)
		copy(out, r.entries[:r.next])
	}

	if tail > 0 && tail < len(out) {
		out = out[len(out)-tail:]
	}
	return out
}

// adminLogsHandler serves the captured server log as plain text (oldest line
// first). The admin log viewer polls it for a live tail. A ?tail=N query
// limits the response to the last N lines (default 500, capped at the buffer
// capacity).
func adminLogsHandler(w http.ResponseWriter, r *http.Request) {
	tail := 500
	if v := r.URL.Query().Get("tail"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			tail = n
		}
	}

	lines := logBuffer.Lines(tail)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if len(lines) == 0 {
		_, _ = w.Write([]byte("(no log output captured yet)\n"))
		return
	}
	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}
