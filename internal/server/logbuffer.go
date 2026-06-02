package server

import (
	"net/http"
	"sort"
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
	next    int    // index the next write lands at
	full    bool   // whether the ring has wrapped at least once
	written uint64 // total lines ever written (monotonic, for Mark/LinesSince)
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
	r.written++
	r.mu.Unlock()
	return len(p), nil
}

// Mark returns the current monotonic write count. Pair it with LinesSince to
// read only the log lines emitted after the mark (used by the scheduler to
// scan a single job run's output for errors).
func (r *logRing) Mark() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.written
}

// LinesSince returns the log lines written after mark, in chronological order.
// Lines older than the ring's retention window are silently dropped (the ring
// only holds the most recent logBufferCapacity lines).
func (r *logRing) LinesSince(mark uint64) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	capacity := uint64(len(r.entries))
	oldest := uint64(0)
	if r.written > capacity {
		oldest = r.written - capacity
	}
	if mark < oldest {
		mark = oldest
	}
	out := make([]string, 0, r.written-mark)
	for i := mark; i < r.written; i++ {
		out = append(out, r.entries[i%capacity])
	}
	return out
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
// first). The admin log viewer polls it for a live tail.
//
// Query params:
//   - tail=N    — limit to the last N lines (default 100, capped at capacity).
//   - source=   — which process's logs: "app" (this process, default), "proxy"
//     (the proxy process, fetched over IPC in ModeApp), or "both" (app+proxy
//     merged by timestamp, each line tagged). In ModeAll/Proxy the app and proxy
//     share one process, so "proxy"/"both" return the same single stream.
func adminLogsHandler(w http.ResponseWriter, r *http.Request) {
	tail := 100
	if v := r.URL.Query().Get("tail"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			tail = n
		}
	}
	source := r.URL.Query().Get("source")

	var lines []string
	switch source {
	case "proxy":
		if !proxyLogsRemote {
			lines = logBuffer.Lines(tail) // single process: proxy logs == app logs
		} else if pl, err := fetchProxyLogs(tail); err != nil {
			lines = []string{"(proxy logs unavailable: " + err.Error() + ")"}
		} else {
			lines = pl
		}
	case "both":
		if !proxyLogsRemote {
			lines = logBuffer.Lines(tail) // single process: one combined stream already
		} else {
			app := logBuffer.Lines(tail)
			proxy, err := fetchProxyLogs(tail)
			if err != nil {
				proxy = []string{"(proxy logs unavailable: " + err.Error() + ")"}
			}
			lines = mergeLogsByTime(app, proxy, tail)
		}
	default: // "app" or unset
		lines = logBuffer.Lines(tail)
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if len(lines) == 0 {
		_, _ = w.Write([]byte("(no log output captured yet)\n"))
		return
	}
	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}

// mergeLogsByTime interleaves app and proxy log lines into one chronological
// stream, tagging each with its source. It sorts on the slog "time=" field,
// whose RFC3339 value sorts lexicographically into time order (the box runs a
// single fixed UTC offset). Lines with no parseable time sort to the top.
func mergeLogsByTime(app, proxy []string, tail int) []string {
	type tagged struct{ key, line string }
	all := make([]tagged, 0, len(app)+len(proxy))
	for _, l := range app {
		all = append(all, tagged{logLineTimeKey(l), "[app]   " + l})
	}
	for _, l := range proxy {
		all = append(all, tagged{logLineTimeKey(l), "[proxy] " + l})
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].key < all[j].key })
	out := make([]string, len(all))
	for i, t := range all {
		out[i] = t.line
	}
	if tail > 0 && tail < len(out) {
		out = out[len(out)-tail:]
	}
	return out
}

// logLineTimeKey extracts the value of the slog "time=" field for sorting, or
// "" if the line has none (those sort oldest-first, which is acceptable).
func logLineTimeKey(line string) string {
	_, rest, ok := strings.Cut(line, "time=")
	if !ok {
		return ""
	}
	key, _, _ := strings.Cut(rest, " ")
	return key
}
