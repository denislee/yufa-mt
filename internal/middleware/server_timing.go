// Package middleware: Server-Timing wall-clock attribution.
//
// ServerTiming records how long each handler takes and emits a
// `Server-Timing: total;dur=<ms>` response header so the browser's
// devtools Network panel shows per-request server time without any
// client-side instrumentation. Cheap (one time.Now diff per request)
// and only adds bytes to the response when the timer fires before the
// first body Write.
package middleware

import (
	"net/http"
	"strconv"
	"time"
)

// ServerTiming wraps h with a wall-clock timer that writes
// `Server-Timing: total;dur=<ms>` headers. To make the header land on
// the response (rather than be a no-op after WriteHeader was already
// called), we intercept WriteHeader and Write via a tiny wrapper.
func ServerTiming(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		stw := &serverTimingWriter{ResponseWriter: w, start: start}
		h.ServeHTTP(stw, r)
		// If the handler never wrote anything, write the header now so
		// HEAD requests / 304s still get the timing.
		if !stw.wrote {
			stw.setTimingHeader()
		}
	})
}

type serverTimingWriter struct {
	http.ResponseWriter
	start time.Time
	wrote bool
}

func (w *serverTimingWriter) setTimingHeader() {
	ms := time.Since(w.start).Seconds() * 1000
	w.Header().Set("Server-Timing", "total;dur="+strconv.FormatFloat(ms, 'f', 1, 64))
}

func (w *serverTimingWriter) WriteHeader(code int) {
	if !w.wrote && code != http.StatusEarlyHints {
		w.setTimingHeader()
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *serverTimingWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.setTimingHeader()
		w.wrote = true
	}
	return w.ResponseWriter.Write(b)
}

// Flush propagates Flush calls to the wrapped writer when supported,
// so streaming handlers (e.g. SSE) keep their flush semantics through
// this wrapper.
func (w *serverTimingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
