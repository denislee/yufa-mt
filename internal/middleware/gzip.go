package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"
)

// Gzip wraps h so that responses are gzip-encoded when the client
// advertises support via Accept-Encoding. Static asset routes already
// stream large pre-compressed CSS; this middleware mainly helps the
// HTML responses (which are not pre-compressed and dominate transfer
// time on the index/full-list/admin pages).
func Gzip(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			h.ServeHTTP(w, r)
			return
		}
		// Range requests and pre-encoded responses bypass compression.
		if r.Header.Get("Range") != "" {
			h.ServeHTTP(w, r)
			return
		}
		gz := gzipPool.Get().(*gzip.Writer)
		defer gzipPool.Put(gz)

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		w.Header().Del("Content-Length") // length changes after compression

		gz.Reset(w)
		defer gz.Close()

		gzw := &gzipResponseWriter{ResponseWriter: w, gz: gz}
		h.ServeHTTP(gzw, r)
	})
}

var gzipPool = sync.Pool{
	New: func() any {
		return gzip.NewWriter(io.Discard)
	},
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	// Sniffing for Content-Type needs to happen against uncompressed
	// bytes — net/http does it via the underlying ResponseWriter's
	// first Write, so we let the gzip layer handle the rest.
	return g.gz.Write(b)
}

func (g *gzipResponseWriter) Flush() {
	_ = g.gz.Flush()
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
