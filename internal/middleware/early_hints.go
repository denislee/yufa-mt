// Package middleware: 103 Early Hints helper.
//
// EarlyHints sends an HTTP 103 with Link: rel=preload headers for the
// site's critical assets BEFORE the wrapped handler begins its DB work.
// Modern browsers (Chrome 103+, Firefox 120+) start fetching the
// referenced assets immediately, overlapping network time with the
// server's render time and shaving 100–300 ms off slow pages.
//
// We skip 103 for htmx-boosted requests (the swap response is HTML
// fragments, not a full page) and for non-GET requests.
package middleware

import (
	"net/http"
	"strings"
)

// EarlyHints wraps h and sends a 103 Early Hints response containing
// the given link header values before delegating to h. links must be
// formatted as RFC 8288 Link header values (e.g. "</static/app.css>;
// rel=preload; as=style"). Empty links makes the middleware a no-op.
func EarlyHints(links []string, h http.Handler) http.Handler {
	if len(links) == 0 {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shouldSendEarlyHints(r) {
			hdr := w.Header()
			for _, l := range links {
				hdr.Add("Link", l)
			}
			w.WriteHeader(http.StatusEarlyHints)
			// Clear so the final response decides its own Link headers.
			hdr.Del("Link")
		}
		h.ServeHTTP(w, r)
	})
}

func shouldSendEarlyHints(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	// htmx partial swaps don't render a fresh <head> — preloading
	// stylesheets again is pointless and confuses some intermediaries.
	if r.Header.Get("HX-Request") == "true" {
		return false
	}
	p := r.URL.Path
	if strings.HasPrefix(p, "/static/") || strings.HasPrefix(p, "/emblems/") {
		return false
	}
	return true
}
