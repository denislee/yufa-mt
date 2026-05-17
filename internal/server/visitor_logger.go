package server

import (
	"context"
	"net/http"

	"github.com/denislee/yufa-mt/internal/visitor"
)

// visitorLogger is the package-internal *visitor.Logger constructed once
// by Run. It backs the visitorTracker middleware and the
// startVisitorLogger goroutine.
var visitorLogger *visitor.Logger

// visitorTracker shims into the visitor package. Existing call sites in
// server.go (e.g. mux.HandleFunc("/", visitorTracker(summaryHandler)))
// stay unchanged.
func visitorTracker(next http.HandlerFunc) http.HandlerFunc {
	return visitorLogger.Track(next)
}

// startVisitorLogger shims into the visitor package's main loop.
func startVisitorLogger(ctx context.Context) {
	visitorLogger.Run(ctx)
}
