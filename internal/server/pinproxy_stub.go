//go:build !linux

package server

import (
	"context"
	"log/slog"
)

// startPinProxy is a no-op off Linux: the proxy relies on netfilter (iptables
// REDIRECT + SO_ORIGINAL_DST + SO_MARK), which only exists on Linux.
func startPinProxy(_ context.Context) {
	slog.Warn("PIN proxy is enabled but only supported on Linux; skipping")
}
