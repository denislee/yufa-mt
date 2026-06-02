//go:build !linux

package server

import (
	"context"
	"fmt"
	"log/slog"
)

// The zone proxy relies on netfilter (iptables REDIRECT + SO_ORIGINAL_DST +
// SO_MARK), which only exists on Linux. Off Linux these are logged no-ops so
// the mob-scrape driver degrades gracefully.

func startZoneProxy(_ context.Context) {
	slog.Warn("zone proxy is enabled but only supported on Linux; skipping")
}

func injectChatCommandLocal(string) error {
	return fmt.Errorf("zone proxy (chat injection) is only supported on Linux")
}

func zoneProxyReadyLocal() (bool, string) { return false, "" }
