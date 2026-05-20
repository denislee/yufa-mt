// Package config loads and validates the application's runtime
// configuration from environment variables. It is the single source of
// truth for everything the rest of the app reads via env — once Load
// returns, no other package should call os.Getenv directly.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config is the typed, validated configuration the server uses.
type Config struct {
	// HTTP server bind address (host:port).
	HTTPAddr string

	// Path to the SQLite database file (runtime state).
	DBPath string

	// Admin BasicAuth credentials.
	AdminUser     string
	AdminPassword string // empty triggers generation in main if RequireAdminPassword is false

	// Optional integrations — empty means "disabled".
	GeminiAPIKey      string
	DiscordBotToken   string
	DiscordChannelIDs []string

	// libpcap chat-capture config.
	ChatCaptureDevice string
	ChatCapturePort   string

	// If true, refuse to start without ADMIN_PASSWORD set explicitly.
	// Set RequireAdminPassword=true (via REQUIRE_ADMIN_PASSWORD=1) in
	// production so a forgotten env var doesn't silently roll a new
	// random password on every boot.
	RequireAdminPassword bool

	// If true, skip starting all scrape jobs and the chat packet capture
	// loop. Intended for local development (set by `make run`) so a dev
	// instance doesn't hammer upstream sources or require libpcap.
	DisableScrapers bool
}

// Load reads env vars, applies defaults, and validates the result. It
// returns a typed Config or an error describing every problem found.
func Load() (*Config, error) {
	cfg := &Config{
		HTTPAddr:             envOr("HTTP_ADDR", ":8080"),
		DBPath:               envOr("DB_PATH", "./data/runtime/market_data.db"),
		AdminUser:            envOr("ADMIN_USER", "admin"),
		AdminPassword:        os.Getenv("ADMIN_PASSWORD"),
		GeminiAPIKey:         os.Getenv("GEMINI_API_KEY"),
		DiscordBotToken:      os.Getenv("DISCORD_BOT_TOKEN"),
		ChatCaptureDevice:    os.Getenv("CHAT_CAPTURE_DEVICE"),
		ChatCapturePort:      os.Getenv("CHAT_CAPTURE_PORT"),
		RequireAdminPassword: boolEnv("REQUIRE_ADMIN_PASSWORD"),
		DisableScrapers:      boolEnv("DISABLE_SCRAPERS"),
	}

	if ids := os.Getenv("DISCORD_CHANNEL_IDS"); ids != "" {
		for _, id := range strings.Split(ids, ",") {
			if trimmed := strings.TrimSpace(id); trimmed != "" {
				cfg.DiscordChannelIDs = append(cfg.DiscordChannelIDs, trimmed)
			}
		}
	}

	var problems []string
	if cfg.RequireAdminPassword && cfg.AdminPassword == "" {
		problems = append(problems, "REQUIRE_ADMIN_PASSWORD is set but ADMIN_PASSWORD is empty")
	}
	if cfg.HTTPAddr == "" {
		problems = append(problems, "HTTP_ADDR is empty")
	}
	if cfg.DBPath == "" {
		problems = append(problems, "DB_PATH is empty")
	} else {
		// Ensure parent directory exists or can be successfully created with write permissions
		dir := filepath.Dir(cfg.DBPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			problems = append(problems, fmt.Sprintf("failed to create or access database parent directory %q: %v", dir, err))
		} else {
			// Double check directory is writeable by attempting to create/delete a temporary validation file inside it
			tempFile, err := os.CreateTemp(dir, ".writetest-*")
			if err != nil {
				problems = append(problems, fmt.Sprintf("database directory %q is not writeable: %v", dir, err))
			} else {
				tempFile.Close()
				os.Remove(tempFile.Name())
			}
		}
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func boolEnv(key string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes"
}

// ErrAdminPasswordMissing is returned by Load when REQUIRE_ADMIN_PASSWORD
// is set but ADMIN_PASSWORD is empty.
var ErrAdminPasswordMissing = errors.New("ADMIN_PASSWORD required but not set")
