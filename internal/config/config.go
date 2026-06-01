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
	"strconv"
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

	// If true, run ONLY the chat packet capture loop and skip every market/
	// player/guild scrape job. Lets a machine dedicated to chat logging avoid
	// hammering upstream sources. Takes precedence over DisableScrapers for
	// the chat-capture loop (chat capture runs even if DisableScrapers is set).
	ChatCaptureOnly bool

	// In-process character-select PIN proxy (internal/server/pinproxy.go).
	// When PinProxyEnabled, the app installs an iptables REDIRECT of the
	// char-server port to a local listener that auto-injects the PIN, so the
	// game client clears the randomized keypad with no mouse click and no
	// Gepard bypass. Linux-only; needs CAP_NET_ADMIN (granted by the
	// documented setcap on the binary). Replaces the old standalone
	// yufa-mitm-proxy root service.
	PinProxyEnabled bool
	// PinProxyPIN is the real character-select PIN (read from YUFA_PIN, or
	// PIN). Required when injection is on.
	PinProxyPIN string
	// PinProxyCharPort is the char-server TCP port carrying the cleartext PIN
	// packets (the REDIRECT match). Default 7121.
	PinProxyCharPort string
	// PinProxyListenPort is the local port the REDIRECT delivers to and the
	// in-process proxy listens on. Default 7799.
	PinProxyListenPort string
	// PinProxySelectSlot, when >= 0, also auto-selects that character slot
	// after the PIN is accepted. <0 (unset) = don't auto-select.
	PinProxySelectSlot int
	// PinProxyServerIP, when set, narrows the REDIRECT rule to one server IP.
	PinProxyServerIP string
	// PinProxyInject toggles PIN injection; false = pure transparent relay
	// (for testing). Default true.
	PinProxyInject bool

	// SelfUpdateEnabled gates the admin "Self-Update" action
	// (internal/server/selfupdate.go): git pull → rebuild → restart. Off by
	// default so a dev instance can't be shut down by an accidental click;
	// enable with SELF_UPDATE=1 on the live box, which runs under systemd
	// (Restart=always) so the process respawns with the new binary.
	SelfUpdateEnabled bool
	// SelfUpdateBranch is the git branch the self-update pulls (default
	// "main"). Read from SELF_UPDATE_BRANCH.
	SelfUpdateBranch string
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
		ChatCaptureOnly:      boolEnv("CHAT_CAPTURE_ONLY"),
		PinProxyEnabled:      boolEnv("PIN_PROXY"),
		PinProxyPIN:          envOr("YUFA_PIN", os.Getenv("PIN")),
		PinProxyCharPort:     envOr("PIN_PROXY_CHAR_PORT", "7121"),
		PinProxyListenPort:   envOr("PIN_PROXY_LISTEN_PORT", "7799"),
		PinProxySelectSlot:   intEnv("PIN_PROXY_SELECT_SLOT", -1),
		PinProxyServerIP:     os.Getenv("PIN_PROXY_SERVER_IP"),
		PinProxyInject:       boolEnvDefault("PIN_PROXY_INJECT", true),
		SelfUpdateEnabled:    boolEnv("SELF_UPDATE"),
		SelfUpdateBranch:     envOr("SELF_UPDATE_BRANCH", "main"),
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

	// Note: PIN_PROXY enabled without a PIN is NOT a fatal config error — the
	// proxy is auxiliary to chat capture, so the server just logs and skips it
	// at startup (see startPinProxy) rather than refusing to boot.

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

// boolEnvDefault is boolEnv but returns def when the key is unset/empty.
func boolEnvDefault(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes"
}

// intEnv parses key as an int, returning def when unset or unparseable.
func intEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// ErrAdminPasswordMissing is returned by Load when REQUIRE_ADMIN_PASSWORD
// is set but ADMIN_PASSWORD is empty.
var ErrAdminPasswordMissing = errors.New("ADMIN_PASSWORD required but not set")
