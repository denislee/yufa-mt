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

// Process modes. The same binary runs as one of three roles, selected by the
// --mode flag (or YUFA_MODE env). See docs/two-process-split-plan.md.
const (
	// ModeAll is the single-process default: proxies + app in one process with
	// in-process chat injection. Used for dev (`make run`) and any box that
	// hasn't been split into two units.
	ModeAll = "all"
	// ModeApp runs everything EXCEPT the proxies (web, scrapers, chat capture,
	// Discord, scheduler). Chat injection / readiness go to the proxy process
	// over the IPC socket. Redeploys freely without dropping the game link.
	ModeApp = "app"
	// ModeProxy runs ONLY the PIN + zone proxies (which own the live game
	// connection) and the IPC server the app talks to. Restarted rarely.
	ModeProxy = "proxy"
)

// Config is the typed, validated configuration the server uses.
type Config struct {
	// Mode selects the process role: ModeAll (default), ModeApp, or ModeProxy.
	// Set via the --mode flag (cmd/server/main.go) or YUFA_MODE env.
	Mode string

	// ProxyIPCSocket is the unix-domain socket the app (ModeApp) uses to ask the
	// proxy (ModeProxy) to inject chat and report readiness. Unused in ModeAll
	// (in-process). Default /run/yufa-mt/proxy.sock; env PROXY_IPC_SOCKET.
	ProxyIPCSocket string

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

	// Source IPs whose requests are excluded from the visitor/page-view
	// stats (the operator's own browsing). Comma-separated VISITOR_IGNORE_IPS.
	// Admins are also auto-excluded via a no-track cookie set on login.
	VisitorIgnoreIPs []string

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

	// In-process zone-server proxy (internal/server/zoneproxy.go). When
	// ZoneProxyEnabled, the app installs an iptables REDIRECT of the zone/map
	// server port to a local listener that relays the live game connection and
	// can inject chat/atcommand packets (CZ_REQUEST_CHAT 0x00f3) — used to drive
	// the @mobinfo reference-data scrape. Linux-only; needs CAP_NET_ADMIN (the
	// same capability the PIN proxy uses). Independent of PinProxy (different
	// port). Plaintext on this server, so injection is straightforward.
	// Defaults ON (set ZONE_PROXY=0 to disable); startBackgroundJobs skips it
	// in dev mode (DISABLE_SCRAPERS) so a dev box never installs iptables rules.
	ZoneProxyEnabled bool
	// ZoneProxyZonePort is the zone/map TCP port carrying in-game chat and
	// atcommands (the REDIRECT match). Default 6121 (matches ChatCapturePort).
	ZoneProxyZonePort string
	// ZoneProxyListenPort is the local port the REDIRECT delivers to and the
	// in-process proxy listens on. Default 6799.
	ZoneProxyListenPort string
	// ZoneProxyServerIP, when set, narrows the REDIRECT rule to one server IP.
	ZoneProxyServerIP string
	// ZoneProxyCharName is an optional fallback character name used as the
	// "<name> : <msg>" prefix when injecting chat. Normally the proxy learns it
	// from the client's own outgoing chat; this only seeds it before then.
	ZoneProxyCharName string

	// Mob-info scrape first-run defaults (internal/server/mobscrape.go). The
	// live values are persisted in the mobscrape_config table and editable from
	// the admin Schedulers tab; these env vars only seed the initial row.
	MobScrapeFromID  int // first mob id to sweep (default 1001)
	MobScrapeToID    int // last mob id to sweep (default 2500)
	MobScrapeDelayMs int // delay between @mobinfo commands, ms (default 400)

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
		Mode:                 envOr("YUFA_MODE", ModeAll),
		ProxyIPCSocket:       envOr("PROXY_IPC_SOCKET", "/run/yufa-mt/proxy.sock"),
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
		ZoneProxyEnabled:     boolEnvDefault("ZONE_PROXY", true),
		ZoneProxyZonePort:    envOr("ZONE_PROXY_PORT", "6121"),
		ZoneProxyListenPort:  envOr("ZONE_PROXY_LISTEN_PORT", "6799"),
		ZoneProxyServerIP:    os.Getenv("ZONE_PROXY_SERVER_IP"),
		ZoneProxyCharName:    os.Getenv("ZONE_PROXY_CHAR_NAME"),
		MobScrapeFromID:      intEnv("MOBSCRAPE_FROM", 1001),
		MobScrapeToID:        intEnv("MOBSCRAPE_TO", 2500),
		MobScrapeDelayMs:     intEnv("MOBSCRAPE_DELAY_MS", 400),
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

	if ips := os.Getenv("VISITOR_IGNORE_IPS"); ips != "" {
		for _, ip := range strings.Split(ips, ",") {
			if trimmed := strings.TrimSpace(ip); trimmed != "" {
				cfg.VisitorIgnoreIPs = append(cfg.VisitorIgnoreIPs, trimmed)
			}
		}
	}

	var problems []string

	switch cfg.Mode {
	case ModeAll, ModeApp, ModeProxy:
	default:
		problems = append(problems, fmt.Sprintf("invalid YUFA_MODE/--mode %q (want %q, %q, or %q)",
			cfg.Mode, ModeAll, ModeApp, ModeProxy))
	}

	// ModeProxy owns no DB, serves no HTTP and needs no admin password: it only
	// runs the proxies + IPC server. Skip the app-oriented checks for it.
	if cfg.Mode == ModeProxy {
		if cfg.ProxyIPCSocket == "" {
			problems = append(problems, "PROXY_IPC_SOCKET is empty (required in proxy mode)")
		}
		if len(problems) > 0 {
			return nil, fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
		}
		return cfg, nil
	}

	if cfg.Mode == ModeApp && cfg.ProxyIPCSocket == "" {
		problems = append(problems, "PROXY_IPC_SOCKET is empty (required in app mode)")
	}
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
