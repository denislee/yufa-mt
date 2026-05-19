package config

import (
	"testing"
)

// envKeys are every var the package reads. clearEnv() ensures tests run
// in a clean environment regardless of host state.
var envKeys = []string{
	"HTTP_ADDR", "DB_PATH", "ADMIN_USER", "ADMIN_PASSWORD",
	"GEMINI_API_KEY", "DISCORD_BOT_TOKEN", "DISCORD_CHANNEL_IDS",
	"CHAT_CAPTURE_DEVICE", "CHAT_CAPTURE_PORT", "REQUIRE_ADMIN_PASSWORD",
	"DISABLE_SCRAPERS",
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range envKeys {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr default = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.DBPath != "./data/runtime/market_data.db" {
		t.Errorf("DBPath default = %q", cfg.DBPath)
	}
	if cfg.AdminUser != "admin" {
		t.Errorf("AdminUser default = %q", cfg.AdminUser)
	}
	if cfg.RequireAdminPassword {
		t.Error("RequireAdminPassword should default to false")
	}
}

func TestLoadDiscordChannelIDsSplit(t *testing.T) {
	clearEnv(t)
	t.Setenv("DISCORD_CHANNEL_IDS", "abc, def ,, ghi")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	want := []string{"abc", "def", "ghi"}
	if len(cfg.DiscordChannelIDs) != len(want) {
		t.Fatalf("DiscordChannelIDs = %v, want %v", cfg.DiscordChannelIDs, want)
	}
	for i, id := range cfg.DiscordChannelIDs {
		if id != want[i] {
			t.Errorf("DiscordChannelIDs[%d] = %q, want %q", i, id, want[i])
		}
	}
}

func TestLoadRequireAdminPasswordValidation(t *testing.T) {
	clearEnv(t)
	t.Setenv("REQUIRE_ADMIN_PASSWORD", "true")

	if _, err := Load(); err == nil {
		t.Fatal("Load() with REQUIRE_ADMIN_PASSWORD=true and no ADMIN_PASSWORD should fail")
	}
}
