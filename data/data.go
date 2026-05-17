// Package data embeds the application's seed YAML files so the binary is
// self-contained at runtime. Only the seed/ subtree is embedded —
// runtime/ holds mutable state (SQLite DB, generated password) and lives
// on the filesystem.
package data

import "embed"

//go:embed seed/*.yml
var Seed embed.FS
