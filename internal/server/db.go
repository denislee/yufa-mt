package server

import (
	"database/sql"

	"github.com/denislee/yufa-mt/internal/config"
	"github.com/denislee/yufa-mt/internal/storage"
)

// App carries the per-process dependencies that used to be a constellation
// of package-level globals (db, adminUser/adminPass, config). It is
// constructed once by Run from a validated *config.Config and stored in
// the package-internal srv pointer. Code that previously read the bare
// db global now reaches through srv.db; tests can inject a different
// *App by assigning srv directly.
//
// This is an intermediate step toward methods-on-Handlers: the global
// is now a struct with named fields rather than a scattering of bare
// variables. The next refactor session can convert handlers into
// methods on *App without disturbing the call graph again.
type App struct {
	db  *sql.DB
	cfg *config.Config
}

// srv is the package-internal *App handle set by Run. Internal callers
// use srv.db / srv.cfg in place of the old bare globals.
var srv *App

// initDB is a thin wrapper that hands the MVP mob list to internal/storage
// so storage stays domain-agnostic.
func initDB(filepath string) (*sql.DB, error) {
	return storage.Open(filepath, mvpMobIDs)
}
