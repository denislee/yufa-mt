package server

import (
	"database/sql"

	"github.com/denislee/yufa-mt/internal/storage"
)

// db is the global database handle used throughout the legacy main
// package. New code should accept *sql.DB explicitly rather than reaching
// for this variable; once the main-package files are migrated into
// subpackages, this global goes away.
var db *sql.DB

// initDB is a thin wrapper that hands the MVP mob list to internal/storage
// so storage stays domain-agnostic.
func initDB(filepath string) (*sql.DB, error) {
	return storage.Open(filepath, mvpMobIDs)
}
