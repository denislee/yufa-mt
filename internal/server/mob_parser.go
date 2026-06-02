package server

import (
	"github.com/denislee/yufa-mt/data"
	"github.com/denislee/yufa-mt/internal/mobdb"
)

// populateMobDBOnStartup hydrates the local internal_mob_db SQLite
// table from the embedded seed YAML mob dumps.
func populateMobDBOnStartup() {
	if err := mobdb.Populate(srv.db, data.Seed, mobdb.DefaultFiles); err != nil {
		// mobdb.Populate already logs per-file warnings; this only fires
		// for a hard transaction failure.
		panic(err)
	}
}
