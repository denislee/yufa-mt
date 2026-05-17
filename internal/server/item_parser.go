package server

import (
	"github.com/denislee/yufa-mt/data"
	"github.com/denislee/yufa-mt/internal/itemdb"
)

// populateItemDBOnStartup hydrates the local internal_item_db SQLite
// table from the embedded seed YAML item dumps.
func populateItemDBOnStartup() {
	if err := itemdb.Populate(srv.db, data.Seed, itemdb.DefaultFiles); err != nil {
		// itemdb.Populate already logs per-file warnings; this only fires
		// for a hard transaction failure.
		panic(err)
	}
}
