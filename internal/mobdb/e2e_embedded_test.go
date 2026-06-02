package mobdb

import (
	"path/filepath"
	"testing"

	"github.com/denislee/yufa-mt/data"
	"github.com/denislee/yufa-mt/internal/storage"
)

func TestPopulateFromEmbeddedSeed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := storage.Open(dbPath, nil)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer db.Close()

	if err := Populate(db, data.Seed, DefaultFiles); err != nil {
		t.Fatalf("Populate: %v", err)
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM internal_mob_db").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("expected mobs populated, got 0")
	}
	var name, drops string
	if err := db.QueryRow("SELECT name, drops FROM internal_mob_db WHERE mob_id=1002").Scan(&name, &drops); err != nil {
		t.Fatal(err)
	}
	t.Logf("populated %d mobs; mob 1002 name=%q drops=%s", n, name, drops)
	if name != "Poring" {
		t.Fatalf("mob 1002 name = %q, want Poring", name)
	}
}
