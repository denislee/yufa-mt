package itemdb

import (
	"testing/fstest"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestPopulateMissingFileIsNotFatal(t *testing.T) {
	// Empty FS — no files at all. Populate should log and return nil
	// rather than failing the startup.
	fs := fstest.MapFS{}
	if err := Populate(nil, fs, []string{"seed/missing.yml"}); err != nil {
		t.Fatalf("Populate with missing files returned error: %v", err)
	}
}

func TestPopulateInvalidYAMLIsNotFatal(t *testing.T) {
	fs := fstest.MapFS{
		"seed/bad.yml": &fstest.MapFile{Data: []byte("not: [valid yaml")},
	}
	if err := Populate(nil, fs, []string{"seed/bad.yml"}); err != nil {
		t.Fatalf("Populate with malformed YAML returned error: %v", err)
	}
}
