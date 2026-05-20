package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageLifecycleAndDynamicMVPColumns(t *testing.T) {
	// Create a temporary database file in a clean environment
	tempDir, err := os.MkdirTemp("", "yufa-mt-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")

	// Dynamic MVP Mob IDs to test schema generator
	testMobIDs := []string{"1038", "1039", "9999"}

	// 1. Open database connection (this runs table creation, indexes, dynamic tables, and migrations)
	db, err := Open(dbPath, testMobIDs)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// 2. Validate core tables exist
	coreTables := []string{"items", "market_events", "scrape_history", "player_history", "guilds", "characters", "character_changelog"}
	for _, tbl := range coreTables {
		// Just run a simple SELECT count to confirm table exists
		_, err := db.Exec("SELECT COUNT(*) FROM " + tbl)
		if err != nil {
			t.Errorf("Core table %q does not exist or query failed: %v", tbl, err)
		}
	}

	// 3. Validate dynamic MVP columns exist in character_mvp_kills
	for _, mobID := range testMobIDs {
		columnName := "mvp_" + mobID
		exists, err := columnExists(db, "character_mvp_kills", columnName)
		if err != nil {
			t.Errorf("columnExists check failed for %q: %v", columnName, err)
		}
		if !exists {
			t.Errorf("Expected dynamic MVP column %q to exist in character_mvp_kills, but it was missing", columnName)
		}
	}

	// Validate non-existent MVP column does not exist
	exists, err := columnExists(db, "character_mvp_kills", "mvp_8888")
	if err != nil {
		t.Errorf("columnExists check failed for non-existent: %v", err)
	}
	if exists {
		t.Errorf("Did not expect column mvp_8888 to exist")
	}

	// 4. Validate migrated columns exist (e.g. event_kind on character_changelog, is_active on guilds)
	migratedGuildCol, err := columnExists(db, "guilds", "is_active")
	if err != nil {
		t.Errorf("Failed to check guild migration column: %v", err)
	}
	if !migratedGuildCol {
		t.Error("Expected migrated column 'is_active' to exist in guilds table")
	}

	migratedChangelogCol, err := columnExists(db, "character_changelog", "event_kind")
	if err != nil {
		t.Errorf("Failed to check changelog migration column: %v", err)
	}
	if !migratedChangelogCol {
		t.Error("Expected migrated column 'event_kind' to exist in character_changelog table")
	}

	// 5. Close and optimize database using storage.Close()
	err = Close(db)
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}
