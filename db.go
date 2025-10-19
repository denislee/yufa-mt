package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// db is a package-level variable to hold the database connection pool.
var db *sql.DB

// applyMigrations checks for and applies database schema changes.
func applyMigrations(db *sql.DB) error {
	// --- MIGRATION 1: Add 'zeny' column to 'characters' table ---
	// Check if the column already exists to make this operation idempotent.
	rows, err := db.Query("PRAGMA table_info(characters);")
	if err != nil {
		return fmt.Errorf("could not query table info for characters: %w", err)
	}
	defer rows.Close()

	var columnExists bool
	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notnull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return err
		}
		if name == "zeny" {
			columnExists = true
			break
		}
	}

	if !columnExists {
		// If the column does not exist, add it. Default to 0 and disallow NULLs.
		_, err := db.Exec("ALTER TABLE characters ADD COLUMN zeny INTEGER NOT NULL DEFAULT 0;")
		if err != nil {
			return fmt.Errorf("failed to add 'zeny' column to 'characters' table: %w", err)
		}
	}

	return nil
}

// initDB opens the database file and creates the application tables if they don't exist.
func initDB(filepath string) (*sql.DB, error) {
	var err error
	db, err = sql.Open("sqlite3", filepath)
	if err != nil {
		return nil, err
	}

	// SQL statement to create the 'items' table for market listings.
	createItemsTableSQL := `
	CREATE TABLE IF NOT EXISTS items (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"name_of_the_item" TEXT,
		"item_id" INTEGER,
		"quantity" INTEGER,
		"price" TEXT,
		"store_name" TEXT,
		"seller_name" TEXT,
		"date_and_time_retrieved" TEXT,
		"map_name" TEXT,
		"map_coordinates" TEXT,
		"is_available" INTEGER DEFAULT 1
	);`
	if _, err = db.Exec(createItemsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create items table: %w", err)
	}

	// SQL statement to create the 'market_events' table for logging changes.
	createEventsTableSQL := `
	CREATE TABLE IF NOT EXISTS market_events (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"event_timestamp" TEXT NOT NULL,
		"event_type" TEXT NOT NULL,
		"item_name" TEXT NOT NULL,
		"item_id" INTEGER,
		"details" TEXT
	);`
	if _, err = db.Exec(createEventsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create market_events table: %w", err)
	}

	// SQL statement to create the 'scrape_history' table to track scrape times.
	createHistoryTableSQL := `
	CREATE TABLE IF NOT EXISTS scrape_history (
		"timestamp" TEXT NOT NULL PRIMARY KEY
	);`
	if _, err = db.Exec(createHistoryTableSQL); err != nil {
		return nil, fmt.Errorf("could not create scrape_history table: %w", err)
	}

	// SQL statement to create the 'rms_item_cache' table for RateMyServer data.
	createRMSCacheTableSQL := `
	CREATE TABLE IF NOT EXISTS rms_item_cache (
		"item_id" INTEGER NOT NULL PRIMARY KEY,
		"name" TEXT,
		"name_pt" TEXT,
		"image_url" TEXT,
		"item_type" TEXT,
		"item_class" TEXT,
		"buy" TEXT,
		"sell" TEXT,
		"weight" TEXT,
		"prefix" TEXT,
		"description" TEXT,
		"script" TEXT,
		"dropped_by_json" TEXT,
		"obtainable_from_json" TEXT,
		"last_checked" TEXT
	);`
	if _, err = db.Exec(createRMSCacheTableSQL); err != nil {
		return nil, fmt.Errorf("could not create rms_item_cache table: %w", err)
	}

	// SQL statement to create the 'player_history' table.
	createPlayerHistoryTableSQL := `
	CREATE TABLE IF NOT EXISTS player_history (
		"timestamp" TEXT NOT NULL PRIMARY KEY,
		"count" INTEGER NOT NULL,
		"seller_count" INTEGER
	);`
	if _, err = db.Exec(createPlayerHistoryTableSQL); err != nil {
		return nil, fmt.Errorf("could not create player_history table: %w", err)
	}

	// SQL statement to create the 'guilds' table.
	createGuildsTableSQL := `
	CREATE TABLE IF NOT EXISTS guilds (
		"rank" INTEGER NOT NULL,
		"name" TEXT NOT NULL PRIMARY KEY,
		"level" INTEGER NOT NULL,
		"experience" INTEGER NOT NULL,
		"master" TEXT NOT NULL,
		"emblem_url" TEXT,
		"last_updated" TEXT NOT NULL
	);`
	if _, err = db.Exec(createGuildsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create guilds table: %w", err)
	}

	// SQL statement to create the 'characters' table, now with guild info.
	createCharactersTableSQL := `
	CREATE TABLE IF NOT EXISTS characters (
		"rank" INTEGER NOT NULL,
		"name" TEXT NOT NULL PRIMARY KEY,
		"base_level" INTEGER NOT NULL,
		"job_level" INTEGER NOT NULL,
		"experience" REAL NOT NULL,
		"class" TEXT NOT NULL,
		"guild_name" TEXT,
		"last_updated" TEXT NOT NULL,
		"last_active" TEXT NOT NULL,
		FOREIGN KEY(guild_name) REFERENCES guilds(name) ON DELETE SET NULL ON UPDATE CASCADE
	);`
	if _, err = db.Exec(createCharactersTableSQL); err != nil {
		return nil, fmt.Errorf("could not create characters table: %w", err)
	}

	// SQL statement to create the 'character_mvp_kills' table.
	mvpMobIDsDb := []string{
		"1038", "1039", "1046", "1059", "1086", "1087", "1112", "1115", "1147",
		"1150", "1157", "1159", "1190", "1251", "1252", "1272", "1312", "1373",
		"1389", "1418", "1492", "1511",
	}
	var mvpColumns []string
	mvpColumns = append(mvpColumns, `"character_name" TEXT NOT NULL PRIMARY KEY`)
	for _, mobID := range mvpMobIDsDb {
		mvpColumns = append(mvpColumns, fmt.Sprintf(`"mvp_%s" INTEGER NOT NULL DEFAULT 0`, mobID))
	}

	createMvpKillsTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS character_mvp_kills (
		%s,
		FOREIGN KEY(character_name) REFERENCES characters(name) ON DELETE CASCADE ON UPDATE CASCADE
	);`, strings.Join(mvpColumns, ",\n\t\t"))

	if _, err = db.Exec(createMvpKillsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create character_mvp_kills table: %w", err)
	}

	// SQL statement to create the 'character_changelog' table.
	createChangelogTableSQL := `CREATE TABLE IF NOT EXISTS character_changelog (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"character_name" TEXT NOT NULL,
		"change_time" TEXT NOT NULL,
		"activity_description" TEXT NOT NULL,
		FOREIGN KEY(character_name) REFERENCES characters(name) ON DELETE CASCADE ON UPDATE CASCADE
	);`
	if _, err = db.Exec(createChangelogTableSQL); err != nil {
		return nil, fmt.Errorf("could not create character_changelog table: %w", err)
	}

	// SQL statement to create the 'v_character_changelog' view.
	createChangelogViewSQL := `CREATE VIEW IF NOT EXISTS v_character_changelog AS
		SELECT
			id,
			character_name,
			change_time,
			activity_description
		FROM
			character_changelog
		ORDER BY
			change_time DESC;`
	if _, err = db.Exec(createChangelogViewSQL); err != nil {
		return nil, fmt.Errorf("could not create v_character_changelog view: %w", err)
	}

	// SQL statement for the 'visitors' table.
	createVisitorsTableSQL := `
	CREATE TABLE IF NOT EXISTS visitors (
		"visitor_hash" TEXT NOT NULL PRIMARY KEY,
		"first_visit" TEXT NOT NULL,
		"last_visit" TEXT NOT NULL
	);`
	if _, err = db.Exec(createVisitorsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create visitors table: %w", err)
	}

	createPageViewsTableSQL := `
	CREATE TABLE IF NOT EXISTS page_views (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"visitor_hash" TEXT NOT NULL,
		"page_path" TEXT NOT NULL,
		"view_timestamp" TEXT NOT NULL
	);`
	if _, err = db.Exec(createPageViewsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create page_views table: %w", err)
	}

	createPageIndexSQL := `CREATE INDEX IF NOT EXISTS idx_page_path ON page_views (page_path);`
	if _, err = db.Exec(createPageIndexSQL); err != nil {
		return nil, fmt.Errorf("could not create index on page_views: %w", err)
	}

	// This table holds info about the post itself, not the items.
	createTradingPostsTableSQL := `
CREATE TABLE IF NOT EXISTS trading_posts (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "title" TEXT NOT NULL,
    "post_type" TEXT NOT NULL, -- 'buying' or 'selling'
    "character_name" TEXT NOT NULL,
    "contact_info" TEXT,
    "notes" TEXT,
    "created_at" TEXT NOT NULL,
    "edit_token_hash" TEXT NOT NULL
);`
	if _, err = db.Exec(createTradingPostsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create trading_posts table: %w", err)
	}

	// This table holds the individual items, linked to a post.
	createTradingPostItemsTableSQL := `
CREATE TABLE IF NOT EXISTS trading_post_items (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "post_id" INTEGER NOT NULL,
    "item_name" TEXT NOT NULL,
    "item_id" INTEGER,
    "quantity" INTEGER NOT NULL,
    "price" INTEGER NOT NULL,
    "currency" TEXT NOT NULL DEFAULT 'zeny',
    "payment_methods" TEXT NOT NULL DEFAULT 'zeny',
    "refinement" INTEGER NOT NULL DEFAULT 0,
    "slots" INTEGER NOT NULL DEFAULT 0,
    "card1" TEXT,
    "card2" TEXT,
    "card3" TEXT,
    "card4" TEXT,
    FOREIGN KEY(post_id) REFERENCES trading_posts(id) ON DELETE CASCADE
);`
	if _, err = db.Exec(createTradingPostItemsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create trading_post_items table: %w", err)
	}
	// After all tables are ensured to exist, apply any pending migrations.
	if err := applyMigrations(db); err != nil {
		return nil, err
	}

	return db, nil
}
