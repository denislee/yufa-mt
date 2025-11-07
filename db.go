package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

const (
	createItemsTableSQL = `
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

	createEventsTableSQL = `
	CREATE TABLE IF NOT EXISTS market_events (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"event_timestamp" TEXT NOT NULL,
		"event_type" TEXT NOT NULL,
		"item_name" TEXT NOT NULL,
		"item_id" INTEGER,
		"details" TEXT
	);`

	createHistoryTableSQL = `
	CREATE TABLE IF NOT EXISTS scrape_history (
		"timestamp" TEXT NOT NULL PRIMARY KEY
	);`

	createRMSCacheTableSQL = `
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
		"slots" INTEGER,
		"prefix" TEXT,
		"description" TEXT,
		"script" TEXT,
		"dropped_by_json" TEXT,
		"obtainable_from_json" TEXT,
		"last_checked" TEXT
	);`

	createRMSFTSSTableSQL = `
	CREATE VIRTUAL TABLE IF NOT EXISTS rms_item_cache_fts USING fts5(
		name, 
		name_pt, 
		content='rms_item_cache', 
		content_rowid='item_id'
	);`

	createTriggersSQL = `
	CREATE TRIGGER IF NOT EXISTS rms_item_cache_ai AFTER INSERT ON rms_item_cache BEGIN
		INSERT INTO rms_item_cache_fts(rowid, name, name_pt) 
		VALUES (new.item_id, new.name, new.name_pt);
	END;
	CREATE TRIGGER IF NOT EXISTS rms_item_cache_ad AFTER DELETE ON rms_item_cache BEGIN
		INSERT INTO rms_item_cache_fts(rms_item_cache_fts, rowid, name, name_pt) 
		VALUES ('delete', old.item_id, old.name, old.name_pt);
	END;
	CREATE TRIGGER IF NOT EXISTS rms_item_cache_au AFTER UPDATE ON rms_item_cache BEGIN
		INSERT INTO rms_item_cache_fts(rms_item_cache_fts, rowid, name, name_pt) 
		VALUES ('delete', old.item_id, old.name, old.name_pt);
		INSERT INTO rms_item_cache_fts(rowid, name, name_pt) 
		VALUES (new.item_id, new.name, new.name_pt);
	END;
	`

	createPlayerHistoryTableSQL = `
	CREATE TABLE IF NOT EXISTS player_history (
		"timestamp" TEXT NOT NULL PRIMARY KEY,
		"count" INTEGER NOT NULL,
		"seller_count" INTEGER
	);`

	createGuildsTableSQL = `
	CREATE TABLE IF NOT EXISTS guilds (
		"rank" INTEGER NOT NULL,
		"name" TEXT NOT NULL PRIMARY KEY,
		"level" INTEGER NOT NULL,
		"experience" INTEGER NOT NULL,
		"master" TEXT NOT NULL,
		"emblem_url" TEXT,
		"last_updated" TEXT NOT NULL
	);`

	createCharactersTableSQL = `
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

	createChangelogTableSQL = `
	CREATE TABLE IF NOT EXISTS character_changelog (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"character_name" TEXT NOT NULL,
		"change_time" TEXT NOT NULL,
		"activity_description" TEXT NOT NULL,
		FOREIGN KEY(character_name) REFERENCES characters(name) ON DELETE CASCADE ON UPDATE CASCADE
	);`

	createChangelogViewSQL = `
	CREATE VIEW IF NOT EXISTS v_character_changelog AS
		SELECT
			id,
			character_name,
			change_time,
			activity_description
		FROM
			character_changelog
		ORDER BY
			change_time DESC;`

	createVisitorsTableSQL = `
	CREATE TABLE IF NOT EXISTS visitors (
		"visitor_hash" TEXT NOT NULL PRIMARY KEY,
		"first_visit" TEXT NOT NULL,
		"last_visit" TEXT NOT NULL
	);`

	createPageViewsTableSQL = `
	CREATE TABLE IF NOT EXISTS page_views (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"visitor_hash" TEXT NOT NULL,
		"page_path" TEXT NOT NULL,
		"view_timestamp" TEXT NOT NULL
	);`

	createPageIndexSQL = `
	CREATE INDEX IF NOT EXISTS idx_page_path ON page_views (page_path);`

	createTradingPostsTableSQL = `
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

	createTradingPostItemsTableSQL = `
	CREATE TABLE IF NOT EXISTS trading_post_items (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"post_id" INTEGER NOT NULL,
		"item_name" TEXT NOT NULL,
		"item_id" INTEGER,
		"quantity" INTEGER NOT NULL,
		"price_zeny" INTEGER NOT NULL DEFAULT 0,
		"price_rmt" INTEGER NOT NULL DEFAULT 0,
		"payment_methods" TEXT NOT NULL DEFAULT 'zeny',
		"refinement" INTEGER NOT NULL DEFAULT 0,
		"slots" INTEGER NOT NULL DEFAULT 0,
		"card1" TEXT,
		"card2" TEXT,
		"card3" TEXT,
		"card4" TEXT,
		FOREIGN KEY(post_id) REFERENCES trading_posts(id) ON DELETE CASCADE
	);`

	// --- NEW TABLE for YAML Item DB ---
	createInternalItemDBTableSQL = `
	CREATE TABLE IF NOT EXISTS internal_item_db (
		"item_id" INTEGER NOT NULL PRIMARY KEY,
		"aegis_name" TEXT,
		"name" TEXT,
		"name_pt" TEXT,
		"type" TEXT,
		"buy" INTEGER,
		"sell" INTEGER,
		"weight" INTEGER,
		"slots" INTEGER,
		"jobs" TEXT,
		"locations" TEXT,
		"script" TEXT,
		"equip_script" TEXT,
		"unequip_script" TEXT
	);`

	createWoeCharRankingsTableSQL = `
	CREATE TABLE IF NOT EXISTS woe_character_rankings (
		"name" TEXT NOT NULL PRIMARY KEY, -- Changed from char_id
		"class" TEXT NOT NULL,
		"guild_id" INTEGER,
		"guild_name" TEXT,
		"kill_count" INTEGER NOT NULL,
		"death_count" INTEGER NOT NULL,
		"damage_done" INTEGER NOT NULL,
		"emperium_kill" INTEGER NOT NULL,
		"healing_done" INTEGER NOT NULL,
		"score" INTEGER NOT NULL,
		"points" INTEGER NOT NULL,
		"last_updated" TEXT NOT NULL
	);`

	createChatTableSQL = `
	CREATE TABLE IF NOT EXISTS chat (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"timestamp" TEXT NOT NULL,
		"channel" TEXT NOT NULL,
		"character_name" TEXT NOT NULL,
		"message" TEXT NOT NULL
	);`

	createChatActivityLogTableSQL = `
	CREATE TABLE IF NOT EXISTS chat_activity_log (
		"timestamp" TEXT NOT NULL PRIMARY KEY -- Stores the timestamp truncated to the minute
	);`
)

func applyMigrations(db *sql.DB) error {

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

		_, err := db.Exec("ALTER TABLE characters ADD COLUMN zeny INTEGER NOT NULL DEFAULT 0;")
		if err != nil {
			return fmt.Errorf("failed to add 'zeny' column to 'characters' table: %w", err)
		}
	}

	return nil
}

func initDB(filepath string) (*sql.DB, error) {
	var err error
	db, err = sql.Open("sqlite3", filepath)
	if err != nil {
		return nil, err
	}

	// Sequentially execute all table creation queries
	queries := map[string]string{
		"items":                  createItemsTableSQL,
		"market_events":          createEventsTableSQL,
		"scrape_history":         createHistoryTableSQL,
		"player_history":         createPlayerHistoryTableSQL,
		"guilds":                 createGuildsTableSQL,
		"characters":             createCharactersTableSQL,
		"character_changelog":    createChangelogTableSQL,
		"v_character_changelog":  createChangelogViewSQL,
		"visitors":               createVisitorsTableSQL,
		"page_views":             createPageViewsTableSQL,
		"idx_page_path":          createPageIndexSQL,
		"trading_posts":          createTradingPostsTableSQL,
		"trading_post_items":     createTradingPostItemsTableSQL,
		"internal_item_db":       createInternalItemDBTableSQL,
		"woe_character_rankings": createWoeCharRankingsTableSQL,
		"chat":                   createChatTableSQL,
		"chat_activity_log":      createChatActivityLogTableSQL,
	}

	for name, query := range queries {
		if _, err = db.Exec(query); err != nil {
			return nil, fmt.Errorf("could not create table/view '%s': %w", name, err)
		}
	}

	// This list contains all the recommended indexes for performance.
	indexQueries := []string{
		// 'items' table
		`CREATE INDEX IF NOT EXISTS idx_items_name_available ON items (name_of_the_item, is_available);`,
		`CREATE INDEX IF NOT EXISTS idx_items_item_id ON items (item_id);`,
		`CREATE INDEX IF NOT EXISTS idx_items_available_seller ON items (is_available, seller_name);`,
		`CREATE INDEX IF NOT EXISTS idx_items_timestamp_desc ON items (date_and_time_retrieved DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_items_store_seller ON items (store_name, seller_name);`,
		// 'market_events' table
		`CREATE INDEX IF NOT EXISTS idx_events_timestamp_desc ON market_events (event_timestamp DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_events_item_id_type ON market_events (item_id, event_type);`,
		// 'characters' table
		`CREATE INDEX IF NOT EXISTS idx_chars_guild_name ON characters (guild_name);`,
		`CREATE INDEX IF NOT EXISTS idx_chars_class ON characters (class);`,
		// 'character_changelog' table
		`CREATE INDEX IF NOT EXISTS idx_changelog_char_time_desc ON character_changelog (character_name, change_time DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_changelog_time_desc ON character_changelog (change_time DESC);`,
		// 'page_views' table
		`CREATE INDEX IF NOT EXISTS idx_page_views_visitor_timestamp ON page_views (visitor_hash, view_timestamp DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_page_views_timestamp_desc ON page_views (view_timestamp DESC);`,
		// 'trading_posts' table
		`CREATE INDEX IF NOT EXISTS idx_trading_posts_created_desc ON trading_posts (created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_trading_posts_lookup ON trading_posts (character_name, contact_info, post_type);`,
		// 'trading_post_items' table
		`CREATE INDEX IF NOT EXISTS idx_trading_items_post_id ON trading_post_items (post_id);`,
		`CREATE INDEX IF NOT EXISTS idx_trading_items_item_id ON trading_post_items (item_id);`,
		`CREATE INDEX IF NOT EXISTS idx_trading_items_item_name ON trading_post_items (item_name);`,
		// 'internal_item_db' table
		`CREATE INDEX IF NOT EXISTS idx_internal_db_type ON internal_item_db (type);`,
		`CREATE INDEX IF NOT EXISTS idx_internal_db_slots ON internal_item_db (slots);`,
		`CREATE INDEX IF NOT EXISTS idx_internal_db_lower_name ON internal_item_db (LOWER(name));`,
		`CREATE INDEX IF NOT EXISTS idx_internal_db_lower_name_pt ON internal_item_db (LOWER(name_pt));`,
		// 'woe_character_rankings' table
		`CREATE INDEX IF NOT EXISTS idx_woe_guild_name ON woe_character_rankings (guild_name);`,
		// 'chat' table
		`CREATE INDEX IF NOT EXISTS idx_chat_channel_timestamp_desc ON chat (channel, timestamp DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_chat_timestamp_desc ON chat (timestamp DESC);`,
	}

	// Execute all index creation queries
	for i, query := range indexQueries {
		if _, err = db.Exec(query); err != nil {
			return nil, fmt.Errorf("could not create index #%d: %w", i, err)
		}
	}

	// --- Dynamic Table Creation (MVP Kills) ---
	var mvpColumns []string
	mvpColumns = append(mvpColumns, `"character_name" TEXT NOT NULL PRIMARY KEY`)
	for _, mobID := range mvpMobIDs {
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

	// --- Migrations ---
	if err := applyMigrations(db); err != nil {
		return nil, err
	}

	return db, nil
}
