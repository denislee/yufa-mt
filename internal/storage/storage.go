// Package storage owns SQLite connection setup, schema creation, indexes,
// dynamic tables (e.g. per-MVP-mob columns), and idempotent migrations.
// Callers pass in a list of MVP mob IDs for the dynamic kill-tracking
// table — keeping the data list outside this package preserves the
// dependency direction (storage knows nothing about gameplay).
package storage

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	// Core Market Tables
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

	// Scheduler config + run history. job_config persists per-job interval
	// overrides and the enabled flag (absent rows fall back to code defaults);
	// job_runs records every scheduled/manual run with its outcome.
	createJobConfigTableSQL = `
	CREATE TABLE IF NOT EXISTS job_config (
		"job_name" TEXT NOT NULL PRIMARY KEY,
		"interval_seconds" INTEGER NOT NULL,
		"enabled" INTEGER NOT NULL DEFAULT 1
	);`
	createJobRunsTableSQL = `
	CREATE TABLE IF NOT EXISTS job_runs (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"job_name" TEXT NOT NULL,
		"trigger" TEXT NOT NULL,
		"status" TEXT NOT NULL,
		"started_at" TEXT NOT NULL,
		"finished_at" TEXT,
		"duration_ms" INTEGER,
		"message" TEXT
	);`

	// scrape_checkpoint persists the partial progress of a paginated scrape so
	// it can resume after a restart instead of re-fetching from page 1. One row
	// per scraped page holds that page's data as JSON; total_pages records the
	// page count the run was started against (a mismatch on resume means the
	// upstream dataset changed, so the checkpoint is discarded). Rows are
	// cleared once the run saves successfully.
	createScrapeCheckpointTableSQL = `
	CREATE TABLE IF NOT EXISTS scrape_checkpoint (
		"job_name" TEXT NOT NULL,
		"page" INTEGER NOT NULL,
		"total_pages" INTEGER NOT NULL,
		"payload" TEXT NOT NULL,
		"updated_at" TEXT NOT NULL,
		PRIMARY KEY ("job_name", "page")
	);`
)

const (
	// Item Definition & Cache Tables
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
	createInternalMobDBTableSQL = `
	CREATE TABLE IF NOT EXISTS internal_mob_db (
		"mob_id" INTEGER NOT NULL PRIMARY KEY,
		"aegis_name" TEXT,
		"name" TEXT,
		"name_pt" TEXT,
		"level" INTEGER,
		"hp" INTEGER,
		"sp" INTEGER,
		"base_exp" INTEGER,
		"job_exp" INTEGER,
		"mvp_exp" INTEGER,
		"attack" INTEGER,
		"attack2" INTEGER,
		"defense" INTEGER,
		"magic_defense" INTEGER,
		"str" INTEGER,
		"agi" INTEGER,
		"vit" INTEGER,
		"int" INTEGER,
		"dex" INTEGER,
		"luk" INTEGER,
		"attack_range" INTEGER,
		"skill_range" INTEGER,
		"chase_range" INTEGER,
		"size" TEXT,
		"race" TEXT,
		"element" TEXT,
		"element_level" INTEGER,
		"walk_speed" INTEGER,
		"attack_delay" INTEGER,
		"attack_motion" INTEGER,
		"damage_motion" INTEGER,
		"ai" TEXT,
		"modes" TEXT,
		"is_mvp" INTEGER,
		"drops" TEXT,
		"mvp_drops" TEXT
	);`
	// mob_server_db holds the LIVE values scraped from the running server via
	// @mobinfo (mobinfo_parser.go). It is kept separate from internal_mob_db
	// (the immutable rAthena YAML baseline) so the bestiary can show the
	// difference between stock and the server's modifications. Only the fields
	// @mobinfo reports are stored. scraped_at marks the last capture.
	createMobServerDBTableSQL = `
	CREATE TABLE IF NOT EXISTS mob_server_db (
		"mob_id" INTEGER NOT NULL PRIMARY KEY,
		"aegis_name" TEXT,
		"name" TEXT,
		"level" INTEGER,
		"hp" INTEGER,
		"base_exp" INTEGER,
		"job_exp" INTEGER,
		"mvp_exp" INTEGER,
		"attack" INTEGER,
		"attack2" INTEGER,
		"defense" INTEGER,
		"magic_defense" INTEGER,
		"str" INTEGER,
		"agi" INTEGER,
		"vit" INTEGER,
		"int" INTEGER,
		"dex" INTEGER,
		"luk" INTEGER,
		"attack_range" INTEGER,
		"skill_range" INTEGER,
		"chase_range" INTEGER,
		"size" TEXT,
		"race" TEXT,
		"element" TEXT,
		"element_level" INTEGER,
		"is_mvp" INTEGER,
		"drops" TEXT,
		"mvp_drops" TEXT,
		"scraped_at" TEXT
	);`
	// mobscrape_config holds the single-row, admin-editable parameters for the
	// @mobinfo injection sweep (id range + inter-command delay). Pinned to id=1.
	createMobScrapeConfigTableSQL = `
	CREATE TABLE IF NOT EXISTS mobscrape_config (
		"id" INTEGER PRIMARY KEY CHECK (id = 1),
		"from_id" INTEGER NOT NULL,
		"to_id" INTEGER NOT NULL,
		"delay_ms" INTEGER NOT NULL
	);`
)

const (
	// Player & Guild Tables
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
		"last_updated" TEXT NOT NULL,
		"is_active" INTEGER NOT NULL DEFAULT 1
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
	createWoeSeasonsTableSQL = `
	CREATE TABLE IF NOT EXISTS woe_seasons (
		"season_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"start_date" TEXT NOT NULL,
		"end_date" TEXT -- Can be NULL if season is ongoing
	);`
	createWoeEventsTableSQL = `
	CREATE TABLE IF NOT EXISTS woe_events (
		"event_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"season_id" INTEGER NOT NULL,
		"event_date" TEXT NOT NULL,
		"is_season_summary" INTEGER NOT NULL DEFAULT 0,
		FOREIGN KEY("season_id") REFERENCES "woe_seasons"("season_id") ON DELETE CASCADE
	);`
	createWoeEventRankingsTableSQL = `
	CREATE TABLE IF NOT EXISTS woe_event_rankings (
		"event_id" INTEGER NOT NULL,
		"character_name" TEXT NOT NULL,
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
		PRIMARY KEY("event_id", "character_name"),
		FOREIGN KEY("event_id") REFERENCES "woe_events"("event_id") ON DELETE CASCADE
	);`
)

const (
	// Visitor Tracking Tables
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
)

const (
	// Discord Trading Post Tables
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
)

const (
	// Chat Tables
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

func columnExists(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return false, fmt.Errorf("could not query table info for %s: %w", table, err)
	}
	defer rows.Close()

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
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, nil
}

func addColumnIfMissing(db *sql.DB, table, column, columnDef string) error {
	exists, err := columnExists(db, table, column)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if _, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, columnDef)); err != nil {
		return fmt.Errorf("failed to add '%s' column to '%s' table: %w", column, table, err)
	}
	return nil
}

func applyMigrations(db *sql.DB) error {
	if err := addColumnIfMissing(db, "characters", "zeny", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := addColumnIfMissing(db, "guilds", "is_active", "INTEGER NOT NULL DEFAULT 1"); err != nil {
		return err
	}
	if err := addColumnIfMissing(db, "guilds", "emblem_local_path", "TEXT"); err != nil {
		return err
	}
	// event_kind lets readers filter by category without scanning the
	// free-form activity_description. New rows set it at insert time;
	// existing rows get backfilled below.
	addedKind, err := addColumnIfMissingReport(db, "character_changelog", "event_kind", "TEXT")
	if err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_changelog_kind_time_desc ON character_changelog (event_kind, change_time DESC);`); err != nil {
		return fmt.Errorf("failed to create idx_changelog_kind_time_desc: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_changelog_kind_char_time_desc ON character_changelog (event_kind, character_name, change_time DESC);`); err != nil {
		return fmt.Errorf("failed to create idx_changelog_kind_char_time_desc: %w", err)
	}
	if addedKind {
		if _, err := db.Exec(backfillChangelogKindSQL); err != nil {
			return fmt.Errorf("failed to backfill character_changelog.event_kind: %w", err)
		}
	}
	// Normalize live-scraped race labels onto the YAML baseline's spellings so
	// the bestiary diff stops flagging equivalent races. @mobinfo prints the
	// engine's display names ("Beast", "Demi-Human") where internal_mob_db uses
	// the enum names ("Brute", "Demihuman"); the parser now maps these at scrape
	// time, this backfills rows captured before that fix. Idempotent: only the
	// old spellings match, so re-running it on later startups is a no-op.
	if _, err := db.Exec(backfillMobServerRaceSQL); err != nil {
		return fmt.Errorf("failed to backfill mob_server_db.race: %w", err)
	}
	return nil
}

// backfillMobServerRaceSQL rewrites the two race labels that @mobinfo spells
// differently from the rAthena YAML baseline. Kept in sync with normalizeRace
// in internal/server/mobinfo_parser.go.
const backfillMobServerRaceSQL = `
UPDATE mob_server_db SET race = CASE race
	WHEN 'Beast'      THEN 'Brute'
	WHEN 'Demi-Human' THEN 'Demihuman'
	ELSE race
END
WHERE race IN ('Beast', 'Demi-Human');`

// backfillChangelogKindSQL classifies existing rows from their description.
// The patterns mirror the formats produced by logCharacterActivity callers.
const backfillChangelogKindSQL = `
UPDATE character_changelog SET event_kind = CASE
	WHEN activity_description LIKE 'Dropped item: %' THEN 'drop'
	WHEN activity_description LIKE 'Joined guild %' THEN 'guild_join'
	WHEN activity_description LIKE 'Left guild %' THEN 'guild_leave'
	WHEN activity_description LIKE 'Moved from guild %' THEN 'guild_move'
	WHEN activity_description LIKE 'Leveled up to Base Level%' THEN 'level_base'
	WHEN activity_description LIKE 'Leveled up to Job Level%' THEN 'level_job'
	WHEN activity_description LIKE 'Gained %experience%' THEN 'exp_gain'
	WHEN activity_description LIKE 'Lost %experience%' THEN 'exp_loss'
	WHEN activity_description LIKE 'Zeny increased by%' THEN 'zeny_up'
	WHEN activity_description LIKE 'Zeny decreased by%' THEN 'zeny_down'
	WHEN activity_description LIKE 'Changed class from%' THEN 'class_change'
	WHEN activity_description LIKE 'New character %' THEN 'new_char'
	ELSE 'other'
END
WHERE event_kind IS NULL;`

// addColumnIfMissingReport is like addColumnIfMissing but returns whether
// the column had to be added — callers use this to trigger one-time
// backfills only on first migration.
func addColumnIfMissingReport(db *sql.DB, table, column, columnDef string) (bool, error) {
	exists, err := columnExists(db, table, column)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	if _, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, columnDef)); err != nil {
		return false, fmt.Errorf("failed to add '%s' column to '%s' table: %w", column, table, err)
	}
	return true, nil
}

// Open opens the SQLite database at filepath, configures the pool for
// WAL-mode concurrent reads, and runs schema/indexes/dynamic-tables/
// migrations in order. mvpMobIDs are mob IDs for the dynamic
// character_mvp_kills table; pass an empty slice to skip kill tracking.
func Open(filepath string, mvpMobIDs []string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filepath+"?_journal_mode=WAL&_busy_timeout=5000&_sync=NORMAL&_foreign_keys=on")
	if err != nil {
		return nil, err
	}

	// With WAL mode enabled, SQLite supports concurrent reads alongside a
	// single writer. MaxOpenConns > 1 enables parallel reads.
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}
	if err := createIndexes(db); err != nil {
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}
	if err := createDynamicTables(db, mvpMobIDs); err != nil {
		return nil, fmt.Errorf("failed to create dynamic tables: %w", err)
	}
	if err := applyMigrations(db); err != nil {
		return nil, fmt.Errorf("failed to apply migrations: %w", err)
	}

	return db, nil
}

// createTables executes all the CREATE TABLE and CREATE VIEW statements in a deterministic order.
func createTables(db *sql.DB) error {
	type tableQuery struct {
		name  string
		query string
	}
	queries := []tableQuery{
		{"items", createItemsTableSQL},
		{"market_events", createEventsTableSQL},
		{"scrape_history", createHistoryTableSQL},
		{"player_history", createPlayerHistoryTableSQL},
		{"guilds", createGuildsTableSQL},
		{"characters", createCharactersTableSQL},
		{"character_changelog", createChangelogTableSQL},
		{"v_character_changelog", createChangelogViewSQL},
		{"visitors", createVisitorsTableSQL},
		{"page_views", createPageViewsTableSQL},
		{"trading_posts", createTradingPostsTableSQL},
		{"trading_post_items", createTradingPostItemsTableSQL},
		{"internal_item_db", createInternalItemDBTableSQL},
		{"internal_mob_db", createInternalMobDBTableSQL},
		{"mob_server_db", createMobServerDBTableSQL},
		{"mobscrape_config", createMobScrapeConfigTableSQL},
		{"woe_seasons", createWoeSeasonsTableSQL},
		{"woe_events", createWoeEventsTableSQL},
		{"woe_event_rankings", createWoeEventRankingsTableSQL},
		{"chat", createChatTableSQL},
		{"chat_activity_log", createChatActivityLogTableSQL},
		{"rms_item_cache", createRMSCacheTableSQL},
		{"rms_item_cache_fts", createRMSFTSSTableSQL},
		{"rms_triggers", createTriggersSQL},
		{"job_config", createJobConfigTableSQL},
		{"job_runs", createJobRunsTableSQL},
		{"scrape_checkpoint", createScrapeCheckpointTableSQL},
	}

	for _, t := range queries {
		if _, err := db.Exec(t.query); err != nil {
			return fmt.Errorf("could not create table/view/trigger '%s': %w", t.name, err)
		}
	}
	return nil
}

// createIndexes executes all the CREATE INDEX statements.
func createIndexes(db *sql.DB) error {
	indexQueries := []string{
		createPageIndexSQL, // This was missed in the original map
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
		// 'internal_mob_db' table
		`CREATE INDEX IF NOT EXISTS idx_internal_mob_db_race ON internal_mob_db (race);`,
		`CREATE INDEX IF NOT EXISTS idx_internal_mob_db_element ON internal_mob_db (element);`,
		`CREATE INDEX IF NOT EXISTS idx_internal_mob_db_lower_name ON internal_mob_db (LOWER(name));`,
		`CREATE INDEX IF NOT EXISTS idx_internal_mob_db_lower_name_pt ON internal_mob_db (LOWER(name_pt));`,
		// 'woe_events' table
		`CREATE INDEX IF NOT EXISTS idx_woe_events_season_id ON woe_events (season_id);`,
		`CREATE INDEX IF NOT EXISTS idx_woe_events_date_desc ON woe_events (event_date DESC);`,
		// 'woe_event_rankings' table
		`CREATE INDEX IF NOT EXISTS idx_woe_rankings_event_id ON woe_event_rankings (event_id);`,
		`CREATE INDEX IF NOT EXISTS idx_woe_rankings_guild_name ON woe_event_rankings (guild_name);`,
		`CREATE INDEX IF NOT EXISTS idx_woe_rankings_char_name ON woe_event_rankings (character_name);`,
		// 'chat' table
		`CREATE INDEX IF NOT EXISTS idx_chat_channel_timestamp_desc ON chat (channel, timestamp DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_chat_timestamp_desc ON chat (timestamp DESC);`,
		// 'job_runs' table
		`CREATE INDEX IF NOT EXISTS idx_job_runs_name_started ON job_runs (job_name, started_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_job_runs_started ON job_runs (started_at DESC);`,
	}

	for i, query := range indexQueries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("could not create index #%d: %w", i, err)
		}
	}
	return nil
}

// createDynamicTables creates tables whose schemas depend on runtime variables.
func createDynamicTables(db *sql.DB, mvpMobIDs []string) error {
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

	if _, err := db.Exec(createMvpKillsTableSQL); err != nil {
		return fmt.Errorf("could not create character_mvp_kills table: %w", err)
	}

	return nil
}

// Close runs PRAGMA optimize to update SQLite query planner statistics
// prior to closing the connection. This is highly recommended for
// production-grade database systems using SQLite.
func Close(db *sql.DB) error {
	slog.Info("Running SQLite query optimizer (PRAGMA optimize)...")
	if _, err := db.Exec("PRAGMA optimize;"); err != nil {
		slog.Warn("SQLite PRAGMA optimize failed", "error", err)
	}
	return db.Close()
}
