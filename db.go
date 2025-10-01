package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// db is a package-level variable to hold the database connection pool.
var db *sql.DB

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
		"count" INTEGER NOT NULL
	);`
	if _, err = db.Exec(createPlayerHistoryTableSQL); err != nil {
		return nil, fmt.Errorf("could not create player_history table: %w", err)
	}

	return db, nil
}
