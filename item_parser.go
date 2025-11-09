package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

// populateItemDBOnStartup is the entry point called from main.go
// It defines the files to parse and starts the process.
func populateItemDBOnStartup() {
	filenames := []string{
		"item_db_usable.yml",
		"item_db_etc.yml",
		"item_db_equip.yml",
	}

	log.Println("[I] [ItemDB] Starting YAML item database population...")
	if err := parseAndStoreItemDB(filenames); err != nil {
		log.Printf("[E] [ItemDB] Failed to populate item DB: %v", err)
	} else {
		log.Println("[I] [ItemDB] Successfully finished populating YAML item database.")
	}
}

// parseAndStoreItemDB reads, parses, and stores items from a list of YAML files.
func parseAndStoreItemDB(filenames []string) error {
	var allItems []ItemDBEntry
	for _, filename := range filenames {
		log.Printf("[D] [ItemDB] Parsing file: %s", filename)
		data, err := os.ReadFile(filename)
		if err != nil {
			log.Printf("[W] [ItemDB] Could not read file %s: %v. Skipping.", filename, err)
			continue
		}

		var itemFile ItemDBFile
		if err := yaml.Unmarshal(data, &itemFile); err != nil {
			log.Printf("[W] [ItemDB] Could not parse YAML from %s: %v. Skipping.", filename, err)
			continue
		}

		if len(itemFile.Body) > 0 {
			allItems = append(allItems, itemFile.Body...)
			log.Printf("[D] [ItemDB] Found %d items in %s.", len(itemFile.Body), filename)
		}
	}

	if len(allItems) == 0 {
		log.Println("[W] [ItemDB] No items found in any YAML files. Database will not be populated.")
		return nil
	}

	log.Printf("[I] [ItemDB] Total items to process from all files: %d. Starting database transaction.", len(allItems))
	return storeItemsInDB(allItems)
}

// storeItemsInDB takes a slice of items and upserts them into the database
// using a single transaction.
func storeItemsInDB(items []ItemDBEntry) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Rollback on error

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO internal_item_db (
			item_id, aegis_name, name, type, buy, sell, weight, slots,
			jobs, locations, script, equip_script, unequip_script, name_pt
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var successCount int
	for _, item := range items {
		// name_pt is now populated by a separate background job (populateMissingPortugueseNames)
		// We insert NULL here.
		var namePT sql.NullString

		jobsJSON, err := json.Marshal(item.Jobs)
		if err != nil {
			log.Printf("[W] [ItemDB] Could not marshal Jobs for item %d: %v", item.ID, err)
			jobsJSON = []byte("{}")
		}

		locationsJSON, err := json.Marshal(item.Locations)
		if err != nil {
			log.Printf("[W] [ItemDB] Could not marshal Locations for item %d: %v", item.ID, err)
			locationsJSON = []byte("{}")
		}

		// Convert pointer types to sql.NullInt64 for safe insertion
		nullBuy := toNullInt64(item.Buy)
		nullSell := toNullInt64(item.Sell)
		nullWeight := toNullInt64(item.Weight)
		nullSlots := toNullInt64(item.Slots)

		res, err := stmt.Exec(
			item.ID,
			item.AegisName,
			item.Name,
			item.Type,
			nullBuy,
			nullSell,
			nullWeight,
			nullSlots,
			string(jobsJSON),
			string(locationsJSON),
			item.Script,
			item.EquipScript,
			item.UnEquipScript,
			namePT, // Insert NULL for name_pt
		)
		if err != nil {
			log.Printf("[W] [ItemDB] Failed to insert item %d (%s): %v", item.ID, item.Name, err)
			continue
		}

		rowsAffected, _ := res.RowsAffected()
		if rowsAffected > 0 {
			successCount++
		}
	}

	log.Printf("[I] [ItemDB] Successfully inserted %d new items (skipped duplicates).", successCount)
	return tx.Commit()
}

// toNullInt64 converts a *int64 pointer to a sql.NullInt64 struct.
func toNullInt64(ptr *int64) sql.NullInt64 {
	if ptr == nil {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: *ptr, Valid: true}
}
