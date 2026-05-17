// Package itemdb parses item_db_*.yml files and upserts them into the
// internal_item_db SQLite table. It is invoked once at startup so the local
// item database is hydrated before the web server begins serving requests.
package itemdb

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

// Entry is one row in a YAML item_db file.
type Entry struct {
	ID            int             `yaml:"Id"`
	AegisName     string          `yaml:"AegisName"`
	Name          string          `yaml:"Name"`
	Type          string          `yaml:"Type"`
	Buy           *int64          `yaml:"Buy"`
	Sell          *int64          `yaml:"Sell"`
	Weight        *int64          `yaml:"Weight"`
	Slots         *int64          `yaml:"Slots"`
	Jobs          map[string]bool `yaml:"Jobs"`
	Locations     map[string]bool `yaml:"Locations"`
	Script        string          `yaml:"Script"`
	EquipScript   string          `yaml:"EquipScript"`
	UnEquipScript string          `yaml:"UnEquipScript"`
}

// File is the top-level structure of an item_db YAML file.
type File struct {
	Header map[string]any `yaml:"Header"`
	Body   []Entry        `yaml:"Body"`
}

// DefaultFiles is the list of YAML files this app expects under data/.
var DefaultFiles = []string{
	"data/item_db_usable.yml",
	"data/item_db_etc.yml",
	"data/item_db_equip.yml",
}

// Populate reads filenames, parses them as item_db YAML, and upserts the
// rows into internal_item_db on db. Missing or malformed files are logged
// and skipped rather than treated as fatal — the rest of the app degrades
// gracefully when items are absent.
func Populate(db *sql.DB, filenames []string) error {
	var all []Entry
	for _, fn := range filenames {
		log.Printf("[D] [ItemDB] Parsing file: %s", fn)
		data, err := os.ReadFile(fn)
		if err != nil {
			log.Printf("[W] [ItemDB] Could not read file %s: %v. Skipping.", fn, err)
			continue
		}
		var f File
		if err := yaml.Unmarshal(data, &f); err != nil {
			log.Printf("[W] [ItemDB] Could not parse YAML from %s: %v. Skipping.", fn, err)
			continue
		}
		if len(f.Body) > 0 {
			all = append(all, f.Body...)
			log.Printf("[D] [ItemDB] Found %d items in %s.", len(f.Body), fn)
		}
	}
	if len(all) == 0 {
		log.Println("[W] [ItemDB] No items found in any YAML files. Database will not be populated.")
		return nil
	}
	log.Printf("[I] [ItemDB] Total items to process from all files: %d.", len(all))
	return store(db, all)
}

func store(db *sql.DB, items []Entry) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

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

	var success int
	for _, item := range items {
		var namePT sql.NullString
		jobsJSON, err := json.Marshal(item.Jobs)
		if err != nil {
			log.Printf("[W] [ItemDB] Could not marshal Jobs for item %d: %v", item.ID, err)
			jobsJSON = []byte("{}")
		}
		locsJSON, err := json.Marshal(item.Locations)
		if err != nil {
			log.Printf("[W] [ItemDB] Could not marshal Locations for item %d: %v", item.ID, err)
			locsJSON = []byte("{}")
		}

		res, err := stmt.Exec(
			item.ID, item.AegisName, item.Name, item.Type,
			toNullInt64(item.Buy), toNullInt64(item.Sell),
			toNullInt64(item.Weight), toNullInt64(item.Slots),
			string(jobsJSON), string(locsJSON),
			item.Script, item.EquipScript, item.UnEquipScript,
			namePT,
		)
		if err != nil {
			log.Printf("[W] [ItemDB] Failed to insert item %d (%s): %v", item.ID, item.Name, err)
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			success++
		}
	}
	log.Printf("[I] [ItemDB] Successfully inserted %d new items (skipped duplicates).", success)
	return tx.Commit()
}

func toNullInt64(p *int64) sql.NullInt64 {
	if p == nil {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: *p, Valid: true}
}
