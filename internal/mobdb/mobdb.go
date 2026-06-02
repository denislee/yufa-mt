// Package mobdb parses mob_db.yml files (the rAthena MOB_DB dump format) and
// upserts them into the internal_mob_db SQLite table. It mirrors the itemdb
// package: it is invoked once at startup so the local monster database is
// hydrated before the web server begins serving requests.
package mobdb

import (
	"database/sql"
	"encoding/json"
	"io/fs"
	"log"

	"gopkg.in/yaml.v2"
)

// Drop is one entry in a mob's Drops / MvpDrops list. Only the fields useful to
// this app are captured; the rest of the rAthena drop schema is ignored.
type Drop struct {
	Item string `yaml:"Item"`
	Rate int    `yaml:"Rate"`
}

// Entry is one row in a mob_db YAML file. Only the commonly-used scalar fields
// are modeled directly; Drops/MvpDrops are kept as structured lists and stored
// as JSON, the same way itemdb stores Jobs/Locations.
type Entry struct {
	ID              int             `yaml:"Id"`
	AegisName       string          `yaml:"AegisName"`
	Name            string          `yaml:"Name"`
	Level           *int64          `yaml:"Level"`
	HP              *int64          `yaml:"Hp"`
	SP              *int64          `yaml:"Sp"`
	BaseExp         *int64          `yaml:"BaseExp"`
	JobExp          *int64          `yaml:"JobExp"`
	MvpExp          *int64          `yaml:"MvpExp"`
	Attack          *int64          `yaml:"Attack"`
	Attack2         *int64          `yaml:"Attack2"`
	Defense         *int64          `yaml:"Defense"`
	MagicDefense    *int64          `yaml:"MagicDefense"`
	Str             *int64          `yaml:"Str"`
	Agi             *int64          `yaml:"Agi"`
	Vit             *int64          `yaml:"Vit"`
	Int             *int64          `yaml:"Int"`
	Dex             *int64          `yaml:"Dex"`
	Luk             *int64          `yaml:"Luk"`
	AttackRange     *int64          `yaml:"AttackRange"`
	SkillRange      *int64          `yaml:"SkillRange"`
	ChaseRange      *int64          `yaml:"ChaseRange"`
	Size            string          `yaml:"Size"`
	Race            string          `yaml:"Race"`
	Element         string          `yaml:"Element"`
	ElementLevel    *int64          `yaml:"ElementLevel"`
	WalkSpeed       *int64          `yaml:"WalkSpeed"`
	AttackDelay     *int64          `yaml:"AttackDelay"`
	AttackMotion    *int64          `yaml:"AttackMotion"`
	DamageMotion    *int64          `yaml:"DamageMotion"`
	AI              string          `yaml:"Ai"`
	Modes           map[string]bool `yaml:"Modes"`
	Mvp             bool            `yaml:"Mvp"`
	Drops           []Drop          `yaml:"Drops"`
	MvpDrops        []Drop          `yaml:"MvpDrops"`
}

// File is the top-level structure of a mob_db YAML file.
type File struct {
	Header map[string]any `yaml:"Header"`
	Body   []Entry        `yaml:"Body"`
}

// DefaultFiles is the list of YAML files this app expects under seed/.
var DefaultFiles = []string{
	"seed/mob_db.yml",
}

// Populate reads filenames from seedFS, parses them as mob_db YAML, and upserts
// the rows into internal_mob_db on db. Missing or malformed files are logged
// and skipped rather than treated as fatal — the rest of the app degrades
// gracefully when monsters are absent.
func Populate(db *sql.DB, seedFS fs.FS, filenames []string) error {
	var all []Entry
	for _, fn := range filenames {
		log.Printf("[D] [MobDB] Parsing file: %s", fn)
		data, err := fs.ReadFile(seedFS, fn)
		if err != nil {
			log.Printf("[W] [MobDB] Could not read file %s: %v. Skipping.", fn, err)
			continue
		}
		var f File
		if err := yaml.Unmarshal(data, &f); err != nil {
			log.Printf("[W] [MobDB] Could not parse YAML from %s: %v. Skipping.", fn, err)
			continue
		}
		if len(f.Body) > 0 {
			all = append(all, f.Body...)
			log.Printf("[D] [MobDB] Found %d mobs in %s.", len(f.Body), fn)
		}
	}
	if len(all) == 0 {
		log.Println("[W] [MobDB] No mobs found in any YAML files. Database will not be populated.")
		return nil
	}
	log.Printf("[I] [MobDB] Total mobs to process from all files: %d.", len(all))
	return store(db, all)
}

func store(db *sql.DB, mobs []Entry) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO internal_mob_db (
			mob_id, aegis_name, name, name_pt, level, hp, sp,
			base_exp, job_exp, mvp_exp, attack, attack2, defense, magic_defense,
			str, agi, vit, int, dex, luk,
			attack_range, skill_range, chase_range,
			size, race, element, element_level,
			walk_speed, attack_delay, attack_motion, damage_motion,
			ai, modes, is_mvp, drops, mvp_drops
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var success int
	for _, mob := range mobs {
		var namePT sql.NullString
		modesJSON, err := json.Marshal(mob.Modes)
		if err != nil {
			log.Printf("[W] [MobDB] Could not marshal Modes for mob %d: %v", mob.ID, err)
			modesJSON = []byte("{}")
		}
		dropsJSON, err := json.Marshal(mob.Drops)
		if err != nil {
			log.Printf("[W] [MobDB] Could not marshal Drops for mob %d: %v", mob.ID, err)
			dropsJSON = []byte("[]")
		}
		mvpDropsJSON, err := json.Marshal(mob.MvpDrops)
		if err != nil {
			log.Printf("[W] [MobDB] Could not marshal MvpDrops for mob %d: %v", mob.ID, err)
			mvpDropsJSON = []byte("[]")
		}

		res, err := stmt.Exec(
			mob.ID, mob.AegisName, mob.Name, namePT,
			toNullInt64(mob.Level), toNullInt64(mob.HP), toNullInt64(mob.SP),
			toNullInt64(mob.BaseExp), toNullInt64(mob.JobExp), toNullInt64(mob.MvpExp),
			toNullInt64(mob.Attack), toNullInt64(mob.Attack2),
			toNullInt64(mob.Defense), toNullInt64(mob.MagicDefense),
			toNullInt64(mob.Str), toNullInt64(mob.Agi), toNullInt64(mob.Vit),
			toNullInt64(mob.Int), toNullInt64(mob.Dex), toNullInt64(mob.Luk),
			toNullInt64(mob.AttackRange), toNullInt64(mob.SkillRange), toNullInt64(mob.ChaseRange),
			mob.Size, mob.Race, mob.Element, toNullInt64(mob.ElementLevel),
			toNullInt64(mob.WalkSpeed), toNullInt64(mob.AttackDelay),
			toNullInt64(mob.AttackMotion), toNullInt64(mob.DamageMotion),
			mob.AI, string(modesJSON), mob.Mvp, string(dropsJSON), string(mvpDropsJSON),
		)
		if err != nil {
			log.Printf("[W] [MobDB] Failed to insert mob %d (%s): %v", mob.ID, mob.Name, err)
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			success++
		}
	}
	log.Printf("[I] [MobDB] Successfully inserted %d new mobs (skipped duplicates).", success)
	return tx.Commit()
}

func toNullInt64(p *int64) sql.NullInt64 {
	if p == nil {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: *p, Valid: true}
}
