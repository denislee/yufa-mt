package server

import (
	"encoding/json"
	"log"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// whereis_parser.go parses the reply to the GM atcommand `@whereis <id>`, which
// (like @mobinfo) the server prints to the issuing client as a burst of 0x008e
// self-chat lines (already captured by startChatPacketCapture). The rAthena
// format is:
//
//	<mob display name> spawns in:          (msg 1289 — "%s spawns in:", localized)
//	prt_fild08 (10)                        ("%s (%d)" — map index name + spawn count)
//	moc_fild11 (5)
//	…
//
// or, for a mob that never spawns in the wild:
//
//	<mob display name> spawns in:
//	This monster does not spawn normally.  (msg 1290, localized)
//
// Only the per-map "<map> (<qty>)" lines are language-independent (a hardcoded
// printf in rAthena); the header and the "does not spawn" line are translated
// (this server speaks Portuguese). The reply also carries no mob id, so unlike
// @mobinfo we can't read the subject off the wire. Instead the sweep driver
// (whereisscrape.go) calls expect(id) right before injecting each `@whereis
// <id>`, arming the accumulator with the id it is about to ask for; every line
// captured while armed belongs to that mob, and the next expect()/disarm() or
// the staleness flush finalizes the block. This keeps the parser oblivious to
// the locale-specific header text.
//
// Spawn locations are written to mob_spawn_db (live values) — the bestiary's
// stat/drop tables come from a different command, so this stays a separate table.

const whereIsStaleAfter = 2 * time.Second

// reSpawnLine matches one "<map index name> (<count>)" spawn line. Map index
// names are lowercase alphanumerics with underscores, plus the "N@instance"
// form for instanced maps (e.g. "1@cata"). Leading/trailing space is tolerated.
var reSpawnLine = regexp.MustCompile(`^\s*([0-9A-Za-z_@]+)\s+\((\d+)\)\s*$`)

// mobSpawnLoc is one spawn location: the map index name and the spawn count
// there, exactly as @whereis reports them.
type mobSpawnLoc struct {
	Map string `json:"map"`
	Qty int    `json:"qty"`
}

// spawnBlock accumulates the spawn lines of one @whereis reply. sawMaps flips
// once the first map line lands, so a non-map line after the maps cleanly ends
// the block (while a non-map line before any map — the header or the "does not
// spawn" notice — is swallowed without ending it).
type spawnBlock struct {
	id        int
	spawns    []mobSpawnLoc
	sawMaps   bool
	linesSeen int // reply lines consumed while armed (maps + header/notice)
}

// whereIsAccumulator is the single in-flight @whereis block parser. It is only
// "armed" (cur != nil) between an expect() and the matching finalize, i.e. while
// a sweep is actively waiting on a specific id's reply — so it never claims
// stray chat outside a sweep.
type whereIsAccumulator struct {
	mu     sync.Mutex
	cur    *spawnBlock
	lastAt time.Time
}

// whereIs is the package-level accumulator used by the capture loop.
var whereIs whereIsAccumulator

// expect arms the accumulator for the given mob id, finalizing any block still
// open from the previous id. The sweep calls this immediately before injecting
// "@whereis <id>".
func (a *whereIsAccumulator) expect(id int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.finalizeLocked()
	a.cur = &spawnBlock{id: id}
	a.lastAt = time.Now()
}

// disarm finalizes any open block and clears the armed state. The sweep calls
// this when it stops (normally or on cancel) so no later chat is mistaken for a
// spawn reply.
func (a *whereIsAccumulator) disarm() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.finalizeLocked()
}

// handleLine feeds one decoded 0x008e message to the accumulator while armed. It
// returns true if the line was consumed as part of a @whereis reply (so the
// caller skips storing it as ordinary chat), false otherwise.
func (a *whereIsAccumulator) handleLine(msg string, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cur == nil {
		return false // not armed → not ours
	}

	if m := reSpawnLine.FindStringSubmatch(msg); m != nil {
		qty, _ := strconv.Atoi(m[2])
		a.cur.spawns = append(a.cur.spawns, mobSpawnLoc{Map: m[1], Qty: qty})
		a.cur.sawMaps = true
		a.cur.linesSeen++
		a.lastAt = now
		if enableChatScraperDebugLogs {
			log.Printf("[D] [Scraper/WhereIs] mob %d: spawn line matched (map=%s qty=%d): %q", a.cur.id, m[1], qty, msg)
		}
		return true
	}

	// A non-map line before any map line is the localized header ("… spawns
	// in:") or the "does not spawn normally" notice — swallow it but keep the
	// block open for the maps that may follow.
	if !a.cur.sawMaps {
		a.cur.linesSeen++
		a.lastAt = now
		if enableChatScraperDebugLogs {
			log.Printf("[D] [Scraper/WhereIs] mob %d: consumed non-map reply line (header/notice): %q", a.cur.id, msg)
		}
		return true
	}

	// A non-map line after the map list ends the reply: finalize and let the
	// caller process this line as normal chat.
	if enableChatScraperDebugLogs {
		log.Printf("[D] [Scraper/WhereIs] mob %d: non-map line ends spawn block, releasing to chat: %q", a.cur.id, msg)
	}
	a.finalizeLocked()
	return false
}

// flushIfStale finalizes a dangling block whose last line is older than
// whereIsStaleAfter. Called from the capture loop's periodic flush ticker so the
// last id of a sweep (with no following expect) still gets written.
func (a *whereIsAccumulator) flushIfStale(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cur != nil && now.Sub(a.lastAt) >= whereIsStaleAfter {
		a.finalizeLocked()
	}
}

// finalizeLocked upserts the current block (if any) and clears it. Caller holds
// a.mu. A block with no map lines is still written (empty spawns) so the detail
// page can distinguish "scraped, does not spawn" from "never scraped".
func (a *whereIsAccumulator) finalizeLocked() {
	b := a.cur
	a.cur = nil
	if b == nil || b.id == 0 {
		return
	}
	if err := upsertMobSpawns(b); err != nil {
		log.Printf("[E] [Scraper/WhereIs] Failed to upsert spawns for mob %d (lines=%d, maps=%d): %v", b.id, b.linesSeen, len(b.spawns), err)
		return
	}
	switch {
	case b.linesSeen == 0:
		// Armed for this id but the burst produced no lines at all. The likely
		// causes: the server has no @whereis command, its reply isn't a 0x008e
		// self-chat packet (so the capture loop never routed it here), or the
		// reply arrived after the staleness window finalized the block. Flagged
		// loudly because it means the scrape captured nothing for this mob.
		log.Printf("[W] [Scraper/WhereIs] mob %d: armed but captured 0 reply lines — @whereis may be unsupported, not a 0x008e self-chat packet, or slower than the %v staleness window. Stored an empty spawn list.", b.id, whereIsStaleAfter)
	case len(b.spawns) == 0:
		// Saw the header (and/or the "does not spawn" notice) but no map lines.
		log.Printf("[I] [Scraper/WhereIs] Captured @whereis for mob %d: 0 spawn maps from %d reply line(s) — does not spawn in the wild.", b.id, b.linesSeen)
	default:
		log.Printf("[I] [Scraper/WhereIs] Captured @whereis for mob %d: %d spawn map(s) from %d reply line(s).", b.id, len(b.spawns), b.linesSeen)
	}
}

// upsertMobSpawns writes a parsed block into mob_spawn_db, stamping scraped_at.
func upsertMobSpawns(b *spawnBlock) error {
	if srv == nil || srv.db == nil {
		return nil
	}
	spawns := b.spawns
	if spawns == nil {
		spawns = []mobSpawnLoc{}
	}
	spawnsJSON, err := json.Marshal(spawns)
	if err != nil {
		spawnsJSON = []byte("[]")
	}
	now := time.Now().Format(time.RFC3339)
	_, err = srv.db.Exec(`
		INSERT INTO mob_spawn_db (mob_id, spawns, scraped_at) VALUES (?, ?, ?)
		ON CONFLICT(mob_id) DO UPDATE SET spawns=excluded.spawns, scraped_at=excluded.scraped_at`,
		b.id, string(spawnsJSON), now,
	)
	return err
}
