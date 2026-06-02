package server

import (
	"database/sql"
	"encoding/json"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file parses the reply to the GM atcommand `@mobinfo <id>`, which the
// server prints to the issuing client as a burst of self-chat lines carried in
// 0x008e packets (already captured by startChatPacketCapture). The captured
// format on this server (Portuguese labels, Latin-1 text) is:
//
//	Monstro: 'Escorpião'/'Escorpião'/'SCORPION' (1001)
//	Nv:24  HP:1109  EXP Base:287  EXP Classe:176  HIT:76  ESQV:48
//	DEF:30  DEFM:0  FOR:1  AGI:24  VIT:24  INT:5  DES:52  SOR:5
//	ATQ:80~135  Alcance:1~10~12  Tamanho:Small  Raça: Insect  Elemento: Fire (Nv:1)
//	Drops:
//	  - <ITEML>…</ITEML>  0.70% - <ITEML>…</ITEML>  55.00% - <ITEML>…</ITEML>  0.57%
//	  …
//
// The lines stream in across several packets with no explicit terminator, so we
// accumulate into a block, finalize it when the next `Monstro:` header arrives,
// when a non-mobinfo line breaks the run, or on a staleness timer driven by the
// capture loop's flush ticker. Values from @mobinfo are authoritative (they are
// what the running, modified server actually has loaded), so finalize OVERWRITES
// the matching internal_mob_db row rather than INSERT-OR-IGNORE like the YAML seed.

var (
	// mobInfoHeaderRe matches the leading header for both normal and MVP mobs:
	//   "Monstro: 'name'/'jname'/'AEGIS' (id)"
	//   "Monstro MVP: 'name'/'jname'/'AEGIS' (id)"
	// The three quoted names are display/locale/sprite; the third is the AegisName.
	// Group 1 is the optional " MVP" marker.
	mobInfoHeaderRe = regexp.MustCompile(`^Monstro( MVP)?:\s*'(.*?)'/'(.*?)'/'(.*?)'\s*\((\d+)\)`)

	// Stat field extractors. Spacing around the colon varies ("Raça: Insect"
	// vs "DEF:30"), so each is matched independently rather than tokenized.
	// Anchored to line-start/space so it matches the "Nv:24" stat field but NOT
	// the "(Nv:1)" element level on the ATQ/Elemento line.
	reNv       = regexp.MustCompile(`(?:^|\s)Nv:(\d+)`)
	reHP       = regexp.MustCompile(`\bHP:(\d+)`)
	reExpBase  = regexp.MustCompile(`EXP Base:(\d+)`)
	reExpClass = regexp.MustCompile(`EXP Classe:(\d+)`)
	reDEF      = regexp.MustCompile(`\bDEF:(\d+)`)
	reDEFM     = regexp.MustCompile(`\bDEFM:(\d+)`)
	reFOR      = regexp.MustCompile(`\bFOR:(\d+)`)
	reAGI      = regexp.MustCompile(`\bAGI:(\d+)`)
	reVIT      = regexp.MustCompile(`\bVIT:(\d+)`)
	reINT      = regexp.MustCompile(`\bINT:(\d+)`)
	reDES      = regexp.MustCompile(`\bDES:(\d+)`)
	reSOR      = regexp.MustCompile(`\bSOR:(\d+)`)
	reATQ      = regexp.MustCompile(`ATQ:(\d+)~(\d+)`)
	reAlcance  = regexp.MustCompile(`Alcance:(\d+)~(\d+)~(\d+)`)
	reTamanho  = regexp.MustCompile(`Tamanho:\s*(\S+)`)
	reRaca     = regexp.MustCompile(`Ra[çc]a:\s*(\S+)`)
	reElemento = regexp.MustCompile(`Elemento:\s*(\S+)\s*\(Nv:(\d+)\)`)

	// reMvpExp matches the MVP bonus-exp line "EXP Bônus MVP:53625". Only this
	// line has "MVP:" immediately followed by digits (the header has "MVP: '"
	// and the items line has "MVP: <ITEML"), so the pattern is unambiguous.
	reMvpExp = regexp.MustCompile(`MVP:(\d+)`)

	// reDropEntry pulls each "<ITEML>blob</ITEML>  12.34%" pair off a drops line.
	// An optional "[N]" slot marker can sit between the tag and the rate
	// (e.g. "</ITEML> [0]  2.00%"), so allow and skip it.
	reDropEntry = regexp.MustCompile(`<ITEML>([^<]*)</ITEML>\s*(?:\[\d+\]\s*)?([\d.]+)%`)
)

// mobInfoStaleAfter is how long a partially-collected block waits with no new
// lines before the flush ticker finalizes it (the last block of a burst has no
// following header to trigger it).
const mobInfoStaleAfter = 2 * time.Second

// mobDrop is one parsed drop: the decoded item id and its percentage chance.
type mobDrop struct {
	ItemID int64   `json:"item_id"`
	Rate   float64 `json:"rate_pct"`
}

// mobBlock accumulates the fields parsed from one @mobinfo reply.
type mobBlock struct {
	id        int
	name      string
	aegisName string

	level, hp                            sql.NullInt64
	baseExp, jobExp                      sql.NullInt64
	atkMin, atkMax                       sql.NullInt64
	def, mdef                            sql.NullInt64
	str, agi, vit, intl, dex, luk        sql.NullInt64
	atkRange, skillRange, chaseRange     sql.NullInt64
	size, race, element                  string
	elementLevel                         sql.NullInt64

	isMvp    bool
	mvpExp   sql.NullInt64
	drops    []mobDrop
	mvpDrops []mobDrop

	// mvpMode becomes true once the "Itens MVP:" section starts, so subsequent
	// <ITEML> entries are routed to mvpDrops instead of drops.
	mvpMode bool
}

// mobInfoAccumulator is the single in-flight block parser, guarded by its mutex
// because the capture loop calls handleLine from one goroutine and flushIfStale
// from the ticker branch of the same select — but the mutex keeps it safe if
// that ever changes.
type mobInfoAccumulator struct {
	mu     sync.Mutex
	cur    *mobBlock
	lastAt time.Time
}

// mobInfo is the package-level accumulator used by the capture loop.
var mobInfo mobInfoAccumulator

// handleLine feeds one decoded 0x008e message to the accumulator. It returns
// true if the line belonged to a @mobinfo reply (so the caller skips storing it
// as ordinary chat), or false if the line is unrelated and should be handled
// normally. now is the packet's arrival time, used for staleness.
func (a *mobInfoAccumulator) handleLine(msg string, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if m := mobInfoHeaderRe.FindStringSubmatch(msg); m != nil {
		// A new header ends any block already in progress.
		a.finalizeLocked()
		id, _ := strconv.Atoi(m[5])
		a.cur = &mobBlock{id: id, name: m[2], aegisName: m[4], isMvp: m[1] != ""}
		a.lastAt = now
		return true
	}

	if a.cur == nil {
		return false // not in a block and not a header → not ours
	}

	if isMobInfoContinuation(msg) {
		a.cur.apply(msg)
		a.lastAt = now
		return true
	}

	// An unrecognized line breaks the run: finalize what we have and let the
	// caller process this line as normal chat.
	a.finalizeLocked()
	return false
}

// flushIfStale finalizes a dangling block whose last line is older than
// mobInfoStaleAfter. Called from the capture loop's periodic flush ticker.
func (a *mobInfoAccumulator) flushIfStale(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cur != nil && now.Sub(a.lastAt) >= mobInfoStaleAfter {
		a.finalizeLocked()
	}
}

// isMobInfoContinuation reports whether a line looks like part of a @mobinfo
// reply body (stats, the Drops: header, or a drops line). Kept strict so an
// interleaved real chat message ends the block instead of being swallowed.
func isMobInfoContinuation(msg string) bool {
	switch {
	case reNv.MatchString(msg) && reHP.MatchString(msg):
		return true
	case reDEF.MatchString(msg) && reFOR.MatchString(msg):
		return true
	case reATQ.MatchString(msg) && reTamanho.MatchString(msg):
		return true
	case strings.HasPrefix(strings.TrimSpace(msg), "Drops:"):
		return true
	case strings.Contains(msg, "<ITEML>"):
		return true
	case reMvpExp.MatchString(msg):
		// "EXP Bônus MVP:<n>" line of an MVP block.
		return true
	}
	return false
}

// apply extracts whatever fields the given line carries into the block.
func (b *mobBlock) apply(msg string) {
	setI := func(dst *sql.NullInt64, re *regexp.Regexp) {
		if m := re.FindStringSubmatch(msg); m != nil {
			if v, err := strconv.ParseInt(m[1], 10, 64); err == nil {
				*dst = sql.NullInt64{Int64: v, Valid: true}
			}
		}
	}
	setI(&b.level, reNv)
	setI(&b.hp, reHP)
	setI(&b.baseExp, reExpBase)
	setI(&b.jobExp, reExpClass)
	setI(&b.def, reDEF)
	setI(&b.mdef, reDEFM)
	setI(&b.str, reFOR)
	setI(&b.agi, reAGI)
	setI(&b.vit, reVIT)
	setI(&b.intl, reINT)
	setI(&b.dex, reDES)
	setI(&b.luk, reSOR)

	if m := reATQ.FindStringSubmatch(msg); m != nil {
		b.atkMin = parseNullInt(m[1])
		b.atkMax = parseNullInt(m[2])
	}
	if m := reAlcance.FindStringSubmatch(msg); m != nil {
		b.atkRange = parseNullInt(m[1])
		b.skillRange = parseNullInt(m[2])
		b.chaseRange = parseNullInt(m[3])
	}
	if m := reTamanho.FindStringSubmatch(msg); m != nil {
		b.size = m[1]
	}
	if m := reRaca.FindStringSubmatch(msg); m != nil {
		b.race = m[1]
	}
	if m := reElemento.FindStringSubmatch(msg); m != nil {
		b.element = m[1]
		b.elementLevel = parseNullInt(m[2])
	}

	// "Itens MVP:" begins the MVP-reward section; route its (and any following)
	// <ITEML> entries to mvpDrops. "EXP Bônus MVP:<n>" carries the MVP bonus exp.
	if strings.Contains(msg, "Itens MVP") {
		b.mvpMode = true
	} else if m := reMvpExp.FindStringSubmatch(msg); m != nil {
		b.mvpExp = parseNullInt(m[1])
	}

	for _, m := range reDropEntry.FindAllStringSubmatch(msg, -1) {
		d := decodeItemLink(m[1])
		if !d.ok {
			continue
		}
		rate, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			continue
		}
		drop := mobDrop{ItemID: d.itemID, Rate: rate}
		if b.mvpMode {
			b.mvpDrops = append(b.mvpDrops, drop)
		} else {
			b.drops = append(b.drops, drop)
		}
	}
}

func parseNullInt(s string) sql.NullInt64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: v, Valid: true}
}

// finalizeLocked upserts the current block (if any) and clears it. Caller holds
// a.mu.
func (a *mobInfoAccumulator) finalizeLocked() {
	b := a.cur
	a.cur = nil
	if b == nil || b.id == 0 {
		return
	}
	if err := upsertMobInfo(b); err != nil {
		log.Printf("[E] [Scraper/MobInfo] Failed to upsert mob %d (%s): %v", b.id, b.name, err)
		return
	}
	log.Printf("[I] [Scraper/MobInfo] Captured @mobinfo for %s (%d): %d drops.", b.name, b.id, len(b.drops))
}

// upsertMobInfo writes a parsed block into internal_mob_db, overwriting the
// live-authoritative columns while leaving seed-only columns (sp, jobs, etc.)
// untouched on conflict.
func upsertMobInfo(b *mobBlock) error {
	if srv == nil || srv.db == nil {
		return nil
	}
	dropsJSON, err := json.Marshal(b.drops)
	if err != nil {
		dropsJSON = []byte("[]")
	}
	mvpDropsJSON, err := json.Marshal(b.mvpDrops)
	if err != nil {
		mvpDropsJSON = []byte("[]")
	}
	_, err = srv.db.Exec(`
		INSERT INTO internal_mob_db (
			mob_id, aegis_name, name, level, hp, base_exp, job_exp,
			attack, attack2, defense, magic_defense,
			str, agi, vit, int, dex, luk,
			attack_range, skill_range, chase_range,
			size, race, element, element_level,
			is_mvp, mvp_exp, drops, mvp_drops
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(mob_id) DO UPDATE SET
			aegis_name=excluded.aegis_name, name=excluded.name,
			level=excluded.level, hp=excluded.hp,
			base_exp=excluded.base_exp, job_exp=excluded.job_exp,
			attack=excluded.attack, attack2=excluded.attack2,
			defense=excluded.defense, magic_defense=excluded.magic_defense,
			str=excluded.str, agi=excluded.agi, vit=excluded.vit,
			int=excluded.int, dex=excluded.dex, luk=excluded.luk,
			attack_range=excluded.attack_range, skill_range=excluded.skill_range,
			chase_range=excluded.chase_range, size=excluded.size,
			race=excluded.race, element=excluded.element,
			element_level=excluded.element_level,
			is_mvp=excluded.is_mvp, mvp_exp=excluded.mvp_exp,
			drops=excluded.drops, mvp_drops=excluded.mvp_drops
	`,
		b.id, b.aegisName, b.name, b.level, b.hp, b.baseExp, b.jobExp,
		b.atkMin, b.atkMax, b.def, b.mdef,
		b.str, b.agi, b.vit, b.intl, b.dex, b.luk,
		b.atkRange, b.skillRange, b.chaseRange,
		b.size, b.race, b.element, b.elementLevel,
		b.isMvp, b.mvpExp, string(dropsJSON), string(mvpDropsJSON),
	)
	return err
}
