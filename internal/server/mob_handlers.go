package server

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/denislee/yufa-mt/internal/httpx"
	"github.com/denislee/yufa-mt/internal/i18n"
)

// mob_handlers.go serves the user-facing bestiary backed by internal_mob_db
// (seeded from YAML and kept authoritative by the @mobinfo scrape): a
// searchable, paginated mob list at /mobs and a per-mob detail page at
// /mob?id=N showing stats and drops (each drop links to its item page).

const mobsPerPage = 50

// MobListEntry is one row in the /mobs table.
type MobListEntry struct {
	ID           int
	DisplayName  string
	AegisName    string
	Level        int
	HP           int
	Race         string
	Element      string
	ElementLevel int
	Size         string
	IsMvp        bool
}

// MobDropView is one drop on the detail page, resolved to a display name.
type MobDropView struct {
	ItemID  int64
	Name    string // resolved display name, or an aegis/placeholder fallback
	Known   bool   // true when Name links to an item page
	RatePct float64
}

// MobDetail is the full per-mob view at /mob.
type MobDetail struct {
	ID          int
	DisplayName string
	Name        string
	AegisName   string
	Level       int
	HP          int
	SP          int
	BaseExp     int
	JobExp      int
	MvpExp      int
	AtkMin      int
	AtkMax      int
	Def         int
	Mdef        int
	Str         int
	Agi         int
	Vit         int
	Int         int
	Dex         int
	Luk         int
	AtkRange    int
	SkillRange  int
	ChaseRange  int
	Size        string
	Race        string
	Element     string
	ElementLvl  int
	IsMvp       bool
	Drops       []MobDropView
	MvpDrops    []MobDropView
}

// MobDiffEntry is one row in the "Differences" tab: a scraped mob whose live
// server stats diverge from the YAML baseline on more than one field.
type MobDiffEntry struct {
	ID          int
	DisplayName string
	IsMvp       bool
	DiffCount   int
	ScrapedAgo  string
	DiffFields  []MobFieldDiff // only the fields that actually differ
}

type MobsPageData struct {
	Mobs        []MobListEntry
	SearchQuery string
	TotalMobs   int
	Pagination  httpx.PaginationData
	Filter      string // query-string suffix preserving the search + sort across pages
	SortBy      string // active sort column key
	Order       string // active sort direction ("ASC"/"DESC")
	PageTitle   string
	Tab         string         // "" (all monsters) or "diffs" (the Differences tab)
	Diffs       []MobDiffEntry // populated only on the Differences tab
}

// MobFieldDiff is one row of the baseline-vs-server comparison table.
type MobFieldDiff struct {
	Label    string
	Baseline string
	Server   string
	Differs  bool
}

// MobDropDiff is one row of the baseline-vs-server drop comparison: a single
// item, with its baseline and live-server drop chances side by side. An item
// present on only one side has HasBaseline or HasServer cleared.
type MobDropDiff struct {
	ItemID      int64
	Name        string  // resolved display name (or aegis/placeholder fallback)
	Known       bool    // true when Name links to an item page
	Baseline    float64 // baseline (YAML) drop chance, valid when HasBaseline
	HasBaseline bool
	Server      float64 // live-server drop chance, valid when HasServer
	HasServer   bool
	Differs     bool // rate differs, or the item is on only one side
}

// MobComparison holds the difference between the YAML baseline and the live
// server values (from @mobinfo), shown only when the mob has been scraped.
type MobComparison struct {
	HasServer      bool
	ScrapedAgo     string
	DiffCount      int
	Fields         []MobFieldDiff
	ServerDrops    []MobDropView
	ServerMvpDrops []MobDropView
	DropDiffs      []MobDropDiff // baseline-vs-server, regular drops
	DropDiffCount  int           // number of DropDiffs rows that differ
	MvpDropDiffs   []MobDropDiff // baseline-vs-server, MVP rewards
	MvpDiffCount   int           // number of MvpDropDiffs rows that differ
}

// MobSpawnView is one spawn location on the detail page: the map index name and
// the number of that mob spawned there, as reported by the live-server @whereis
// scrape.
type MobSpawnView struct {
	Map string
	Qty int
}

type MobDetailPageData struct {
	Mob           MobDetail
	Comparison    MobComparison
	PageTitle     string
	Spawns        []MobSpawnView // live-server spawn locations, count-descending
	SpawnsScraped bool           // a mob_spawn_db row exists (even if it has no spawns)
	SpawnsAgo     string         // humanized scraped_at, when SpawnsScraped
}

// ni unwraps a nullable int to a plain int (0 when NULL).
func ni(n sql.NullInt64) int { return int(n.Int64) }

// mobDisplayName picks the localized name, falling back to the English name.
func mobDisplayName(name, namePT, lang string) string {
	if lang == "pt" && namePT != "" {
		return namePT
	}
	if name != "" {
		return name
	}
	return namePT
}

func mobsListHandler(w http.ResponseWriter, r *http.Request) {
	lang := i18n.Lang(r)

	// The "Differences" tab lists scraped mobs whose live server stats diverge
	// from the YAML baseline on more than one field.
	if r.FormValue("tab") == "diffs" {
		diffs, err := loadMobDiffs(lang)
		if err != nil {
			log.Printf("[E] [HTTP/Mobs] diff query failed: %v", err)
			http.Error(w, "Could not query mobs", http.StatusInternalServerError)
			return
		}
		renderTemplate(w, r, "mobs.html", MobsPageData{
			Tab:       "diffs",
			Diffs:     diffs,
			TotalMobs: len(diffs),
			PageTitle: "Bestiary",
		})
		return
	}

	q := strings.TrimSpace(r.FormValue("q"))

	// Build the optional search WHERE clause. Matches name / pt name / aegis,
	// plus an exact id match when the query is numeric.
	where := ""
	var args []any
	if q != "" {
		like := "%" + q + "%"
		clauses := []string{"name LIKE ?", "name_pt LIKE ?", "aegis_name LIKE ?"}
		args = append(args, like, like, like)
		if id, err := strconv.Atoi(q); err == nil {
			clauses = append(clauses, "mob_id = ?")
			args = append(args, id)
		}
		where = "WHERE " + strings.Join(clauses, " OR ")
	}

	var total int
	if err := srv.db.QueryRow("SELECT COUNT(*) FROM internal_mob_db "+where, args...).Scan(&total); err != nil {
		log.Printf("[E] [HTTP/Mobs] count failed: %v", err)
		http.Error(w, "Could not query mobs", http.StatusInternalServerError)
		return
	}

	pg := httpx.NewPaginationData(r, total, mobsPerPage)

	// Every listing column is sortable; mob_id is the stable tiebreak. Text
	// columns default ASC and numeric columns DESC via the per-column links in
	// the template; the handler default keeps the historical mob_id ASC order.
	allowedSorts := map[string]string{
		"id":      "mob_id",
		"name":    "name",
		"level":   "level",
		"hp":      "hp",
		"race":    "race",
		"element": "element",
		"size":    "size",
	}
	orderByClause, sortBy, order := httpx.GetSortClause(r, allowedSorts, "id", "ASC")

	query := "SELECT mob_id, name, COALESCE(name_pt,''), aegis_name, level, hp, race, element, element_level, size, is_mvp " +
		"FROM internal_mob_db " + where + " " + orderByClause + ", mob_id LIMIT ? OFFSET ?"
	rows, err := srv.db.Query(query, append(args, mobsPerPage, pg.Offset)...)
	if err != nil {
		log.Printf("[E] [HTTP/Mobs] query failed: %v", err)
		http.Error(w, "Could not query mobs", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var mobs []MobListEntry
	for rows.Next() {
		var (
			e                     MobListEntry
			name, namePT          string
			level, hp, elementLvl sql.NullInt64
			isMvp                 sql.NullInt64
		)
		if err := rows.Scan(&e.ID, &name, &namePT, &e.AegisName, &level, &hp,
			&e.Race, &e.Element, &elementLvl, &e.Size, &isMvp); err != nil {
			log.Printf("[W] [HTTP/Mobs] scan failed: %v", err)
			continue
		}
		e.DisplayName = mobDisplayName(name, namePT, lang)
		e.Level, e.HP, e.ElementLevel = ni(level), ni(hp), ni(elementLvl)
		e.IsMvp = isMvp.Int64 != 0
		mobs = append(mobs, e)
	}

	// Preserve the search query and the active sort across pagination links.
	filterValues := url.Values{}
	if q != "" {
		filterValues.Set("q", q)
	}
	filterValues.Set("sort_by", sortBy)
	filterValues.Set("order", order)
	filter := ""
	if enc := filterValues.Encode(); enc != "" {
		filter = "&" + enc
	}

	renderTemplate(w, r, "mobs.html", MobsPageData{
		Mobs:        mobs,
		SearchQuery: q,
		TotalMobs:   total,
		Pagination:  pg,
		Filter:      filter,
		SortBy:      sortBy,
		Order:       order,
		PageTitle:   "Bestiary",
	})
}

func mobDetailHandler(w http.ResponseWriter, r *http.Request) {
	lang := i18n.Lang(r)
	id, err := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
	if err != nil || id <= 0 {
		http.Redirect(w, r, "/mobs", http.StatusSeeOther)
		return
	}

	var (
		m                                      MobDetail
		name, namePT                           string
		level, hp, sp, baseExp, jobExp, mvpExp sql.NullInt64
		atk, atk2, def, mdef                   sql.NullInt64
		st, ag, vi, in, dx, lk                 sql.NullInt64
		atkR, skR, chR, elemLvl, isMvp         sql.NullInt64
		dropsJSON, mvpDropsJSON                string
	)
	err = srv.db.QueryRow(`
		SELECT mob_id, name, COALESCE(name_pt,''), aegis_name,
			level, hp, sp, base_exp, job_exp, mvp_exp,
			attack, attack2, defense, magic_defense,
			str, agi, vit, int, dex, luk,
			attack_range, skill_range, chase_range,
			size, race, element, element_level, is_mvp,
			COALESCE(drops,'[]'), COALESCE(mvp_drops,'[]')
		FROM internal_mob_db WHERE mob_id = ?`, id).Scan(
		&m.ID, &name, &namePT, &m.AegisName,
		&level, &hp, &sp, &baseExp, &jobExp, &mvpExp,
		&atk, &atk2, &def, &mdef,
		&st, &ag, &vi, &in, &dx, &lk,
		&atkR, &skR, &chR,
		&m.Size, &m.Race, &m.Element, &elemLvl, &isMvp,
		&dropsJSON, &mvpDropsJSON,
	)
	if err == sql.ErrNoRows {
		http.NotFound(w, r)
		return
	} else if err != nil {
		log.Printf("[E] [HTTP/Mob] query failed for id %d: %v", id, err)
		http.Error(w, "Could not query mob", http.StatusInternalServerError)
		return
	}

	m.Name = name
	m.DisplayName = mobDisplayName(name, namePT, lang)
	m.Level, m.HP, m.SP = ni(level), ni(hp), ni(sp)
	m.BaseExp, m.JobExp, m.MvpExp = ni(baseExp), ni(jobExp), ni(mvpExp)
	m.AtkMin, m.AtkMax = ni(atk), ni(atk2)
	m.Def, m.Mdef = ni(def), ni(mdef)
	m.Str, m.Agi, m.Vit, m.Int, m.Dex, m.Luk = ni(st), ni(ag), ni(vi), ni(in), ni(dx), ni(lk)
	m.AtkRange, m.SkillRange, m.ChaseRange = ni(atkR), ni(skR), ni(chR)
	m.ElementLvl = ni(elemLvl)
	m.IsMvp = isMvp.Int64 != 0
	m.Drops = parseMobDrops(dropsJSON, lang)
	m.MvpDrops = parseMobDrops(mvpDropsJSON, lang)

	cmp := loadMobComparison(id, m, lang)
	spawns, spawnsScraped, spawnsAgo := loadMobSpawns(id)

	renderTemplate(w, r, "mob_detail.html", MobDetailPageData{
		Mob:           m,
		Comparison:    cmp,
		PageTitle:     "Bestiary",
		Spawns:        spawns,
		SpawnsScraped: spawnsScraped,
		SpawnsAgo:     spawnsAgo,
	})
}

// loadMobSpawns loads the live-server spawn locations (from the @whereis scrape)
// for a mob, ordered by descending spawn count. scraped reports whether a
// mob_spawn_db row exists at all — false means the @whereis sweep hasn't reached
// this mob yet, which the detail page distinguishes from "scraped, no spawns".
func loadMobSpawns(id int) (spawns []MobSpawnView, scraped bool, ago string) {
	var (
		spawnsJSON string
		scrapedAt  string
	)
	err := srv.db.QueryRow(
		`SELECT COALESCE(spawns,'[]'), COALESCE(scraped_at,'') FROM mob_spawn_db WHERE mob_id = ?`, id,
	).Scan(&spawnsJSON, &scrapedAt)
	if err != nil {
		return nil, false, "" // no row (ErrNoRows) or scan error → not scraped
	}

	var locs []mobSpawnLoc
	if err := json.Unmarshal([]byte(spawnsJSON), &locs); err != nil {
		log.Printf("[W] [HTTP/Mob] bad spawns JSON for mob %d: %v", id, err)
	}
	for _, l := range locs {
		spawns = append(spawns, MobSpawnView{Map: l.Map, Qty: l.Qty})
	}
	sort.SliceStable(spawns, func(i, j int) bool {
		if spawns[i].Qty != spawns[j].Qty {
			return spawns[i].Qty > spawns[j].Qty
		}
		return spawns[i].Map < spawns[j].Map
	})
	return spawns, true, timeAgo(scrapedAt)
}

// mobFieldDiffs builds the baseline-vs-server comparison rows for a mob. The
// MVP bonus EXP row is included only when either side is an MVP. Used both for
// a single mob's detail page and the bulk "Differences" tab.
func mobFieldDiffs(base, s MobDetail) []MobFieldDiff {
	addI := func(label string, a, b int) MobFieldDiff {
		return MobFieldDiff{Label: label, Baseline: strconv.Itoa(a), Server: strconv.Itoa(b), Differs: a != b}
	}
	addS := func(label, a, b string) MobFieldDiff {
		return MobFieldDiff{Label: label, Baseline: a, Server: b, Differs: a != b}
	}
	fields := []MobFieldDiff{
		addI("Level", base.Level, s.Level),
		addI("HP", base.HP, s.HP),
		addI("Base EXP", base.BaseExp, s.BaseExp),
		addI("Job EXP", base.JobExp, s.JobExp),
		addI("ATK min", base.AtkMin, s.AtkMin),
		addI("ATK max", base.AtkMax, s.AtkMax),
		addI("DEF", base.Def, s.Def),
		addI("MDEF", base.Mdef, s.Mdef),
		addI("STR", base.Str, s.Str),
		addI("AGI", base.Agi, s.Agi),
		addI("VIT", base.Vit, s.Vit),
		addI("INT", base.Int, s.Int),
		addI("DEX", base.Dex, s.Dex),
		addI("LUK", base.Luk, s.Luk),
		addI("Atk Range", base.AtkRange, s.AtkRange),
		addI("Skill Range", base.SkillRange, s.SkillRange),
		addI("Chase Range", base.ChaseRange, s.ChaseRange),
		addS("Size", base.Size, s.Size),
		addS("Race", base.Race, s.Race),
		addS("Element", base.Element, s.Element),
		addI("Element Lv", base.ElementLvl, s.ElementLvl),
	}
	if base.IsMvp || s.IsMvp {
		fields = append(fields, addI("MVP Bonus EXP", base.MvpExp, s.MvpExp))
	}
	return fields
}

// mobDropDiffs merges the baseline (YAML) and live-server drop lists into a
// single side-by-side comparison, keyed by item id (falling back to the lower-
// cased display name for items that didn't resolve to an id). Each row flags
// whether the chances differ or the item appears on only one side. Rows are
// ordered most-divergent first, then by descending drop chance. The returned
// count is how many rows actually differ.
func mobDropDiffs(baseline, server []MobDropView) ([]MobDropDiff, int) {
	keyOf := func(d MobDropView) string {
		if d.ItemID > 0 {
			return "id:" + strconv.FormatInt(d.ItemID, 10)
		}
		return "nm:" + strings.ToLower(d.Name)
	}
	// rateDiffers compares two chances at display precision (2 decimals) so
	// float noise from the two different source encodings doesn't show as a diff.
	rateDiffers := func(a, b float64) bool {
		d := a - b
		if d < 0 {
			d = -d
		}
		return d > 0.005
	}

	idx := make(map[string]int, len(baseline)+len(server))
	out := make([]MobDropDiff, 0, len(baseline)+len(server))

	for _, d := range baseline {
		k := keyOf(d)
		idx[k] = len(out)
		out = append(out, MobDropDiff{
			ItemID: d.ItemID, Name: d.Name, Known: d.Known,
			Baseline: d.RatePct, HasBaseline: true,
		})
	}
	for _, d := range server {
		k := keyOf(d)
		if i, ok := idx[k]; ok {
			out[i].Server, out[i].HasServer = d.RatePct, true
			// Prefer a resolvable (linkable) name/id if the server side has one.
			if !out[i].Known && d.Known {
				out[i].Name, out[i].Known, out[i].ItemID = d.Name, true, d.ItemID
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, MobDropDiff{
			ItemID: d.ItemID, Name: d.Name, Known: d.Known,
			Server: d.RatePct, HasServer: true,
		})
	}

	diffCount := 0
	for i := range out {
		r := &out[i]
		r.Differs = !r.HasBaseline || !r.HasServer || rateDiffers(r.Baseline, r.Server)
		if r.Differs {
			diffCount++
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Differs != out[j].Differs {
			return out[i].Differs // differing rows first
		}
		mi := out[i].Server
		if out[i].Baseline > mi {
			mi = out[i].Baseline
		}
		mj := out[j].Server
		if out[j].Baseline > mj {
			mj = out[j].Baseline
		}
		return mi > mj
	})
	return out, diffCount
}

// loadMobComparison loads the live server row (if any) for a mob and diffs it
// against the YAML baseline. Returns HasServer=false when the mob hasn't been
// scraped yet.
func loadMobComparison(id int, base MobDetail, lang string) MobComparison {
	var (
		s                                  MobDetail
		level, hp, baseExp, jobExp, mvpExp sql.NullInt64
		atk, atk2, def, mdef               sql.NullInt64
		st, ag, vi, in, dx, lk             sql.NullInt64
		atkR, skR, chR, elemLvl, isMvp     sql.NullInt64
		dropsJSON, mvpDropsJSON, scrapedAt string
	)
	err := srv.db.QueryRow(`
		SELECT level, hp, base_exp, job_exp, mvp_exp,
			attack, attack2, defense, magic_defense,
			str, agi, vit, int, dex, luk,
			attack_range, skill_range, chase_range,
			size, race, element, element_level, is_mvp,
			COALESCE(drops,'[]'), COALESCE(mvp_drops,'[]'), COALESCE(scraped_at,'')
		FROM mob_server_db WHERE mob_id = ?`, id).Scan(
		&level, &hp, &baseExp, &jobExp, &mvpExp,
		&atk, &atk2, &def, &mdef,
		&st, &ag, &vi, &in, &dx, &lk,
		&atkR, &skR, &chR,
		&s.Size, &s.Race, &s.Element, &elemLvl, &isMvp,
		&dropsJSON, &mvpDropsJSON, &scrapedAt,
	)
	if err != nil {
		return MobComparison{} // no server row (ErrNoRows) or scan error → baseline only
	}

	s.Level, s.HP = ni(level), ni(hp)
	s.BaseExp, s.JobExp, s.MvpExp = ni(baseExp), ni(jobExp), ni(mvpExp)
	s.AtkMin, s.AtkMax, s.Def, s.Mdef = ni(atk), ni(atk2), ni(def), ni(mdef)
	s.Str, s.Agi, s.Vit, s.Int, s.Dex, s.Luk = ni(st), ni(ag), ni(vi), ni(in), ni(dx), ni(lk)
	s.AtkRange, s.SkillRange, s.ChaseRange = ni(atkR), ni(skR), ni(chR)
	s.ElementLvl = ni(elemLvl)
	s.IsMvp = isMvp.Int64 != 0

	fields := mobFieldDiffs(base, s)

	diffCount := 0
	for _, f := range fields {
		if f.Differs {
			diffCount++
		}
	}

	serverDrops := parseMobDrops(dropsJSON, lang)
	serverMvpDrops := parseMobDrops(mvpDropsJSON, lang)
	dropDiffs, dropDiffCount := mobDropDiffs(base.Drops, serverDrops)
	mvpDropDiffs, mvpDiffCount := mobDropDiffs(base.MvpDrops, serverMvpDrops)

	return MobComparison{
		HasServer:      true,
		ScrapedAgo:     timeAgo(scrapedAt),
		DiffCount:      diffCount,
		Fields:         fields,
		ServerDrops:    serverDrops,
		ServerMvpDrops: serverMvpDrops,
		DropDiffs:      dropDiffs,
		DropDiffCount:  dropDiffCount,
		MvpDropDiffs:   mvpDropDiffs,
		MvpDiffCount:   mvpDiffCount,
	}
}

// loadMobDiffs joins the YAML baseline (internal_mob_db) against the live server
// scrape (mob_server_db) for every scraped mob, diffs their stats, and returns
// only those that differ on more than one field — ordered most-divergent first.
func loadMobDiffs(lang string) ([]MobDiffEntry, error) {
	rows, err := srv.db.Query(`
		SELECT b.mob_id, b.name, COALESCE(b.name_pt,''),
			b.level, b.hp, b.base_exp, b.job_exp, b.mvp_exp,
			b.attack, b.attack2, b.defense, b.magic_defense,
			b.str, b.agi, b.vit, b.int, b.dex, b.luk,
			b.attack_range, b.skill_range, b.chase_range,
			b.size, b.race, b.element, b.element_level, b.is_mvp,
			s.level, s.hp, s.base_exp, s.job_exp, s.mvp_exp,
			s.attack, s.attack2, s.defense, s.magic_defense,
			s.str, s.agi, s.vit, s.int, s.dex, s.luk,
			s.attack_range, s.skill_range, s.chase_range,
			s.size, s.race, s.element, s.element_level, s.is_mvp,
			COALESCE(s.scraped_at,'')
		FROM internal_mob_db b JOIN mob_server_db s ON b.mob_id = s.mob_id
		ORDER BY b.mob_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MobDiffEntry
	for rows.Next() {
		var (
			id                                int
			name, namePT                      string
			bLvl, bHP, bBExp, bJExp, bMExp    sql.NullInt64
			bAtk, bAtk2, bDef, bMdef          sql.NullInt64
			bSt, bAg, bVi, bIn, bDx, bLk      sql.NullInt64
			bAtkR, bSkR, bChR, bElemLvl, bMvp sql.NullInt64
			sLvl, sHP, sBExp, sJExp, sMExp    sql.NullInt64
			sAtk, sAtk2, sDef, sMdef          sql.NullInt64
			sSt, sAg, sVi, sIn, sDx, sLk      sql.NullInt64
			sAtkR, sSkR, sChR, sElemLvl, sMvp sql.NullInt64
			base, live                        MobDetail
			scrapedAt                         string
		)
		if err := rows.Scan(&id, &name, &namePT,
			&bLvl, &bHP, &bBExp, &bJExp, &bMExp,
			&bAtk, &bAtk2, &bDef, &bMdef,
			&bSt, &bAg, &bVi, &bIn, &bDx, &bLk,
			&bAtkR, &bSkR, &bChR, &base.Size, &base.Race, &base.Element, &bElemLvl, &bMvp,
			&sLvl, &sHP, &sBExp, &sJExp, &sMExp,
			&sAtk, &sAtk2, &sDef, &sMdef,
			&sSt, &sAg, &sVi, &sIn, &sDx, &sLk,
			&sAtkR, &sSkR, &sChR, &live.Size, &live.Race, &live.Element, &sElemLvl, &sMvp,
			&scrapedAt); err != nil {
			log.Printf("[W] [HTTP/Mobs] diff scan failed: %v", err)
			continue
		}

		base.Level, base.HP, base.BaseExp, base.JobExp, base.MvpExp = ni(bLvl), ni(bHP), ni(bBExp), ni(bJExp), ni(bMExp)
		base.AtkMin, base.AtkMax, base.Def, base.Mdef = ni(bAtk), ni(bAtk2), ni(bDef), ni(bMdef)
		base.Str, base.Agi, base.Vit, base.Int, base.Dex, base.Luk = ni(bSt), ni(bAg), ni(bVi), ni(bIn), ni(bDx), ni(bLk)
		base.AtkRange, base.SkillRange, base.ChaseRange, base.ElementLvl = ni(bAtkR), ni(bSkR), ni(bChR), ni(bElemLvl)
		base.IsMvp = bMvp.Int64 != 0

		live.Level, live.HP, live.BaseExp, live.JobExp, live.MvpExp = ni(sLvl), ni(sHP), ni(sBExp), ni(sJExp), ni(sMExp)
		live.AtkMin, live.AtkMax, live.Def, live.Mdef = ni(sAtk), ni(sAtk2), ni(sDef), ni(sMdef)
		live.Str, live.Agi, live.Vit, live.Int, live.Dex, live.Luk = ni(sSt), ni(sAg), ni(sVi), ni(sIn), ni(sDx), ni(sLk)
		live.AtkRange, live.SkillRange, live.ChaseRange, live.ElementLvl = ni(sAtkR), ni(sSkR), ni(sChR), ni(sElemLvl)
		live.IsMvp = sMvp.Int64 != 0

		var changed []MobFieldDiff
		for _, f := range mobFieldDiffs(base, live) {
			if f.Differs {
				changed = append(changed, f)
			}
		}
		if len(changed) <= 1 {
			continue // "more than 1 attribute" — skip matches and single-field diffs
		}
		out = append(out, MobDiffEntry{
			ID:          id,
			DisplayName: mobDisplayName(name, namePT, lang),
			IsMvp:       live.IsMvp || base.IsMvp,
			DiffCount:   len(changed),
			ScrapedAgo:  timeAgo(scrapedAt),
			DiffFields:  changed,
		})
	}

	// Most-divergent first; ties keep mob_id order (stable from the query).
	sort.SliceStable(out, func(i, j int) bool { return out[i].DiffCount > out[j].DiffCount })
	return out, nil
}

// rawDrop tolerates both drop encodings stored in the drops column:
//   - @mobinfo: {"item_id":990,"rate_pct":0.70}
//   - YAML seed: {"Item":"Jellopy","Rate":7000}   (Rate is 0.01% units)
//
// The JSON field names don't collide, so one struct unmarshals either shape.
type rawDrop struct {
	ItemID  int64   `json:"item_id"`
	RatePct float64 `json:"rate_pct"`
	Item    string  `json:"Item"`
	Rate    *int64  `json:"Rate"`
}

// ItemDropSource is one monster known to drop a given item — the reverse of the
// mob detail page's drop table, surfaced in the "Dropped By" section of the item
// page by cross-referencing the mob DBs.
type ItemDropSource struct {
	MobID       int
	DisplayName string
	IsMvp       bool    // the mob itself is an MVP
	MvpReward   bool    // dropped as an MVP reward rather than a normal drop
	RatePct     float64 // best-known drop chance (live overrides baseline)
	Live        bool    // rate came from the live-server scrape, not the YAML baseline
}

// fetchItemDropSources finds every mob that drops the given item, cross-referenced
// from the YAML baseline (internal_mob_db, whose drop blobs key on the aegis name)
// and the live-server scrape (mob_server_db, which keys on item id). A live rate
// overrides the baseline rate for the same mob. Returns nil when nothing drops it.
func fetchItemDropSources(itemID int, lang string) []ItemDropSource {
	if itemID <= 0 {
		return nil
	}

	// The baseline drop blobs reference items by aegis name, so resolve it first.
	var aegis string
	if err := srv.db.QueryRow(
		`SELECT COALESCE(aegis_name,'') FROM internal_item_db WHERE item_id = ?`, itemID,
	).Scan(&aegis); err != nil {
		log.Printf("[D] [HTTP/History] drop-source: no aegis name for item %d: %v", itemID, err)
	}

	// Keyed by mob_id so a live scrape can override the baseline rate in place.
	byMob := map[int]*ItemDropSource{}

	// scan walks one mob table, decoding the drops/mvp_drops blobs of every
	// prefilter-matched row and recording the mobs whose blobs actually contain
	// the item (match guards against LIKE false positives).
	scan := func(query string, args []any, live bool, match func(rawDrop) bool) {
		rows, err := srv.db.Query(query, args...)
		if err != nil {
			log.Printf("[E] [HTTP/History] drop-source query: %v", err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var (
				mobID              int
				name, namePT       string
				isMvp              bool
				dropsJSON, mvpJSON string
			)
			if err := rows.Scan(&mobID, &name, &namePT, &isMvp, &dropsJSON, &mvpJSON); err != nil {
				continue
			}
			for _, set := range []struct {
				raw       string
				mvpReward bool
			}{{dropsJSON, false}, {mvpJSON, true}} {
				var drops []rawDrop
				if json.Unmarshal([]byte(set.raw), &drops) != nil {
					continue
				}
				for _, d := range drops {
					if !match(d) {
						continue
					}
					pct := d.RatePct
					if d.Rate != nil { // YAML baseline shape: Rate is in 0.01% units
						pct = float64(*d.Rate) / 100.0
					}
					src, ok := byMob[mobID]
					if !ok {
						src = &ItemDropSource{
							MobID:       mobID,
							DisplayName: mobDisplayName(name, namePT, lang),
							IsMvp:       isMvp,
						}
						byMob[mobID] = src
					}
					// Live data wins; otherwise fill in the baseline.
					if live || !src.Live {
						src.RatePct = pct
						src.Live = live
						src.MvpReward = set.mvpReward
					}
					break // the item appears at most once per drop set
				}
			}
		}
	}

	// Baseline (full mob coverage), matched by aegis name.
	if aegis != "" {
		like := "%\"Item\":\"" + aegis + "\"%"
		scan(`SELECT mob_id, COALESCE(name,''), COALESCE(name_pt,''), COALESCE(is_mvp,0),
		             COALESCE(drops,'[]'), COALESCE(mvp_drops,'[]')
		      FROM internal_mob_db
		      WHERE drops LIKE ? OR mvp_drops LIKE ?`,
			[]any{like, like}, false,
			func(d rawDrop) bool { return d.Item == aegis })
	}

	// Live server scrape (scraped mobs only), matched by item id. name_pt isn't
	// stored in this table, so the English name doubles as the fallback.
	likeID := "%\"item_id\":" + strconv.Itoa(itemID) + "%"
	scan(`SELECT mob_id, COALESCE(name,''), COALESCE(name,''), COALESCE(is_mvp,0),
	             COALESCE(drops,'[]'), COALESCE(mvp_drops,'[]')
	      FROM mob_server_db
	      WHERE drops LIKE ? OR mvp_drops LIKE ?`,
		[]any{likeID, likeID}, true,
		func(d rawDrop) bool { return d.ItemID == int64(itemID) })

	if len(byMob) == 0 {
		return nil
	}
	out := make([]ItemDropSource, 0, len(byMob))
	for _, s := range byMob {
		out = append(out, *s)
	}
	// Best chance first; stable tiebreak on mob id keeps the order deterministic.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].RatePct != out[j].RatePct {
			return out[i].RatePct > out[j].RatePct
		}
		return out[i].MobID < out[j].MobID
	})
	return out
}

// parseMobDrops decodes a drops JSON blob into display rows, resolving item ids
// to localized names (and linking them) where possible.
func parseMobDrops(raw, lang string) []MobDropView {
	var drops []rawDrop
	if err := json.Unmarshal([]byte(raw), &drops); err != nil || len(drops) == 0 {
		return nil
	}
	out := make([]MobDropView, 0, len(drops))
	for _, d := range drops {
		var v MobDropView
		switch {
		case d.ItemID > 0:
			v.ItemID = d.ItemID
			v.RatePct = d.RatePct
			if name, ok := itemNameByID(d.ItemID, lang); ok {
				v.Name, v.Known = name, true
			} else {
				v.Name = "item #" + strconv.FormatInt(d.ItemID, 10)
			}
		case d.Item != "":
			// YAML seed shape: Item is the aegis name (e.g. "Yoyo_Tail"),
			// Rate is in 0.01% units. Resolve the aegis name to a localized
			// display name and link it; fall back to the raw aegis name.
			if name, id, ok := itemByAegis(d.Item, lang); ok {
				v.ItemID, v.Name, v.Known = id, name, true
			} else {
				v.Name = d.Item
			}
			if d.Rate != nil {
				v.RatePct = float64(*d.Rate) / 100.0
			}
		default:
			continue
		}
		out = append(out, v)
	}
	return out
}
