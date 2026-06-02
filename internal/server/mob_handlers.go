package server

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
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

type MobsPageData struct {
	Mobs        []MobListEntry
	SearchQuery string
	TotalMobs   int
	Pagination  httpx.PaginationData
	Filter      string // query-string suffix preserving the search across pages
	PageTitle   string
}

// MobFieldDiff is one row of the baseline-vs-server comparison table.
type MobFieldDiff struct {
	Label    string
	Baseline string
	Server   string
	Differs  bool
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
}

type MobDetailPageData struct {
	Mob        MobDetail
	Comparison MobComparison
	PageTitle  string
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

	query := "SELECT mob_id, name, COALESCE(name_pt,''), aegis_name, level, hp, race, element, element_level, size, is_mvp " +
		"FROM internal_mob_db " + where + " ORDER BY mob_id LIMIT ? OFFSET ?"
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
			e                            MobListEntry
			name, namePT                 string
			level, hp, elementLvl        sql.NullInt64
			isMvp                        sql.NullInt64
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

	filter := ""
	if q != "" {
		filter = "&q=" + url.QueryEscape(q)
	}

	renderTemplate(w, r, "mobs.html", MobsPageData{
		Mobs:        mobs,
		SearchQuery: q,
		TotalMobs:   total,
		Pagination:  pg,
		Filter:      filter,
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
		m                                            MobDetail
		name, namePT                                 string
		level, hp, sp, baseExp, jobExp, mvpExp       sql.NullInt64
		atk, atk2, def, mdef                         sql.NullInt64
		st, ag, vi, in, dx, lk                       sql.NullInt64
		atkR, skR, chR, elemLvl, isMvp               sql.NullInt64
		dropsJSON, mvpDropsJSON                      string
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

	renderTemplate(w, r, "mob_detail.html", MobDetailPageData{
		Mob:        m,
		Comparison: cmp,
		PageTitle:  "Bestiary",
	})
}

// loadMobComparison loads the live server row (if any) for a mob and diffs it
// against the YAML baseline. Returns HasServer=false when the mob hasn't been
// scraped yet.
func loadMobComparison(id int, base MobDetail, lang string) MobComparison {
	var (
		s                                      MobDetail
		level, hp, baseExp, jobExp, mvpExp     sql.NullInt64
		atk, atk2, def, mdef                   sql.NullInt64
		st, ag, vi, in, dx, lk                 sql.NullInt64
		atkR, skR, chR, elemLvl, isMvp         sql.NullInt64
		dropsJSON, mvpDropsJSON, scrapedAt     string
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

	diffCount := 0
	for _, f := range fields {
		if f.Differs {
			diffCount++
		}
	}

	return MobComparison{
		HasServer:      true,
		ScrapedAgo:     timeAgo(scrapedAt),
		DiffCount:      diffCount,
		Fields:         fields,
		ServerDrops:    parseMobDrops(dropsJSON, lang),
		ServerMvpDrops: parseMobDrops(mvpDropsJSON, lang),
	}
}

// rawDrop tolerates both drop encodings stored in the drops column:
//   - @mobinfo: {"item_id":990,"rate_pct":0.70}
//   - YAML seed: {"Item":"Jellopy","Rate":7000}   (Rate is 0.01% units)
// The JSON field names don't collide, so one struct unmarshals either shape.
type rawDrop struct {
	ItemID  int64   `json:"item_id"`
	RatePct float64 `json:"rate_pct"`
	Item    string  `json:"Item"`
	Rate    *int64  `json:"Rate"`
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
			// YAML seed shape: Item is the aegis name, Rate is in 0.01% units.
			v.Name = d.Item
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
