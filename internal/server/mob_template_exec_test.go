package server

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/denislee/yufa-mt/internal/httpx"
	"github.com/denislee/yufa-mt/internal/i18n"
)

func TestHistoryTemplateExecutesWithDropSources(t *testing.T) {
	renderPageForTest(t, "history.html", HistoryPageData{
		ItemName:    "Oridecon",
		ItemDetails: &RMSItem{ID: 984, Name: "Oridecon", Type: "Etc", Slots: 0},
		PageTitle:   "Oridecon",
		DropSources: []ItemDropSource{
			{MobID: 1039, DisplayName: "Baphomet", IsMvp: true, MvpReward: true, RatePct: 50, Live: true},
			{MobID: 1086, DisplayName: "Golden Thief Bug", RatePct: 12.5, Live: false},
		},
	})
}

// renders a page template through the layout exactly as renderTemplate does, so
// bad field references / missing partials surface (parsing alone won't catch them).
func renderPageForTest(t *testing.T, name string, data any) {
	t.Helper()
	tmpl, ok := templateCache[name]
	if !ok || tmpl == nil {
		t.Fatalf("%s not in templateCache", name)
	}
	full := TemplateData{
		Page: BasePageData{Lang: "en", T: i18n.Translations("en"), RequestURL: "/" + name},
		Data: data,
	}
	if err := tmpl.ExecuteTemplate(io.Discard, "layout.html", full); err != nil {
		t.Fatalf("%s execute failed: %v", name, err)
	}
}

func TestMobsTemplateExecutes(t *testing.T) {
	renderPageForTest(t, "mobs.html", MobsPageData{
		SearchQuery: "poring",
		TotalMobs:   120,
		Mobs: []MobListEntry{{
			ID: 1002, DisplayName: "Poring", AegisName: "PORING",
			Level: 1, HP: 50, Race: "Plant", Element: "Water", ElementLevel: 1, Size: "Small",
		}, {
			ID: 1039, DisplayName: "Baphomet", AegisName: "BAPHOMET",
			Level: 81, HP: 668000, Race: "Demon", Element: "Dark", ElementLevel: 4, Size: "Large", IsMvp: true,
		}},
		Pagination: httpx.PaginationData{CurrentPage: 1, TotalPages: 3, NextPage: 2, HasNextPage: true},
		Filter:     "&q=poring",
		PageTitle:  "Monsters",
	})
}

func TestMobsDiffsTabTemplateExecutes(t *testing.T) {
	renderPageForTest(t, "mobs.html", MobsPageData{
		Tab:       "diffs",
		TotalMobs: 1,
		PageTitle: "Monsters",
		Diffs: []MobDiffEntry{{
			ID: 1039, DisplayName: "Baphomet", IsMvp: true, DiffCount: 2, ScrapedAgo: "5 minutes ago",
			DiffFields: []MobFieldDiff{
				{Label: "HP", Baseline: "668000", Server: "999999", Differs: true},
				{Label: "DEF", Baseline: "35", Server: "50", Differs: true},
			},
		}},
	})
}

func TestMobDetailTemplateExecutes(t *testing.T) {
	renderPageForTest(t, "mob_detail.html", MobDetailPageData{
		PageTitle: "Monsters",
		Mob: MobDetail{
			ID: 1039, DisplayName: "Baphomet", Name: "Baphomet", AegisName: "BAPHOMET",
			Level: 81, HP: 668000, BaseExp: 107250, JobExp: 37895, MvpExp: 53625,
			AtkMin: 3220, AtkMax: 4040, Def: 35, Mdef: 45,
			Str: 1, Agi: 152, Vit: 96, Int: 85, Dex: 120, Luk: 95,
			AtkRange: 2, SkillRange: 10, ChaseRange: 12,
			Size: "Large", Race: "Demon", Element: "Dark", ElementLvl: 4, IsMvp: true,
			Drops: []MobDropView{
				{ItemID: 984, Name: "Oridecon", Known: true, RatePct: 43},
				{ItemID: 4147, Name: "item #4147", Known: false, RatePct: 0.01},
			},
			MvpDrops: []MobDropView{{ItemID: 607, Name: "Yggdrasil Berry", Known: true, RatePct: 20}},
		},
		Comparison: MobComparison{
			HasServer: true, ScrapedAgo: "5 minutes ago", DiffCount: 1,
			Fields: []MobFieldDiff{
				{Label: "Level", Baseline: "81", Server: "81", Differs: false},
				{Label: "HP", Baseline: "668000", Server: "999999", Differs: true},
			},
			ServerDrops: []MobDropView{{ItemID: 984, Name: "Oridecon", Known: true, RatePct: 50}},
		},
		SpawnsScraped: true, SpawnsAgo: "5 minutes ago",
		Spawns: []MobSpawnView{
			{Map: "abyss_03", Qty: 8},
			{Map: "abbey03", Qty: 2},
		},
	})
}

// TestMobDetailRendersSpawns guards against the spawn locations silently
// dropping out of the page: the handler loads Spawns and passes them, but
// text/template ignores fields the markup never references, so a plain
// "executes without error" check (TestMobDetailTemplateExecutes) would pass
// even with no spawn UI at all. Assert the map names actually reach the HTML.
func TestMobDetailRendersSpawns(t *testing.T) {
	tmpl, ok := templateCache["mob_detail.html"]
	if !ok || tmpl == nil {
		t.Fatal("mob_detail.html not in templateCache")
	}
	var buf bytes.Buffer
	data := TemplateData{
		Page: BasePageData{Lang: "en", T: i18n.Translations("en"), RequestURL: "/mob_detail.html"},
		Data: MobDetailPageData{
			PageTitle:     "Monsters",
			Mob:           MobDetail{ID: 1039, DisplayName: "Baphomet", Name: "Baphomet", AegisName: "BAPHOMET"},
			SpawnsScraped: true, SpawnsAgo: "5 minutes ago",
			Spawns: []MobSpawnView{{Map: "abyss_03", Qty: 8}, {Map: "abbey03", Qty: 2}},
		},
	}
	if err := tmpl.ExecuteTemplate(&buf, "layout.html", data); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"abyss_03", "abbey03", "Spawn Locations"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered mob_detail.html missing %q — spawn locations not rendered", want)
		}
	}
}
