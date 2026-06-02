package server

import (
	"bytes"
	"html/template"
	"strings"
	"testing"

	"github.com/denislee/yufa-mt/web"
)

func TestMobDropDiffsMerge(t *testing.T) {
	baseline := []MobDropView{
		{ItemID: 942, Name: "Yoyo Tail", Known: true, RatePct: 90.00},
		{ItemID: 508, Name: "Yellow Herb", Known: true, RatePct: 2.00},
		{ItemID: 7182, Name: "Cacao", Known: true, RatePct: 9.00}, // baseline only
	}
	server := []MobDropView{
		{ItemID: 942, Name: "Yoyo Tail", Known: true, RatePct: 55.00},   // differs
		{ItemID: 508, Name: "Yellow Herb", Known: true, RatePct: 2.00},  // matches
		{ItemID: 1100, Name: "Animal Skin", Known: true, RatePct: 50.0}, // server only
	}

	diffs, count := mobDropDiffs(baseline, server)
	if len(diffs) != 4 {
		t.Fatalf("got %d merged rows, want 4: %+v", len(diffs), diffs)
	}
	// Yoyo Tail differs, Cacao baseline-only, Animal Skin server-only → 3 differ.
	if count != 3 {
		t.Errorf("diff count = %d, want 3", count)
	}

	byName := map[string]MobDropDiff{}
	for _, d := range diffs {
		byName[d.Name] = d
	}
	if d := byName["Yoyo Tail"]; !d.Differs || !d.HasBaseline || !d.HasServer {
		t.Errorf("Yoyo Tail: %+v, want differs with both sides", d)
	}
	if d := byName["Yellow Herb"]; d.Differs {
		t.Errorf("Yellow Herb should match, got %+v", d)
	}
	if d := byName["Cacao"]; !d.HasBaseline || d.HasServer || !d.Differs {
		t.Errorf("Cacao should be baseline-only, got %+v", d)
	}
	if d := byName["Animal Skin"]; d.HasBaseline || !d.HasServer || !d.Differs {
		t.Errorf("Animal Skin should be server-only, got %+v", d)
	}

	// Differing rows must sort ahead of matching rows.
	if !diffs[0].Differs {
		t.Errorf("first row should differ, got %+v", diffs[0])
	}
	if diffs[len(diffs)-1].Differs {
		t.Errorf("last row should match (non-differing), got %+v", diffs[len(diffs)-1])
	}
}

// TestMobDetailDiffTemplateRenders confirms the mob_detail template (with the
// new mob_drop_diff_table partial) parses and renders the comparison correctly.
func TestMobDetailDiffTemplateRenders(t *testing.T) {
	tmpl, err := template.New("mob_detail.html").Funcs(templateFuncs).
		ParseFS(web.Templates, "templates/mob_detail.html")
	if err != nil {
		t.Fatalf("parse mob_detail.html: %v", err)
	}

	diffs, count := mobDropDiffs(
		[]MobDropView{{ItemID: 942, Name: "Yoyo Tail", Known: true, RatePct: 90}},
		[]MobDropView{{ItemID: 942, Name: "Yoyo Tail", Known: true, RatePct: 55}},
	)
	data := TemplateData{Data: MobDetailPageData{
		Mob: MobDetail{ID: 1057, DisplayName: "Yoyo", Name: "Yoyo"},
		Comparison: MobComparison{
			HasServer: true, ScrapedAgo: "1h ago",
			DropDiffs: diffs, DropDiffCount: count,
		},
	}}

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "content", data); err != nil {
		t.Fatalf("execute content: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Baseline (YAML) vs Live Server") {
		t.Errorf("output missing diff table title:\n%s", out)
	}
	if !strings.Contains(out, "item(s) differ") {
		t.Errorf("output missing diff badge:\n%s", out)
	}
	if !strings.Contains(out, `/item?name=Yoyo`) {
		t.Errorf("output missing linked item:\n%s", out)
	}
	if !strings.Contains(out, "90.00%") || !strings.Contains(out, "55.00%") {
		t.Errorf("output missing both baseline and server rates:\n%s", out)
	}
}
