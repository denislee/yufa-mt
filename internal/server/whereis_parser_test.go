package server

import (
	"testing"
	"time"
)

// feedWhereIs drives lines through a fresh accumulator without touching the DB,
// capturing the finalized block(s) keyed by mob id. It mirrors the sweep: each
// element of seq is one id and the reply lines it produced.
func feedWhereIs(t *testing.T, seq []struct {
	id    int
	lines []string
}) map[int]*spawnBlock {
	t.Helper()
	var a whereIsAccumulator
	got := map[int]*spawnBlock{}
	finalize := func() {
		if a.cur != nil && a.cur.id != 0 {
			got[a.cur.id] = a.cur
		}
		a.cur = nil
	}
	now := time.Now()
	for _, step := range seq {
		finalize()
		a.cur = &spawnBlock{id: step.id}
		for _, ln := range step.lines {
			if m := reSpawnLine.FindStringSubmatch(ln); m != nil {
				qty := atoiSafe(m[2])
				a.cur.spawns = append(a.cur.spawns, mobSpawnLoc{Map: m[1], Qty: qty})
				a.cur.sawMaps = true
			}
			// non-map lines are headers / notices — ignored, as in handleLine.
		}
	}
	finalize()
	_ = now
	return got
}

// Portuguese header text is a guess at the localized "%s spawns in:" line; the
// parser must NOT depend on it (it keys off the sweep's expected id and the
// language-independent "<map> (<qty>)" lines), so the exact wording is
// irrelevant to the test — what matters is that it is a non-map line.
func TestWhereIsParseSpawns(t *testing.T) {
	got := feedWhereIs(t, []struct {
		id    int
		lines []string
	}{
		{1001, []string{
			"Escorpião aparece em:",
			"moc_fild03 (40)",
			"moc_fild11 (30)",
			"  prt_fild08 (5) ",
		}},
		{1002, []string{ // Poring — no spawns
			"Poring aparece em:",
			"Este monstro não aparece normalmente.",
		}},
	})

	b := got[1001]
	if b == nil {
		t.Fatal("no block for mob 1001")
	}
	if len(b.spawns) != 3 {
		t.Fatalf("mob 1001 spawns = %d, want 3 (%+v)", len(b.spawns), b.spawns)
	}
	want := []mobSpawnLoc{
		{"moc_fild03", 40},
		{"moc_fild11", 30},
		{"prt_fild08", 5},
	}
	for i, w := range want {
		if b.spawns[i] != w {
			t.Errorf("spawn[%d] = %+v, want %+v", i, b.spawns[i], w)
		}
	}

	nb := got[1002]
	if nb == nil {
		t.Fatal("no block for mob 1002")
	}
	if len(nb.spawns) != 0 {
		t.Errorf("mob 1002 spawns = %d, want 0 (does not spawn)", len(nb.spawns))
	}
}

// TestWhereIsHandleLineArming checks the live handleLine path: lines are only
// consumed while armed, the header is swallowed, map lines accumulate, and a
// non-map line after the maps releases the block (returns false).
func TestWhereIsHandleLineArming(t *testing.T) {
	var a whereIsAccumulator
	now := time.Now()

	// Not armed → nothing is consumed.
	if a.handleLine("random chat", now) {
		t.Fatal("consumed a line while not armed")
	}

	a.cur = &spawnBlock{id: 1001} // arm (as expect() would)

	if !a.handleLine("Escorpião aparece em:", now) {
		t.Error("header line should be consumed while armed")
	}
	if !a.handleLine("moc_fild03 (40)", now) {
		t.Error("map line should be consumed")
	}
	if !a.cur.sawMaps || len(a.cur.spawns) != 1 {
		t.Errorf("expected 1 spawn after a map line, got %+v", a.cur)
	}
	// A non-map line after the maps ends the block and is NOT consumed.
	if a.handleLine("someone : hello there", now) {
		t.Error("post-map chat line should end the block, not be consumed")
	}
	if a.cur != nil {
		t.Error("block should have been finalized after the terminating line")
	}
}
