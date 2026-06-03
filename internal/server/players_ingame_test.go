package server

import (
	"testing"
	"time"
)

// TestUsersAccumulatorTotal feeds a representative @users reply burst through a
// freshly-armed accumulator and asserts that the per-map lines are consumed and
// the language-independent "all: <N>" total is delivered on the channel.
func TestUsersAccumulatorTotal(t *testing.T) {
	var a usersAccumulator
	res := a.expect()
	now := time.Now()

	// Per-map lines: each must be consumed (claimed as part of the reply) so it
	// is not stored as ordinary chat.
	mapLines := []string{
		"prontera: 42 (40%)",
		"morocc: 30 (28%)",
		"1@cata: 5 (4%)",
	}
	for _, ln := range mapLines {
		if !a.handleLine(ln, now) {
			t.Errorf("per-map line %q was not consumed by the accumulator", ln)
		}
	}

	// The total line must be consumed and deliver the count.
	if !a.handleLine("all: 107", now) {
		t.Fatalf(`"all: 107" total line was not consumed`)
	}
	select {
	case n := <-res:
		if n != 107 {
			t.Errorf("got total %d, want 107", n)
		}
	default:
		t.Fatal("total line did not deliver a count on the channel")
	}

	// After delivery the accumulator is disarmed: a late line is not ours.
	if a.handleLine("all: 999", now) {
		t.Error("accumulator claimed a line after it should have disarmed")
	}
}

// TestUsersAccumulatorNotArmed ensures the accumulator never claims chat when it
// has not been armed by a scrape — otherwise a player typing "all: 5" in chat
// could be swallowed.
func TestUsersAccumulatorNotArmed(t *testing.T) {
	var a usersAccumulator
	if a.handleLine("all: 5", time.Now()) {
		t.Error("unarmed accumulator claimed a line")
	}
	if a.handleLine("prontera: 5 (100%)", time.Now()) {
		t.Error("unarmed accumulator claimed a per-map line")
	}
}

// TestUsersTotalLocaleStable documents that the total is keyed off the hardcoded
// rAthena "all:" prefix and tolerates spacing/case, while a non-@users line
// (e.g. localized chat) is left for the normal chat path.
func TestUsersTotalLocaleStable(t *testing.T) {
	cases := []struct {
		line  string
		match bool
		want  int
	}{
		{"all: 12", true, 12},
		{"all:7", true, 7},
		{"ALL:  88  ", true, 88},
		{"jogadores: 5", false, 0}, // a translated header-like line is not the total
		{"prontera: 5 (10%)", false, 0},
	}
	for _, c := range cases {
		m := reUsersTotal.FindStringSubmatch(c.line)
		if (m != nil) != c.match {
			t.Errorf("reUsersTotal match(%q) = %v, want %v", c.line, m != nil, c.match)
			continue
		}
		if c.match && atoiSafe(m[1]) != c.want {
			t.Errorf("reUsersTotal(%q) captured %q, want %d", c.line, m[1], c.want)
		}
	}
}
