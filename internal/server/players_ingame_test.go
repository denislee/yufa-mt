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
	if a.handleUserCount(5) {
		t.Error("unarmed accumulator claimed a /who count")
	}
}

// TestUsersHandleUserCount feeds a ZC_USER_COUNT (0x00c2) total to an armed
// accumulator and asserts it is delivered, then that the accumulator disarms so
// a late count is not double-claimed.
func TestUsersHandleUserCount(t *testing.T) {
	var a usersAccumulator
	res := a.expect()

	if !a.handleUserCount(137) {
		t.Fatal("armed accumulator did not claim a /who count")
	}
	select {
	case n := <-res:
		if n != 137 {
			t.Errorf("got count %d, want 137", n)
		}
	default:
		t.Fatal("/who count was not delivered on the channel")
	}

	if a.handleUserCount(999) {
		t.Error("accumulator claimed a count after it should have disarmed")
	}
}

// TestScanUserCountPacket exercises the binary scan against the global usersCount
// accumulator: it must only deliver while armed, must decode the little-endian
// uint32 after the 0xc2 0x00 id, and must reject an implausible value (a stray
// prefix in unrelated binary data) without delivering.
func TestScanUserCountPacket(t *testing.T) {
	t.Cleanup(usersCount.disarm) // never leave the global armed for other tests

	// Not armed: a real-looking ZC_USER_COUNT must be ignored.
	pkt := []byte{0xc2, 0x00, 42, 0, 0, 0}
	if scanUserCountPacket(pkt) {
		t.Error("scan delivered a count while not armed")
	}

	// Armed: the count is decoded and delivered.
	res := usersCount.expect()
	// Embed the 6-byte packet inside a larger buffer to prove offset scanning.
	buf := append([]byte{0x01, 0x02, 0x03}, pkt...)
	buf = append(buf, 0x99)
	if !scanUserCountPacket(buf) {
		t.Fatal("scan did not deliver an embedded ZC_USER_COUNT while armed")
	}
	select {
	case n := <-res:
		if n != 42 {
			t.Errorf("decoded count %d, want 42", n)
		}
	default:
		t.Fatal("count was not delivered on the channel")
	}

	// Implausible value: a stray 0xc2 0x00 prefix decoding past the sanity bound
	// must not be claimed (and must not disarm a waiting scrape).
	res = usersCount.expect()
	junk := []byte{0xc2, 0x00, 0xff, 0xff, 0xff, 0xff} // 4294967295
	if scanUserCountPacket(junk) {
		t.Error("scan claimed an implausible count")
	}
	if !usersCount.isArmed() {
		t.Error("an implausible count wrongly disarmed the accumulator")
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
