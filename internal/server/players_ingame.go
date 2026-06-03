package server

import (
	"context"
	"encoding/binary"
	"log"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// players_ingame.go is the in-game counterpart to the website-scraping player
// counter (scrapeAndStorePlayerCount in scraper.go). Instead of fetching
// projetoyufa.com/en/info, it asks the live zone connection (see zoneproxy.go)
// for the online total and reads the count straight off the server's reply.
//
// Two sources, tried in order:
//
//  1. "/who" (PREFERRED). The client's /who command is the bare 2-byte
//     CZ_REQ_USER_COUNT (0x00c1) packet; the server answers with ZC_USER_COUNT
//     (0x00c2) = header + a uint32 total. We inject the raw 0x00c1 (no chat
//     framing, no "<charname>" prefix) via injectRawPacket and read the count
//     off the 0x00c2 reply in scanUserCountPacket. This needs NO GM privilege,
//     carries no locale dependency, and is a single fixed-format reply.
//  2. "@users" (FALLBACK). A GM atcommand: rAthena prints one
//     "<map>: <n> (<pct>%)" line per populated map and a final "all: <total>"
//     line, all via clif_displaymessage — i.e. as 0x008e self-chat packets, the
//     same burst-of-self-chat shape the @mobinfo/@whereis parsers consume off
//     startChatPacketCapture. The per-map and "all:" lines are a hardcoded
//     rAthena printf (not run through the message-table translator), so
//     "all: <N>" is language-independent even though this server speaks
//     Portuguese — we key the total off it. Only reachable if the logged-in
//     account's group is allowed @users.
//
// Both reply forms deliver to the same armed accumulator, so a scrape arms once
// per attempt and either packet satisfies it. The result is written to the same
// player_history table the site scraper uses (via storePlayerCount), so the
// /players graph and the admin "last player count" stat cover all sources
// transparently. It is registered as the default-disabled "players-ingame"
// scheduler job and wired to the admin "Player Count (In-Game)" button.

// ingameUsersLogTag MUST equal the players-ingame job's JobSpec.LogTag so the
// scheduler can classify a run as failed when this driver logs an "[E]" line.
const ingameUsersLogTag = "[Scraper/PlayerCount/InGame]"

// ingameUsersTimeout bounds how long the scrape waits for one reply form (the
// /who 0x00c2 packet, or the @users self-chat burst) before giving up and
// disarming the parser. Each attempt gets its own window.
const ingameUsersTimeout = 5 * time.Second

// maxPlausibleUserCount is an upper sanity bound on a ZC_USER_COUNT (0x00c2)
// total. The 2-byte 0xc2 0x00 prefix can occur incidentally inside other binary
// zone packets, so scanUserCountPacket only matches while a scrape is armed AND
// the decoded uint32 is within [0, maxPlausibleUserCount]. No RO server's online
// population approaches this, so it rejects a stray prefix that decoded to junk.
const maxPlausibleUserCount = 1_000_000

// reUsersTotal matches the final "all: <total>" line of an @users reply. The
// prefix and format are a hardcoded rAthena printf, so this stays locale-stable.
var reUsersTotal = regexp.MustCompile(`(?i)^all:\s*(\d+)\s*$`)

// reUsersMapLine matches a per-map "<map>: <count> (<pct>%)" line so the
// accumulator can swallow it (it is part of the reply, not ordinary chat)
// without mistaking it for the total.
var reUsersMapLine = regexp.MustCompile(`^\S.*:\s*\d+\s*\(\d+%\)\s*$`)

// usersAccumulator is the single in-flight @users reply reader. Like whereIs it
// is only "armed" between expect() and finalize, so it never claims stray chat
// outside a scrape. When it sees the "all:" total it delivers the count on res
// and disarms.
type usersAccumulator struct {
	mu    sync.Mutex
	armed bool
	res   chan int
	lines int // per-map lines consumed while armed (diagnostics)
}

var usersCount usersAccumulator

// expect arms the accumulator and returns the channel the total will be
// delivered on (buffered, so handleLine never blocks). The caller injects
// "@users" immediately after.
func (a *usersAccumulator) expect() <-chan int {
	a.mu.Lock()
	defer a.mu.Unlock()
	ch := make(chan int, 1)
	a.armed = true
	a.res = ch
	a.lines = 0
	return ch
}

// disarm clears the armed state (on timeout or after delivery). Safe to call
// more than once.
func (a *usersAccumulator) disarm() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.armed = false
	a.res = nil
}

// isArmed reports whether a scrape is currently waiting for a reply. It lets the
// packet-capture loop skip the 0x00c2 scan entirely when no /who is in flight,
// so the bare 0xc2 0x00 prefix occurring inside ordinary binary zone traffic is
// never even inspected outside the reply window.
func (a *usersAccumulator) isArmed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.armed
}

// handleUserCount delivers a ZC_USER_COUNT (0x00c2) total to a waiting scrape.
// It is the binary counterpart to handleLine: the /who request (0x00c1) yields a
// single fixed-format reply rather than a self-chat burst. Returns true if the
// count was claimed by an in-flight scrape (so the caller stops scanning).
func (a *usersAccumulator) handleUserCount(n int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.armed {
		return false // not armed → not ours
	}
	if a.res != nil {
		a.res <- n // buffered, non-blocking
	}
	a.armed = false
	a.res = nil
	if enableChatScraperDebugLogs {
		log.Printf("[D] %s captured /who total: %d online (ZC_USER_COUNT)", ingameUsersLogTag, n)
	}
	return true
}

// scanUserCountPacket searches a raw zone payload for a ZC_USER_COUNT (0x00c2)
// reply and, if an in-game player-count scrape is armed, delivers the total.
// 0x00c2 is fixed-length 6 bytes — the 2-byte little-endian id 0xc2 0x00 plus a
// uint32 count — and is not a text packet, so it is handled out-of-band from the
// knownChatPackets text parse loop in startChatPacketCapture. It is a no-op
// unless a /who is in flight (see isArmed / maxPlausibleUserCount). Returns true
// if a count was delivered.
func scanUserCountPacket(payload []byte) bool {
	if !usersCount.isArmed() {
		return false
	}
	for i := 0; i+6 <= len(payload); i++ {
		if payload[i] != 0xc2 || payload[i+1] != 0x00 {
			continue
		}
		n := int(binary.LittleEndian.Uint32(payload[i+2 : i+6]))
		if n < 0 || n > maxPlausibleUserCount {
			continue // implausible → a stray prefix, keep scanning
		}
		if usersCount.handleUserCount(n) {
			return true
		}
	}
	return false
}

// handleLine feeds one decoded 0x008e self-chat line to the accumulator while
// armed. It returns true if the line was part of the @users reply (so the
// capture loop does not store it as ordinary chat), false otherwise.
func (a *usersAccumulator) handleLine(msg string, _ time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.armed {
		return false // not armed → not ours
	}

	if m := reUsersTotal.FindStringSubmatch(msg); m != nil {
		n, _ := strconv.Atoi(m[1])
		if a.res != nil {
			a.res <- n // buffered, non-blocking
		}
		a.armed = false
		a.res = nil
		if enableChatScraperDebugLogs {
			log.Printf("[D] %s captured total: %d online (after %d map line(s)): %q", ingameUsersLogTag, n, a.lines, msg)
		}
		return true
	}

	if reUsersMapLine.MatchString(msg) {
		a.lines++
		if enableChatScraperDebugLogs {
			log.Printf("[D] %s consumed per-map line: %q", ingameUsersLogTag, msg)
		}
		return true
	}

	// Not a recognizable @users line — leave it for the normal chat path.
	return false
}

// scrapeAndStorePlayerCountInGame queries the live zone connection for the
// online count and stores it in player_history. It tries /who first (the
// non-GM CZ_REQ_USER_COUNT packet) and falls back to the @users GM atcommand,
// so it works whether or not the logged-in account has GM privileges. It is the
// scheduler job function for "players-ingame" and the handler target for the
// admin "Player Count (In-Game)" button.
func scrapeAndStorePlayerCountInGame() {
	log.Printf("[I] %s Querying online player count...", ingameUsersLogTag)

	if ready, name := zoneProxyReady(); !ready || name == "" {
		log.Printf("[E] %s zone proxy not ready (active=%v, charName=%q); log into the game first (and enable ZONE_PROXY).",
			ingameUsersLogTag, ready, name)
		return
	}

	// 1. /who (CZ_REQ_USER_COUNT): non-GM, clean binary total. Preferred.
	if n, ok := queryInGameUserCount("/who", func() error {
		return injectRawPacket(buildUserCountRequest())
	}); ok {
		log.Printf("[I] %s /who reported %d players online.", ingameUsersLogTag, n)
		storePlayerCount(n, ingameUsersLogTag)
		return
	}
	log.Printf("[I] %s no /who (0x00c2) reply within %v — server may run an older packetver or have it disabled; trying @users.", ingameUsersLogTag, ingameUsersTimeout)

	// 2. @users (GM atcommand): fallback if /who is unsupported.
	if n, ok := queryInGameUserCount("@users", func() error {
		return injectChatCommand("@users")
	}); ok {
		log.Printf("[I] %s @users reported %d players online.", ingameUsersLogTag, n)
		storePlayerCount(n, ingameUsersLogTag)
		return
	}

	log.Printf("[W] %s no count captured via /who or @users (each waited %v) — /who may be unsupported and @users restricted for this account, or their replies are not the expected packets. Nothing stored.", ingameUsersLogTag, ingameUsersTimeout)
}

// queryInGameUserCount arms the accumulator, runs inject, and waits up to
// ingameUsersTimeout for the count — delivered by either reply form (the /who
// 0x00c2 packet via scanUserCountPacket, or an @users "all:" line via
// handleLine). The accumulator is armed BEFORE inject so a reply that carries
// no correlation id is still attributed to this request. Returns (count, true)
// on success or (0, false) on inject error / timeout.
func queryInGameUserCount(label string, inject func() error) (int, bool) {
	res := usersCount.expect()
	if err := inject(); err != nil {
		usersCount.disarm()
		// A per-attempt inject failure is recoverable: /who falls back to @users,
		// and @users falls back to the site scraper. Log it at [W], NOT [E] — an
		// [E] line carrying this job's LogTag makes the scheduler classify the
		// whole players-ingame run as failed (see scanForError in scheduler.go),
		// which is wrong while a later source can still satisfy the scrape. The
		// real "nothing stored" outcome is reported once, as [W], by the caller.
		// (A common cause of a /who failure is a proxy process still running an
		// older binary without the injectRaw op — restart the proxy unit.)
		log.Printf("[W] %s %s inject failed (trying the next source, if any): %v", ingameUsersLogTag, label, err)
		return 0, false
	}

	// Derive the wait from the app shutdown context so a SIGTERM doesn't leave
	// us blocking on a reply that will never come.
	base := bgCtx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(base, ingameUsersTimeout)
	defer cancel()

	select {
	case n := <-res:
		return n, true
	case <-ctx.Done():
		usersCount.disarm()
		return 0, false
	}
}
