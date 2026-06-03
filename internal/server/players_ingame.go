package server

import (
	"context"
	"log"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// players_ingame.go is the in-game counterpart to the website-scraping player
// counter (scrapeAndStorePlayerCount in scraper.go). Instead of fetching
// projetoyufa.com/en/info, it injects the GM atcommand "@users" into the live
// zone connection (see zoneproxy.go) and reads the count straight off the
// server's reply.
//
// rAthena's @users prints one "<map>: <n> (<pct>%)" line per populated map and a
// final "all: <total>" line, all via clif_displaymessage — i.e. as 0x008e
// self-chat packets, the same burst-of-self-chat shape the @mobinfo/@whereis
// parsers already consume off startChatPacketCapture. The per-map and "all:"
// lines are a hardcoded printf in rAthena (not run through the message-table
// translator), so "all: <N>" is language-independent even though this server
// speaks Portuguese — we key the total off it.
//
// The result is written to the same player_history table the site scraper uses
// (via storePlayerCount), so the /players graph and the admin "last player
// count" stat cover both sources transparently. It is registered as the
// default-disabled "players-ingame" scheduler job and wired to the admin
// "Player Count (In-Game)" button.

// ingameUsersLogTag MUST equal the players-ingame job's JobSpec.LogTag so the
// scheduler can classify a run as failed when this driver logs an "[E]" line.
const ingameUsersLogTag = "[Scraper/PlayerCount/InGame]"

// ingameUsersTimeout bounds how long the scrape waits for the @users reply burst
// before giving up and disarming the parser.
const ingameUsersTimeout = 5 * time.Second

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

// scrapeAndStorePlayerCountInGame injects "@users" into the live zone connection
// and stores the resulting online count in player_history. It is the scheduler
// job function for "players-ingame" and the handler target for the admin
// "Player Count (In-Game)" button.
func scrapeAndStorePlayerCountInGame() {
	log.Printf("[I] %s Querying online player count via @users...", ingameUsersLogTag)

	if ready, name := zoneProxyReady(); !ready || name == "" {
		log.Printf("[E] %s zone proxy not ready (active=%v, charName=%q); log into the game first (and enable ZONE_PROXY).",
			ingameUsersLogTag, ready, name)
		return
	}

	// Arm the parser BEFORE injecting so the reply burst (which carries no
	// correlation id) is attributed to this request.
	res := usersCount.expect()
	if err := injectChatCommand("@users"); err != nil {
		usersCount.disarm()
		log.Printf("[E] %s inject failed: %v", ingameUsersLogTag, err)
		return
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
		log.Printf("[I] %s @users reported %d players online.", ingameUsersLogTag, n)
		storePlayerCount(n, ingameUsersLogTag)
	case <-ctx.Done():
		usersCount.disarm()
		log.Printf("[W] %s no 'all: <N>' line captured within %v — @users may be unsupported, restricted for this account, or its reply is not a 0x008e self-chat packet. Nothing stored.", ingameUsersLogTag, ingameUsersTimeout)
	}
}
