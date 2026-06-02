package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// whereisscrape.go drives the @whereis spawn-location sweep, the sibling of the
// @mobinfo sweep (mobscrape.go): it walks the SAME configured mob-id range and
// delay (mobscrape_config — the two commands cover the same set of mobs, so they
// share one knob) and injects "@whereis <id>" into the live zone connection,
// arming the parser with each id via whereIs.expect(id) just before sending so
// the reply burst is attributed to the right mob (the reply carries no id). The
// replies are parsed and upserted by whereis_parser.go off the passive capture.
//
// Registered as the `mobspawn` scheduler job so it gets the standard admin
// controls; the Schedulers tab adds a dedicated card next to the @mobinfo one.

// whereIsScrapeLogTag MUST equal the mobspawn job's JobSpec.LogTag so the
// scheduler can classify a run as failed when this driver logs an "[E]" line.
const whereIsScrapeLogTag = "[Scraper/WhereIs]"

// whereIsScrapeController guards a single in-flight sweep and exposes progress.
type whereIsScrapeController struct {
	mu        sync.Mutex
	running   bool
	cancel    context.CancelFunc
	from      int
	to        int
	current   int
	sent      int
	startedAt time.Time
}

var whereIsScrape whereIsScrapeController

// whereIsScrapeStatus is a point-in-time snapshot for the admin UI.
type whereIsScrapeStatus struct {
	Running   bool
	From      int
	To        int
	Current   int
	Sent      int
	StartedAt time.Time
}

func (c *whereIsScrapeController) snapshot() whereIsScrapeStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return whereIsScrapeStatus{
		Running: c.running, From: c.from, To: c.to,
		Current: c.current, Sent: c.sent, StartedAt: c.startedAt,
	}
}

// stop cancels an in-flight sweep (no-op if none is running).
func (c *whereIsScrapeController) stop() bool {
	c.mu.Lock()
	cancel := c.cancel
	running := c.running
	c.mu.Unlock()
	if running && cancel != nil {
		cancel()
		return true
	}
	return false
}

// runWhereIsSweep is the scheduler job function. It sends "@whereis <id>" for
// every id in the configured range, arming the parser before each send and
// pacing by the configured delay; it stops early on cancel (admin Stop button)
// or injection failure.
func runWhereIsSweep() {
	from, to, delayMs := loadMobScrapeConfig()
	if from <= 0 || to < from {
		log.Printf("[E] %s invalid mob id range %d-%d", whereIsScrapeLogTag, from, to)
		return
	}
	ready, charName := zoneProxyReady()
	if !ready || charName == "" {
		log.Printf("[E] %s zone proxy not ready (active=%v, charName=%q); log into the game first (and enable ZONE_PROXY).",
			whereIsScrapeLogTag, ready, charName)
		return
	}

	// Derive from the app shutdown context so a SIGTERM aborts an in-flight
	// sweep instead of blocking bgWg.Wait(). The Stop button cancels this same
	// context via whereIsScrape.cancel.
	base := bgCtx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancel(base)

	whereIsScrape.mu.Lock()
	if whereIsScrape.running {
		whereIsScrape.mu.Unlock()
		cancel()
		log.Printf("[W] %s a sweep is already running; ignoring trigger.", whereIsScrapeLogTag)
		return
	}
	whereIsScrape.running = true
	whereIsScrape.cancel = cancel
	whereIsScrape.from, whereIsScrape.to, whereIsScrape.current, whereIsScrape.sent = from, to, from, 0
	whereIsScrape.startedAt = time.Now()
	whereIsScrape.mu.Unlock()

	defer func() {
		whereIsScrape.mu.Lock()
		whereIsScrape.running = false
		whereIsScrape.cancel = nil
		whereIsScrape.mu.Unlock()
		cancel()
		// Flush whatever the last id replied with and stop claiming chat.
		whereIs.disarm()
	}()

	delay := time.Duration(delayMs) * time.Millisecond
	total := to - from + 1
	log.Printf("[I] %s starting sweep %d-%d (%d ids) as %q, %v between commands.", whereIsScrapeLogTag, from, to, total, charName, delay)

	for id := from; id <= to; id++ {
		select {
		case <-ctx.Done():
			log.Printf("[I] %s sweep stopped at id %d (%d/%d sent).", whereIsScrapeLogTag, id, id-from, total)
			return
		default:
		}

		// Arm the parser for this id BEFORE sending, so its reply burst (which
		// carries no id) is attributed correctly. expect() also finalizes the
		// previous id's block.
		whereIs.expect(id)
		if err := injectChatCommand(fmt.Sprintf("@whereis %d", id)); err != nil {
			log.Printf("[E] %s inject failed at id %d (%d/%d sent): %v", whereIsScrapeLogTag, id, id-from, total, err)
			return
		}
		if enableChatScraperDebugLogs {
			log.Printf("[D] %s injected '@whereis %d' (%d/%d)", whereIsScrapeLogTag, id, id-from+1, total)
		}

		whereIsScrape.mu.Lock()
		whereIsScrape.current = id
		whereIsScrape.sent++
		sent := whereIsScrape.sent
		whereIsScrape.mu.Unlock()

		// Periodic progress so a long sweep is observable in the logs even
		// without per-mob debug lines.
		if sent%100 == 0 {
			log.Printf("[I] %s progress: %d/%d sent (at id %d).", whereIsScrapeLogTag, sent, total, id)
		}

		// Pace between commands, but wake immediately on a stop request.
		select {
		case <-ctx.Done():
			log.Printf("[I] %s sweep stopped at id %d.", whereIsScrapeLogTag, id)
			return
		case <-time.After(delay):
		}
	}

	final := whereIsScrape.snapshot()
	log.Printf("[I] %s sweep complete: %d commands sent (%d-%d).", whereIsScrapeLogTag, final.Sent, from, to)
}
