package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// mobscrape.go drives the @mobinfo reference-data sweep: it walks a configured
// mob-id range and injects "@mobinfo <id>" into the live zone connection (see
// zoneproxy.go), pacing the commands by a configurable delay to avoid flooding
// the server. The replies are parsed and upserted by mobinfo_parser.go off the
// passive capture, so this driver only has to send and pace.
//
// It is registered as a scheduler job (scheduler.go) so it gets the standard
// admin controls — enable, repeat-interval, run-now, run history — and the
// Schedulers tab adds a dedicated card for the range + delay + a stop button.

// mobScrapeLogTag MUST equal the mobinfo job's JobSpec.LogTag so the scheduler
// can classify a run as failed when this driver logs an "[E]" line.
const mobScrapeLogTag = "[Scraper/MobInfo]"

// mobScrapeController guards a single in-flight sweep and exposes progress.
type mobScrapeController struct {
	mu        sync.Mutex
	running   bool
	cancel    context.CancelFunc
	from      int
	to        int
	current   int
	sent      int
	startedAt time.Time
}

var mobScrape mobScrapeController

// mobScrapeStatus is a point-in-time snapshot for the admin UI.
type mobScrapeStatus struct {
	Running   bool
	From      int
	To        int
	Current   int
	Sent      int
	StartedAt time.Time
}

func (c *mobScrapeController) snapshot() mobScrapeStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return mobScrapeStatus{
		Running: c.running, From: c.from, To: c.to,
		Current: c.current, Sent: c.sent, StartedAt: c.startedAt,
	}
}

// stop cancels an in-flight sweep (no-op if none is running).
func (c *mobScrapeController) stop() bool {
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

// runMobInfoSweep is the scheduler job function. It sends "@mobinfo <id>" for
// every id in the configured range, pacing by the configured delay, and stops
// early on cancel (admin Stop button) or injection failure.
func runMobInfoSweep() {
	from, to, delayMs := loadMobScrapeConfig()
	if from <= 0 || to < from {
		log.Printf("[E] %s invalid mob id range %d-%d", mobScrapeLogTag, from, to)
		return
	}
	if ready, name := zoneProxyReady(); !ready || name == "" {
		log.Printf("[E] %s zone proxy not ready (active=%v, charName=%q); log into the game first (and enable ZONE_PROXY).",
			mobScrapeLogTag, ready, name)
		return
	}

	// Derive from the app shutdown context so a SIGTERM aborts an in-flight
	// sweep instead of blocking bgWg.Wait() for up to the whole id range. The
	// Stop button cancels this same context via mobScrape.cancel.
	base := bgCtx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancel(base)

	mobScrape.mu.Lock()
	if mobScrape.running {
		mobScrape.mu.Unlock()
		cancel()
		log.Printf("[W] %s a sweep is already running; ignoring trigger.", mobScrapeLogTag)
		return
	}
	mobScrape.running = true
	mobScrape.cancel = cancel
	mobScrape.from, mobScrape.to, mobScrape.current, mobScrape.sent = from, to, from, 0
	mobScrape.startedAt = time.Now()
	mobScrape.mu.Unlock()

	defer func() {
		mobScrape.mu.Lock()
		mobScrape.running = false
		mobScrape.cancel = nil
		mobScrape.mu.Unlock()
		cancel()
	}()

	delay := time.Duration(delayMs) * time.Millisecond
	log.Printf("[I] %s starting sweep %d-%d, %v between commands.", mobScrapeLogTag, from, to, delay)

	for id := from; id <= to; id++ {
		select {
		case <-ctx.Done():
			log.Printf("[I] %s sweep stopped at id %d.", mobScrapeLogTag, id)
			return
		default:
		}

		if err := injectChatCommand(fmt.Sprintf("@mobinfo %d", id)); err != nil {
			log.Printf("[E] %s inject failed at id %d: %v", mobScrapeLogTag, id, err)
			return
		}

		mobScrape.mu.Lock()
		mobScrape.current = id
		mobScrape.sent++
		mobScrape.mu.Unlock()

		// Pace between commands, but wake immediately on a stop request.
		select {
		case <-ctx.Done():
			log.Printf("[I] %s sweep stopped at id %d.", mobScrapeLogTag, id)
			return
		case <-time.After(delay):
		}
	}

	final := mobScrape.snapshot()
	log.Printf("[I] %s sweep complete: %d commands sent (%d-%d).", mobScrapeLogTag, final.Sent, from, to)
}

// --- config persistence (mobscrape_config, single row id=1) ---

func defaultMobScrapeConfig() (from, to, delayMs int) {
	if appConfig != nil {
		return appConfig.MobScrapeFromID, appConfig.MobScrapeToID, appConfig.MobScrapeDelayMs
	}
	return 1001, 2500, 400
}

// loadMobScrapeConfig returns the persisted sweep parameters, falling back to
// the configured first-run defaults when no row exists yet.
func loadMobScrapeConfig() (from, to, delayMs int) {
	from, to, delayMs = defaultMobScrapeConfig()
	if srv == nil || srv.db == nil {
		return
	}
	var f, t, d int
	err := srv.db.QueryRow(`SELECT from_id, to_id, delay_ms FROM mobscrape_config WHERE id = 1`).Scan(&f, &t, &d)
	if err == nil {
		return f, t, d
	}
	return
}

func saveMobScrapeConfig(from, to, delayMs int) error {
	if srv == nil || srv.db == nil {
		return fmt.Errorf("database not ready")
	}
	_, err := srv.db.Exec(`
		INSERT INTO mobscrape_config (id, from_id, to_id, delay_ms) VALUES (1, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET from_id=excluded.from_id, to_id=excluded.to_id, delay_ms=excluded.delay_ms`,
		from, to, delayMs)
	return err
}
