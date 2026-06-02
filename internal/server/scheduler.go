package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// minJobInterval is the smallest interval an admin may configure. It guards
// against accidentally hammering the upstream server (or this app) with a
// pathologically short schedule.
const minJobInterval = 30 * time.Second

// jobRunHistoryLimit is the number of most-recent runs kept per job; older
// rows are pruned after each run to bound the job_runs table's growth.
const jobRunHistoryLimit = 200

// JobSpec is the static definition of a schedulable background job. LogTag is
// the log prefix the job's function emits (e.g. "[Scraper/Market]"); the
// scheduler scans the in-memory log ring for that tag combined with "[E]" to
// decide whether a run failed, since the scrape functions log errors rather
// than returning them.
type JobSpec struct {
	Name            string // stable key, also the job_config/job_runs key
	Label           string // human-facing name
	Category        string // grouping shown in the admin UI
	Func            func()
	LogTag          string
	DefaultInterval time.Duration
	// DefaultDisabled makes a brand-new job (no job_config row yet) start
	// disabled instead of the usual enabled-by-default. Used for jobs that must
	// not auto-run on first boot — e.g. the @mobinfo injection sweep.
	DefaultDisabled bool
}

// jobRegistry is the ordered set of jobs the scheduler manages. It mirrors the
// jobs that startBackgroundJobs used to launch inline. Manual-only triggers
// (emblems, PT-name populator) are intentionally absent — they are not
// scheduled and keep their direct admin handlers.
var jobRegistry = []JobSpec{
	{Name: "market", Label: "Market", Category: "Market & Economy", Func: scrapeData, LogTag: "[Scraper/Market]", DefaultInterval: 3 * time.Minute},
	{Name: "players", Label: "Player Count", Category: "Players & Characters", Func: scrapeAndStorePlayerCount, LogTag: "[Scraper/PlayerCount]", DefaultInterval: 1 * time.Minute},
	{Name: "characters", Label: "Player Character", Category: "Players & Characters", Func: scrapePlayerCharacters, LogTag: "[Scraper/Char]", DefaultInterval: 6 * time.Hour},
	{Name: "guilds", Label: "Guild", Category: "Guilds & Events", Func: scrapeGuilds, LogTag: "[Scraper/Guild]", DefaultInterval: 1 * time.Hour},
	{Name: "zeny", Label: "Zeny", Category: "Market & Economy", Func: scrapeZeny, LogTag: "[Scraper/Zeny]", DefaultInterval: 6 * time.Hour},
	{Name: "mvp", Label: "MVP Kill", Category: "Players & Characters", Func: scrapeMvpKills, LogTag: "[Scraper/MVP]", DefaultInterval: 5 * time.Minute},
	{Name: "woe", Label: "WoE Char Rankings", Category: "Guilds & Events", Func: scrapeWoeCharacterRankings, LogTag: "[Scraper/WoE]", DefaultInterval: 12 * time.Hour},
	{Name: "mobinfo", Label: "Mob Info Scrape (@mobinfo)", Category: "Reference Data", Func: runMobInfoSweep, LogTag: "[Scraper/MobInfo]", DefaultInterval: 24 * time.Hour, DefaultDisabled: true},
}

// globalScheduler is the running scheduler, set by startBackgroundJobs.
var globalScheduler *scheduler

// jobState is the live, mutable state of a single job. All scalar fields are
// guarded by mu; the control channels signal the job's loop goroutine to
// reconcile against the (already-updated) state.
type jobState struct {
	spec JobSpec

	mu       sync.RWMutex
	interval time.Duration
	enabled  bool
	nextRun  time.Time

	running atomic.Bool

	resetCh  chan struct{} // interval changed; loop should ticker.Reset
	enableCh chan struct{} // enabled flag changed; loop should start/stop ticking
	runCh    chan string   // manual trigger carrying the trigger label
}

func (js *jobState) getInterval() time.Duration {
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.interval
}

func (js *jobState) setInterval(d time.Duration) {
	js.mu.Lock()
	js.interval = d
	js.mu.Unlock()
}

func (js *jobState) isEnabled() bool {
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.enabled
}

func (js *jobState) setEnabled(v bool) {
	js.mu.Lock()
	js.enabled = v
	js.mu.Unlock()
}

func (js *jobState) getNextRun() time.Time {
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.nextRun
}

func (js *jobState) setNextRun(t time.Time) {
	js.mu.Lock()
	js.nextRun = t
	js.mu.Unlock()
}

// scheduler owns all job states and exposes the admin-facing controls.
type scheduler struct {
	jobs  map[string]*jobState
	order []string // registry order, for stable UI listing
}

// newScheduler builds a scheduler from the registry, hydrating each job's
// interval and enabled flag from job_config (falling back to code defaults).
func newScheduler(specs []JobSpec) *scheduler {
	s := &scheduler{jobs: make(map[string]*jobState, len(specs))}
	for _, spec := range specs {
		interval, enabled := loadJobConfig(spec.Name, spec.DefaultInterval, !spec.DefaultDisabled)
		s.jobs[spec.Name] = &jobState{
			spec:     spec,
			interval: interval,
			enabled:  enabled,
			resetCh:  make(chan struct{}, 1),
			enableCh: make(chan struct{}, 1),
			runCh:    make(chan string, 1),
		}
		s.order = append(s.order, spec.Name)
	}
	return s
}

func (s *scheduler) job(name string) *jobState {
	if s == nil {
		return nil
	}
	return s.jobs[name]
}

// Start launches one loop goroutine per job. Each goroutine runs the job once
// immediately (preserving the previous startup behavior), then ticks on its
// interval until ctx is canceled.
func (s *scheduler) Start(ctx context.Context, wg *sync.WaitGroup) {
	for _, name := range s.order {
		js := s.jobs[name]
		wg.Add(1)
		go s.runLoop(ctx, js, wg)
	}
}

func (s *scheduler) runLoop(ctx context.Context, js *jobState, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(js.getInterval())
	defer ticker.Stop()
	if !js.isEnabled() {
		ticker.Stop()
	}

	// Initial run on startup. To avoid re-running an expensive job right after a
	// restart, skip the immediate run when a successful run finished less than
	// one interval ago, and align the first tick to that run's original cadence.
	if js.isEnabled() {
		interval := js.getInterval()
		if next, skip := lastSuccessNextRun(js.spec.Name, interval); skip {
			delay := time.Until(next)
			if delay < time.Second {
				delay = time.Second
			}
			ticker.Reset(delay)
			js.setNextRun(next)
			slog.Info("Skipping startup run; recent success on record",
				"job", js.spec.Name, "next_run", next.Format(time.RFC3339))
		} else {
			s.runOnce(js, "startup")
			js.setNextRun(time.Now().Add(interval))
		}
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopping scheduled job due to shutdown", "job", js.spec.Name)
			return
		case <-ticker.C:
			if js.isEnabled() {
				s.runOnce(js, "scheduled")
				// Re-arm at the full interval: the first tick after a skipped
				// startup run was shortened to align with the prior cadence.
				ticker.Reset(js.getInterval())
				js.setNextRun(time.Now().Add(js.getInterval()))
			}
		case <-js.resetCh:
			ticker.Reset(js.getInterval())
			if js.isEnabled() {
				js.setNextRun(time.Now().Add(js.getInterval()))
			}
		case <-js.enableCh:
			if js.isEnabled() {
				ticker.Reset(js.getInterval())
				js.setNextRun(time.Now().Add(js.getInterval()))
			} else {
				ticker.Stop()
				js.setNextRun(time.Time{})
			}
		case trigger := <-js.runCh:
			s.runOnce(js, trigger)
		}
	}
}

// runOnce executes the job, records start/finish in job_runs, and classifies
// the outcome (panic or a matching "[E]" log line ⇒ error).
func (s *scheduler) runOnce(js *jobState, trigger string) {
	spec := js.spec
	if !js.running.CompareAndSwap(false, true) {
		return // a run is already in flight (defensive; loop is single-threaded)
	}
	defer js.running.Store(false)

	startedAt := time.Now()
	id := insertJobRunStart(spec.Name, trigger, startedAt)
	mark := logBuffer.Mark()

	slog.Info("Running scheduled job", "job", spec.Name, "trigger", trigger)

	status := "success"
	message := ""
	func() {
		defer func() {
			if r := recover(); r != nil {
				status = "error"
				message = truncateMessage(fmt.Sprintf("panic: %v", r))
				slog.Error("Scheduled job panicked", "job", spec.Name, "panic", r)
			}
		}()
		spec.Func()
	}()

	if status != "error" {
		if line := scanForError(logBuffer.LinesSince(mark), spec.LogTag); line != "" {
			status = "error"
			message = truncateMessage(line)
		}
	}

	finishedAt := time.Now()
	finishJobRun(id, status, finishedAt, finishedAt.Sub(startedAt).Milliseconds(), message)
	pruneJobRuns(spec.Name, jobRunHistoryLimit)
}

// scanForError returns the first log line emitted during a run that belongs to
// the job (matches logTag) and carries the "[E]" error marker, or "" if none.
func scanForError(lines []string, logTag string) string {
	for _, line := range lines {
		if strings.Contains(line, logTag) && strings.Contains(line, "[E]") {
			return line
		}
	}
	return ""
}

func truncateMessage(s string) string {
	const max = 500
	s = strings.TrimSpace(s)
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

// UpdateInterval clamps, persists, and live-applies a new interval.
func (s *scheduler) UpdateInterval(name string, d time.Duration) error {
	if d < minJobInterval {
		d = minJobInterval
	}
	js := s.job(name)
	if js == nil {
		return fmt.Errorf("unknown job %q", name)
	}
	js.setInterval(d)
	if err := saveJobConfig(name, d, js.isEnabled()); err != nil {
		return err
	}
	wakeLoop(js.resetCh)
	return nil
}

// SetEnabled persists and live-applies the enabled flag.
func (s *scheduler) SetEnabled(name string, enabled bool) error {
	js := s.job(name)
	if js == nil {
		return fmt.Errorf("unknown job %q", name)
	}
	js.setEnabled(enabled)
	if err := saveJobConfig(name, js.getInterval(), enabled); err != nil {
		return err
	}
	wakeLoop(js.enableCh)
	return nil
}

// TriggerNow requests an immediate manual run. If a manual run is already
// queued, this is a no-op (the queued run covers the request).
func (s *scheduler) TriggerNow(name string) error {
	js := s.job(name)
	if js == nil {
		return fmt.Errorf("unknown job %q", name)
	}
	select {
	case js.runCh <- "manual":
	default:
	}
	return nil
}

// signal performs a non-blocking send on a buffered size-1 channel. Because
// the loop reconciles against the latest jobState (not the signal payload), a
// dropped signal is harmless — a pending one already triggers reconciliation.
func wakeLoop(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// JobStatusSnapshot is a point-in-time view of a job's live state for the UI.
type JobStatusSnapshot struct {
	Spec     JobSpec
	Interval time.Duration
	Enabled  bool
	Running  bool
	NextRun  time.Time
}

// Snapshots returns every job's current state in registry order.
func (s *scheduler) Snapshots() []JobStatusSnapshot {
	if s == nil {
		return nil
	}
	out := make([]JobStatusSnapshot, 0, len(s.order))
	for _, name := range s.order {
		js := s.jobs[name]
		out = append(out, JobStatusSnapshot{
			Spec:     js.spec,
			Interval: js.getInterval(),
			Enabled:  js.isEnabled(),
			Running:  js.running.Load(),
			NextRun:  js.getNextRun(),
		})
	}
	return out
}

// --- DB helpers ---

// lastSuccessNextRun reports when a job is next due based on its most recent
// successful run, and whether that success is recent enough (less than one
// interval ago) that the startup run should be skipped. This keeps a process
// restart from immediately re-running an expensive job whose data is still
// fresh; the returned time is the prior run's finish + interval, used to align
// the first tick to the original cadence.
func lastSuccessNextRun(name string, interval time.Duration) (next time.Time, skip bool) {
	var finishedStr string
	err := srv.db.QueryRow(
		`SELECT finished_at FROM job_runs
		 WHERE job_name = ? AND status = 'success' AND finished_at IS NOT NULL
		 ORDER BY started_at DESC, id DESC LIMIT 1`,
		name).Scan(&finishedStr)
	if err != nil {
		return time.Time{}, false
	}
	finished, perr := time.Parse(time.RFC3339, finishedStr)
	if perr != nil {
		return time.Time{}, false
	}
	if time.Since(finished) >= interval {
		return time.Time{}, false
	}
	return finished.Add(interval), true
}

// markInterruptedRuns flags any run still recorded as 'running' as
// 'interrupted'. Such rows are left behind when the process exits mid-run (the
// finishJobRun update never fires); rewriting them on startup keeps the run
// history honest and stops a stale 'running' row from lingering in the UI.
func markInterruptedRuns() {
	if srv == nil || srv.db == nil {
		return
	}
	if _, err := srv.db.Exec(
		`UPDATE job_runs SET status = 'interrupted', finished_at = started_at WHERE status = 'running'`); err != nil {
		slog.Warn("Failed to mark interrupted job runs", "error", err)
	}
}

// loadJobConfig returns the persisted interval/enabled for a job, or the code
// default + enabled when no row exists.
func loadJobConfig(name string, def time.Duration, defaultEnabled bool) (time.Duration, bool) {
	var secs, enabled int
	err := srv.db.QueryRow(`SELECT interval_seconds, enabled FROM job_config WHERE job_name = ?`, name).Scan(&secs, &enabled)
	if err != nil {
		return def, defaultEnabled
	}
	d := time.Duration(secs) * time.Second
	if d < minJobInterval {
		d = minJobInterval
	}
	return d, enabled != 0
}

func saveJobConfig(name string, d time.Duration, enabled bool) error {
	en := 0
	if enabled {
		en = 1
	}
	_, err := srv.db.Exec(`
		INSERT INTO job_config (job_name, interval_seconds, enabled) VALUES (?, ?, ?)
		ON CONFLICT(job_name) DO UPDATE SET interval_seconds = excluded.interval_seconds, enabled = excluded.enabled`,
		name, int(d.Seconds()), en)
	if err != nil {
		slog.Error("Failed to persist job config", "job", name, "error", err)
	}
	return err
}

func insertJobRunStart(name, trigger string, started time.Time) int64 {
	res, err := srv.db.Exec(
		`INSERT INTO job_runs (job_name, trigger, status, started_at) VALUES (?, ?, 'running', ?)`,
		name, trigger, started.Format(time.RFC3339))
	if err != nil {
		slog.Error("Failed to record job run start", "job", name, "error", err)
		return 0
	}
	id, _ := res.LastInsertId()
	return id
}

func finishJobRun(id int64, status string, finished time.Time, durationMs int64, message string) {
	if id == 0 {
		return
	}
	_, err := srv.db.Exec(
		`UPDATE job_runs SET status = ?, finished_at = ?, duration_ms = ?, message = ? WHERE id = ?`,
		status, finished.Format(time.RFC3339), durationMs, message, id)
	if err != nil {
		slog.Error("Failed to record job run finish", "id", id, "error", err)
	}
}

func pruneJobRuns(name string, keep int) {
	_, err := srv.db.Exec(
		`DELETE FROM job_runs WHERE job_name = ? AND id NOT IN (
			SELECT id FROM job_runs WHERE job_name = ? ORDER BY started_at DESC, id DESC LIMIT ?
		)`, name, name, keep)
	if err != nil {
		slog.Warn("Failed to prune job run history", "job", name, "error", err)
	}
}
