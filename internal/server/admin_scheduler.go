package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

// jobLabel maps a job's stable name to its human-facing label, for rendering
// run-history rows (which store only the name).
func jobLabel(name string) string {
	for _, spec := range jobRegistry {
		if spec.Name == name {
			return spec.Label
		}
	}
	return name
}

// splitInterval expresses a duration as a value + the largest whole unit that
// divides it evenly, so the config form pre-fills naturally (3 minutes, not
// 180 seconds).
func splitInterval(d time.Duration) (int, string) {
	secs := int(d.Seconds())
	switch {
	case secs > 0 && secs%3600 == 0:
		return secs / 3600, "hours"
	case secs > 0 && secs%60 == 0:
		return secs / 60, "minutes"
	default:
		return secs, "seconds"
	}
}

// unitToDuration converts a form value + unit into a duration.
func unitToDuration(value int, unit string) time.Duration {
	switch unit {
	case "hours":
		return time.Duration(value) * time.Hour
	case "seconds":
		return time.Duration(value) * time.Second
	default: // minutes
		return time.Duration(value) * time.Minute
	}
}

// humanizeUntil renders a positive duration as "in N units" for the next-run
// column. Negative/zero means the tick is imminent.
func humanizeUntil(d time.Duration) string {
	if d <= 0 {
		return "imminent"
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("in %ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("in %dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("in %.1fh", d.Hours())
	}
}

// humanizeDuration renders an elapsed run duration from its millisecond count.
func humanizeRunDuration(ms int64) string {
	if ms <= 0 {
		return "—"
	}
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	return (time.Duration(ms) * time.Millisecond).Round(100 * time.Millisecond).String()
}

type lastRun struct {
	status     string
	startedAt  string
	durationMs int64
	message    string
}

// getSchedulerData populates the Schedulers tab: a config/status row per job
// plus the recent cross-job run history.
func getSchedulerData(stats *AdminDashboardData) {
	// Latest run per job (MAX(id) is the newest given AUTOINCREMENT).
	lastByJob := make(map[string]lastRun)
	rows, err := srv.db.Query(`
		SELECT job_name, status, started_at, COALESCE(duration_ms, 0), COALESCE(message, '')
		FROM job_runs
		WHERE id IN (SELECT MAX(id) FROM job_runs GROUP BY job_name)`)
	if err != nil {
		log.Printf("[W] [Admin/Scheduler] Could not load last runs: %v", err)
	} else {
		for rows.Next() {
			var name string
			var lr lastRun
			if err := rows.Scan(&name, &lr.status, &lr.startedAt, &lr.durationMs, &lr.message); err == nil {
				lastByJob[name] = lr
			}
		}
		rows.Close()
	}

	for _, snap := range globalScheduler.Snapshots() {
		val, unit := splitInterval(snap.Interval)
		view := SchedulerJobView{
			Name:          snap.Spec.Name,
			Label:         snap.Spec.Label,
			Category:      snap.Spec.Category,
			Enabled:       snap.Enabled,
			Running:       snap.Running,
			IntervalValue: val,
			IntervalUnit:  unit,
			IntervalText:  snap.Interval.String(),
			NextRunText:   "—",
		}
		if snap.Enabled && !snap.NextRun.IsZero() {
			view.NextRunText = humanizeUntil(time.Until(snap.NextRun))
		}
		if lr, ok := lastByJob[snap.Spec.Name]; ok {
			view.LastStatus = lr.status
			view.LastRunAgo = timeAgo(lr.startedAt)
			view.LastDuration = humanizeRunDuration(lr.durationMs)
			view.LastMessage = lr.message
		}
		stats.SchedulerJobs = append(stats.SchedulerJobs, view)
	}

	// Dedicated @mobinfo scrape control card.
	from, to, delay := loadMobScrapeConfig()
	ready, charName := zoneProxyReady()
	snap := mobScrape.snapshot()
	mv := MobScrapeView{
		FromID: from, ToID: to, DelayMs: delay,
		ZoneReady: ready, CharName: charName,
		Running: snap.Running, Current: snap.Current, Sent: snap.Sent,
	}
	if appConfig != nil {
		mv.ZoneEnabled = appConfig.ZoneProxyEnabled
	}
	if snap.Running && !snap.StartedAt.IsZero() {
		mv.StartedAgo = timeAgo(snap.StartedAt.Format(time.RFC3339))
	}
	stats.MobScrape = mv

	// Recent runs across all jobs.
	histRows, err := srv.db.Query(`
		SELECT job_name, trigger, status, started_at, COALESCE(duration_ms, 0), COALESCE(message, '')
		FROM job_runs
		ORDER BY id DESC
		LIMIT 50`)
	if err != nil {
		log.Printf("[W] [Admin/Scheduler] Could not load run history: %v", err)
		return
	}
	defer histRows.Close()
	for histRows.Next() {
		var name, trigger, status, startedAt, message string
		var durationMs int64
		if err := histRows.Scan(&name, &trigger, &status, &startedAt, &durationMs, &message); err != nil {
			continue
		}
		stats.JobRuns = append(stats.JobRuns, JobRunView{
			JobLabel:   jobLabel(name),
			Trigger:    trigger,
			Status:     status,
			StartedAgo: timeAgo(startedAt),
			StartedAt:  startedAt,
			Duration:   humanizeRunDuration(durationMs),
			Message:    message,
		})
	}
}

// adminSchedulerConfigHandler saves a job's interval + enabled flag (combined
// in one row form) and applies it live.
func adminSchedulerConfigHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin?tab=scheduler", http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error parsing form."), http.StatusSeeOther)
		return
	}
	if globalScheduler == nil {
		http.Redirect(w, r, adminRedirectURL(r, "Scheduler is not running (scrapers disabled)."), http.StatusSeeOther)
		return
	}

	name := r.PostFormValue("job_name")
	value, err := strconv.Atoi(r.PostFormValue("interval_value"))
	if err != nil || value <= 0 {
		http.Redirect(w, r, adminRedirectURL(r, "Error: interval must be a positive number."), http.StatusSeeOther)
		return
	}
	unit := r.PostFormValue("interval_unit")
	enabled := r.PostFormValue("enabled") == "1"

	d := unitToDuration(value, unit)
	clamped := ""
	if d < minJobInterval {
		d = minJobInterval
		clamped = fmt.Sprintf(" (clamped to %s minimum)", minJobInterval)
	}

	if err := globalScheduler.UpdateInterval(name, d); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, fmt.Sprintf("Error: %v", err)), http.StatusSeeOther)
		return
	}
	if err := globalScheduler.SetEnabled(name, enabled); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, fmt.Sprintf("Error: %v", err)), http.StatusSeeOther)
		return
	}

	state := "enabled"
	if !enabled {
		state = "disabled"
	}
	msg := fmt.Sprintf("%s schedule updated: every %s, %s%s", jobLabel(name), d.String(), state, clamped)
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// adminSchedulerRunHandler triggers an immediate manual run of a job.
func adminSchedulerRunHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin?tab=scheduler", http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error parsing form."), http.StatusSeeOther)
		return
	}
	if globalScheduler == nil {
		http.Redirect(w, r, adminRedirectURL(r, "Scheduler is not running (scrapers disabled)."), http.StatusSeeOther)
		return
	}

	name := r.PostFormValue("job_name")
	if err := globalScheduler.TriggerNow(name); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, fmt.Sprintf("Error: %v", err)), http.StatusSeeOther)
		return
	}
	log.Printf("[I] [Admin/Scheduler] Manual run triggered for '%s'.", name)
	http.Redirect(w, r, adminRedirectURL(r, fmt.Sprintf("%s run triggered.", jobLabel(name))), http.StatusSeeOther)
}

// minMobScrapeDelayMs floors the inter-command delay so a misconfiguration
// can't hammer the server with back-to-back @mobinfo commands.
const minMobScrapeDelayMs = 50

// adminMobScrapeConfigHandler saves the @mobinfo sweep's id range and
// inter-command delay (mobscrape_config).
func adminMobScrapeConfigHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin?tab=scheduler", http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error parsing form."), http.StatusSeeOther)
		return
	}
	from, errF := strconv.Atoi(r.PostFormValue("from_id"))
	to, errT := strconv.Atoi(r.PostFormValue("to_id"))
	delay, errD := strconv.Atoi(r.PostFormValue("delay_ms"))
	if errF != nil || errT != nil || errD != nil || from <= 0 || to < from || delay < 0 {
		http.Redirect(w, r, adminRedirectURL(r, "Error: need from ≤ to (both > 0) and a non-negative delay."), http.StatusSeeOther)
		return
	}
	clamped := ""
	if delay < minMobScrapeDelayMs {
		delay = minMobScrapeDelayMs
		clamped = fmt.Sprintf(" (delay clamped to %dms minimum)", minMobScrapeDelayMs)
	}
	if err := saveMobScrapeConfig(from, to, delay); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, fmt.Sprintf("Error saving: %v", err)), http.StatusSeeOther)
		return
	}
	msg := fmt.Sprintf("Mob scrape config saved: ids %d–%d, %dms between commands%s", from, to, delay, clamped)
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// adminMobScrapeStopHandler cancels an in-flight @mobinfo sweep.
func adminMobScrapeStopHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin?tab=scheduler", http.StatusSeeOther)
		return
	}
	if mobScrape.stop() {
		log.Printf("[I] [Admin/Scheduler] Mob scrape sweep stop requested.")
		http.Redirect(w, r, adminRedirectURL(r, "Mob scrape sweep stopping…"), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, adminRedirectURL(r, "No mob scrape sweep is currently running."), http.StatusSeeOther)
}
