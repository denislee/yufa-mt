package server

import (
	"bytes"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/denislee/yufa-mt/internal/storage"
)

// setupSchedulerTestDB wires srv to a fresh temp database and routes stdlib
// log output into the in-memory ring buffer (as initLogger does in production)
// so runOnce's log-scan classification can be exercised.
func setupSchedulerTestDB(t *testing.T) {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "sched_test.db"), nil)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	srv = &App{db: db}
	initLogger() // tee log output to logBuffer
	t.Cleanup(func() {
		_ = storage.Close(db)
		srv = nil
	})
}

func newTestJob(name, logTag string, fn func()) *jobState {
	return &jobState{
		spec:     JobSpec{Name: name, Label: name, LogTag: logTag, Func: fn},
		interval: time.Minute,
		enabled:  true,
		resetCh:  make(chan struct{}, 1),
		enableCh: make(chan struct{}, 1),
		runCh:    make(chan string, 1),
	}
}

func latestRun(t *testing.T, job string) (status, message string) {
	t.Helper()
	err := srv.db.QueryRow(
		`SELECT status, COALESCE(message, '') FROM job_runs WHERE job_name = ? ORDER BY id DESC LIMIT 1`,
		job).Scan(&status, &message)
	if err != nil {
		t.Fatalf("no job_runs row for %q: %v", job, err)
	}
	return status, message
}

func TestJobConfigRoundTrip(t *testing.T) {
	setupSchedulerTestDB(t)

	// Absent row → code default, enabled.
	if d, en := loadJobConfig("market", 3*time.Minute); d != 3*time.Minute || !en {
		t.Fatalf("default load = (%v, %v), want (3m, true)", d, en)
	}

	// Persisted value round-trips, including the disabled flag.
	if err := saveJobConfig("market", 90*time.Second, false); err != nil {
		t.Fatalf("saveJobConfig: %v", err)
	}
	if d, en := loadJobConfig("market", 3*time.Minute); d != 90*time.Second || en {
		t.Fatalf("load after save = (%v, %v), want (90s, false)", d, en)
	}

	// A persisted value below the minimum is clamped on load.
	if err := saveJobConfig("market", 5*time.Second, true); err != nil {
		t.Fatalf("saveJobConfig: %v", err)
	}
	if d, _ := loadJobConfig("market", 3*time.Minute); d != minJobInterval {
		t.Fatalf("load of sub-minimum = %v, want clamp to %v", d, minJobInterval)
	}
}

func TestRunOnceClassifiesOutcome(t *testing.T) {
	setupSchedulerTestDB(t)
	s := &scheduler{jobs: map[string]*jobState{}}

	// A clean run with no [E] lines → success.
	ok := newTestJob("ok", "[Scraper/OK]", func() {
		log.Println("[I] [Scraper/OK] did some work")
	})
	s.runOnce(ok, "manual")
	if status, _ := latestRun(t, "ok"); status != "success" {
		t.Errorf("clean run status = %q, want success", status)
	}

	// A run that logs its own [E] line → error, message captured.
	boom := newTestJob("boom", "[Scraper/Boom]", func() {
		log.Println("[E] [Scraper/Boom] upstream unreachable")
	})
	s.runOnce(boom, "manual")
	status, msg := latestRun(t, "boom")
	if status != "error" {
		t.Errorf("failing run status = %q, want error", status)
	}
	if msg == "" {
		t.Errorf("failing run captured empty message, want the [E] log line")
	}

	// Another job's [E] line must not bleed into this job's classification.
	quiet := newTestJob("quiet", "[Scraper/Quiet]", func() {
		log.Println("[E] [Scraper/Other] not my error")
		log.Println("[I] [Scraper/Quiet] all good")
	})
	s.runOnce(quiet, "manual")
	if status, _ := latestRun(t, "quiet"); status != "success" {
		t.Errorf("run with foreign error line status = %q, want success", status)
	}

	// A panic is recovered and recorded as an error.
	panicky := newTestJob("panicky", "[Scraper/Panic]", func() {
		panic("kaboom")
	})
	s.runOnce(panicky, "manual")
	status, msg = latestRun(t, "panicky")
	if status != "error" {
		t.Errorf("panicking run status = %q, want error", status)
	}
	if msg == "" {
		t.Errorf("panicking run captured empty message")
	}
}

// TestAdminTemplateRendersSchedulerTab executes admin.html (which the parse
// test skips) with populated scheduler data, catching any field/func mismatch
// introduced by the new tab.
func TestAdminTemplateRendersSchedulerTab(t *testing.T) {
	tmpl, ok := templateCache["admin.html"]
	if !ok {
		t.Fatal("admin.html not in templateCache")
	}
	data := AdminDashboardData{
		SchedulerJobs: []SchedulerJobView{{
			Name: "market", Label: "Market", Category: "Market & Economy",
			Enabled: true, IntervalValue: 3, IntervalUnit: "minutes",
			IntervalText: "3m0s", NextRunText: "in 2m",
			LastStatus: "error", LastRunAgo: "1 minutes ago",
			LastDuration: "812ms", LastMessage: "[E] [Scraper/Market] boom",
		}},
		JobRuns: []JobRunView{{
			JobLabel: "Market", Trigger: "scheduled", Status: "success",
			StartedAgo: "just now", StartedAt: "2026-06-01T00:00:00Z", Duration: "1.2s",
		}},
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		t.Fatalf("admin.html execute: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"Scheduled Jobs", "Recent Runs", "Market", "Run Now", "/admin/scheduler/config"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered admin page missing %q", want)
		}
	}
}

func TestSplitInterval(t *testing.T) {
	cases := []struct {
		d     time.Duration
		value int
		unit  string
	}{
		{3 * time.Minute, 3, "minutes"},
		{6 * time.Hour, 6, "hours"},
		{90 * time.Second, 90, "seconds"},
		{2 * time.Hour, 2, "hours"},
	}
	for _, c := range cases {
		if v, u := splitInterval(c.d); v != c.value || u != c.unit {
			t.Errorf("splitInterval(%v) = (%d, %q), want (%d, %q)", c.d, v, u, c.value, c.unit)
		}
	}
}
