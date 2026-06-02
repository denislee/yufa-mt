package server

import (
	"testing"
	"time"
)

func TestScrapeCheckpointRoundTrip(t *testing.T) {
	setupSchedulerTestDB(t)

	// No checkpoint yet → start fresh from page 1.
	if players, from := loadScrapeCheckpoint("characters", 5, time.Hour); players != nil || from != 1 {
		t.Fatalf("empty checkpoint = (%v, %d), want (nil, 1)", players, from)
	}

	// Record three contiguous pages of a five-page run.
	saveScrapeCheckpointPage("characters", 1, 5, []PlayerCharacter{{Name: "a"}, {Name: "b"}})
	saveScrapeCheckpointPage("characters", 2, 5, []PlayerCharacter{{Name: "c"}})
	saveScrapeCheckpointPage("characters", 3, 5, []PlayerCharacter{{Name: "d"}, {Name: "e"}})

	players, from := loadScrapeCheckpoint("characters", 5, time.Hour)
	if from != 4 {
		t.Errorf("resume page = %d, want 4 (max scraped page + 1)", from)
	}
	if len(players) != 5 {
		t.Fatalf("accumulated %d players, want 5", len(players))
	}
	if players[0].Name != "a" || players[4].Name != "e" {
		t.Errorf("players out of order: %q..%q", players[0].Name, players[4].Name)
	}

	// Re-saving a page replaces its payload (idempotent resume of the same page).
	saveScrapeCheckpointPage("characters", 3, 5, []PlayerCharacter{{Name: "d"}})
	if players, from := loadScrapeCheckpoint("characters", 5, time.Hour); from != 4 || len(players) != 4 {
		t.Errorf("after page replace = (%d players, resume %d), want (4, 4)", len(players), from)
	}
}

func TestScrapeCheckpointDiscardedOnPageCountChange(t *testing.T) {
	setupSchedulerTestDB(t)
	saveScrapeCheckpointPage("characters", 1, 5, []PlayerCharacter{{Name: "a"}})

	// Upstream now reports a different page count → checkpoint is discarded.
	players, from := loadScrapeCheckpoint("characters", 7, time.Hour)
	if players != nil || from != 1 {
		t.Fatalf("page-count mismatch = (%v, %d), want (nil, 1)", players, from)
	}
	// ...and the stale rows were cleared, so a matching count won't resurrect them.
	if _, from := loadScrapeCheckpoint("characters", 5, time.Hour); from != 1 {
		t.Errorf("discarded checkpoint still present: resume = %d, want 1", from)
	}
}

func TestScrapeCheckpointDiscardedWhenStale(t *testing.T) {
	setupSchedulerTestDB(t)
	saveScrapeCheckpointPage("characters", 1, 5, []PlayerCharacter{{Name: "a"}})

	// A non-positive maxAge makes any checkpoint stale.
	if players, from := loadScrapeCheckpoint("characters", 5, -time.Second); players != nil || from != 1 {
		t.Fatalf("stale checkpoint = (%v, %d), want (nil, 1)", players, from)
	}
	if _, from := loadScrapeCheckpoint("characters", 5, time.Hour); from != 1 {
		t.Errorf("stale checkpoint not cleared: resume = %d, want 1", from)
	}
}

func TestScrapeCheckpointClear(t *testing.T) {
	setupSchedulerTestDB(t)
	saveScrapeCheckpointPage("characters", 1, 2, []PlayerCharacter{{Name: "a"}})
	clearScrapeCheckpoint("characters")
	if players, from := loadScrapeCheckpoint("characters", 2, time.Hour); players != nil || from != 1 {
		t.Fatalf("after clear = (%v, %d), want (nil, 1)", players, from)
	}
}

func TestLastSuccessNextRun(t *testing.T) {
	setupSchedulerTestDB(t)
	const interval = time.Hour

	// No history → don't skip.
	if _, skip := lastSuccessNextRun("characters", interval); skip {
		t.Error("no history should not skip the startup run")
	}

	insertRun := func(status string, finished time.Time) {
		_, err := srv.db.Exec(
			`INSERT INTO job_runs (job_name, trigger, status, started_at, finished_at) VALUES (?, 'scheduled', ?, ?, ?)`,
			"characters", status, finished.Add(-time.Minute).Format(time.RFC3339), finished.Format(time.RFC3339))
		if err != nil {
			t.Fatalf("insert run: %v", err)
		}
	}

	// A success well within one interval → skip, next aligned to finish+interval.
	recent := time.Now().Add(-10 * time.Minute)
	insertRun("success", recent)
	next, skip := lastSuccessNextRun("characters", interval)
	if !skip {
		t.Fatal("recent success should skip the startup run")
	}
	if want := recent.Add(interval); next.Sub(want).Abs() > time.Second {
		t.Errorf("next run = %v, want ~%v", next, want)
	}

	// A success older than one interval → don't skip.
	if _, err := srv.db.Exec(`DELETE FROM job_runs`); err != nil {
		t.Fatalf("clear runs: %v", err)
	}
	insertRun("success", time.Now().Add(-2*interval))
	if _, skip := lastSuccessNextRun("characters", interval); skip {
		t.Error("stale success should not skip the startup run")
	}

	// Only failed runs on record → don't skip.
	if _, err := srv.db.Exec(`DELETE FROM job_runs`); err != nil {
		t.Fatalf("clear runs: %v", err)
	}
	insertRun("error", time.Now().Add(-time.Minute))
	if _, skip := lastSuccessNextRun("characters", interval); skip {
		t.Error("a recent failure should not count as a recent success")
	}
}

func TestMarkInterruptedRuns(t *testing.T) {
	setupSchedulerTestDB(t)
	res, err := srv.db.Exec(
		`INSERT INTO job_runs (job_name, trigger, status, started_at) VALUES ('characters', 'startup', 'running', ?)`,
		time.Now().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert running row: %v", err)
	}
	id, _ := res.LastInsertId()

	markInterruptedRuns()

	var status string
	var finished *string
	if err := srv.db.QueryRow(`SELECT status, finished_at FROM job_runs WHERE id = ?`, id).Scan(&status, &finished); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != "interrupted" {
		t.Errorf("status = %q, want interrupted", status)
	}
	if finished == nil {
		t.Error("interrupted run should have a finished_at stamp")
	}
}
