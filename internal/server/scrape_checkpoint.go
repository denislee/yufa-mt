package server

import (
	"encoding/json"
	"log/slog"
	"time"
)

// Scrape checkpointing lets a long, paginated scrape resume after a process
// restart instead of re-fetching every page from scratch. Each completed page
// is recorded as a row in scrape_checkpoint (page → JSON of that page's
// records). On the next run the scraper reloads the accumulated pages and
// continues from the first un-scraped page; the rows are cleared once the run
// saves successfully. See scrapePlayerCharacters for the only current user.

// loadScrapeCheckpoint returns the records accumulated by a prior interrupted
// run for jobName and the page to resume scraping from (1 when starting fresh).
//
// The checkpoint is discarded — and a clean restart from page 1 signalled — when
// it is empty, its stored total_pages no longer matches the current page count
// (the upstream dataset changed shape), or its newest page is older than maxAge
// (a stale half-run not worth finishing). Pages are written sequentially, so the
// stored rows are contiguous and resuming from max(page)+1 is correct.
func loadScrapeCheckpoint(jobName string, totalPages int, maxAge time.Duration) (players []PlayerCharacter, resumeFrom int) {
	rows, err := srv.db.Query(
		`SELECT page, total_pages, payload, updated_at FROM scrape_checkpoint WHERE job_name = ? ORDER BY page`,
		jobName)
	if err != nil {
		slog.Warn("Failed to read scrape checkpoint", "job", jobName, "error", err)
		return nil, 1
	}
	defer rows.Close()

	maxPage := 0
	var newest time.Time
	for rows.Next() {
		var page, storedTotal int
		var payload, updatedAt string
		if err := rows.Scan(&page, &storedTotal, &payload, &updatedAt); err != nil {
			slog.Warn("Failed to scan scrape checkpoint row", "job", jobName, "error", err)
			return nil, 1
		}
		// A changed page count means the dataset moved under us; don't stitch
		// old pages onto a differently-shaped scrape.
		if storedTotal != totalPages {
			slog.Info("Discarding scrape checkpoint: page count changed",
				"job", jobName, "checkpoint_total", storedTotal, "current_total", totalPages)
			clearScrapeCheckpoint(jobName)
			return nil, 1
		}
		var pagePlayers []PlayerCharacter
		if err := json.Unmarshal([]byte(payload), &pagePlayers); err != nil {
			slog.Warn("Discarding scrape checkpoint: corrupt payload", "job", jobName, "page", page, "error", err)
			clearScrapeCheckpoint(jobName)
			return nil, 1
		}
		players = append(players, pagePlayers...)
		if page > maxPage {
			maxPage = page
		}
		if t, perr := time.Parse(time.RFC3339, updatedAt); perr == nil && t.After(newest) {
			newest = t
		}
	}

	if maxPage == 0 {
		return nil, 1
	}
	if !newest.IsZero() && time.Since(newest) >= maxAge {
		slog.Info("Discarding scrape checkpoint: stale", "job", jobName, "age", time.Since(newest).Round(time.Second))
		clearScrapeCheckpoint(jobName)
		return nil, 1
	}
	return players, maxPage + 1
}

// saveScrapeCheckpointPage records (or replaces) one page's scraped records so
// the run can resume from the next page if interrupted.
func saveScrapeCheckpointPage(jobName string, page, totalPages int, players []PlayerCharacter) {
	payload, err := json.Marshal(players)
	if err != nil {
		slog.Warn("Failed to encode scrape checkpoint page", "job", jobName, "page", page, "error", err)
		return
	}
	_, err = srv.db.Exec(`
		INSERT INTO scrape_checkpoint (job_name, page, total_pages, payload, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(job_name, page) DO UPDATE SET
			total_pages = excluded.total_pages,
			payload = excluded.payload,
			updated_at = excluded.updated_at`,
		jobName, page, totalPages, string(payload), time.Now().Format(time.RFC3339))
	if err != nil {
		slog.Warn("Failed to persist scrape checkpoint page", "job", jobName, "page", page, "error", err)
	}
}

// clearScrapeCheckpoint drops all checkpoint rows for a job, called once its run
// has saved successfully (or when a stale/mismatched checkpoint is discarded).
func clearScrapeCheckpoint(jobName string) {
	if _, err := srv.db.Exec(`DELETE FROM scrape_checkpoint WHERE job_name = ?`, jobName); err != nil {
		slog.Warn("Failed to clear scrape checkpoint", "job", jobName, "error", err)
	}
}
