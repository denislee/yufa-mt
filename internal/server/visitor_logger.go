package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// pageViewLog is one row that the visitor logger batches into the
// page_views table.
type pageViewLog struct {
	VisitorHash string
	PageURI     string
	Timestamp   string
}

// pageViewChannel is a bounded buffer between the request middleware
// (visitorTracker) and the background batch flusher (startVisitorLogger).
// A full channel drops the page view rather than blocking the request.
var pageViewChannel = make(chan pageViewLog, 1000)

// getVisitorHash returns a stable hash for the requesting visitor based
// on (X-Forwarded-For or RemoteAddr) + User-Agent.
func getVisitorHash(r *http.Request) string {
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	} else {
		ip = strings.Split(ip, ",")[0]
	}

	ua := r.UserAgent()
	data := fmt.Sprintf("%s-%s", ip, ua)

	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// visitorTracker is the middleware that enqueues a pageViewLog for each
// public request and then calls the next handler.
func visitorTracker(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entry := pageViewLog{
			VisitorHash: getVisitorHash(r),
			PageURI:     r.URL.RequestURI(),
			Timestamp:   time.Now().Format(time.RFC3339),
		}
		select {
		case pageViewChannel <- entry:
		default:
			log.Println("[W] [Logger] Page view log channel is full. Dropping a page view.")
		}
		next.ServeHTTP(w, r)
	}
}

func flushBatchIfFull(batch []pageViewLog, maxSize int) []pageViewLog {
	if len(batch) >= maxSize {
		flushVisitorBatchToDB(batch)
		return nil
	}
	return batch
}

// flushVisitorBatchToDB upserts visitors and inserts page views from one
// batch inside a single transaction. Per-row failures are counted, not
// logged individually, to avoid drowning the logs.
func flushVisitorBatchToDB(batch []pageViewLog) {
	if len(batch) == 0 {
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("[E] [Logger] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	visitorStmt, err := tx.Prepare(`
		INSERT INTO visitors (visitor_hash, first_visit, last_visit)
		VALUES (?, ?, ?)
		ON CONFLICT(visitor_hash) DO UPDATE SET
			last_visit = excluded.last_visit;
	`)
	if err != nil {
		log.Printf("[E] [Logger] Failed to prepare visitor statement: %v", err)
		return
	}
	defer visitorStmt.Close()

	viewStmt, err := tx.Prepare(`
		INSERT INTO page_views (visitor_hash, page_path, view_timestamp)
		VALUES (?, ?, ?);
	`)
	if err != nil {
		log.Printf("[E] [Logger] Failed to prepare page_view statement: %v", err)
		return
	}
	defer viewStmt.Close()

	visitorsProcessed := make(map[string]bool)
	var visitorErrors, viewErrors int

	for _, entry := range batch {
		if !visitorsProcessed[entry.VisitorHash] {
			if _, err := visitorStmt.Exec(entry.VisitorHash, entry.Timestamp, entry.Timestamp); err != nil {
				visitorErrors++
			}
			visitorsProcessed[entry.VisitorHash] = true
		}
		if _, err := viewStmt.Exec(entry.VisitorHash, entry.PageURI, entry.Timestamp); err != nil {
			viewErrors++
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[E] [Logger] Failed to commit batch: %v", err)
	} else {
		log.Printf("[I] [Logger] Flushed %d views. (Visitor upsert errors: %d, View insert errors: %d)",
			len(batch), visitorErrors, viewErrors)
	}
}

// startVisitorLogger runs the main loop for batch-processing page views.
// On ctx cancellation it drains the channel and flushes a final partial
// batch before returning.
func startVisitorLogger(ctx context.Context) {
	var batch []pageViewLog
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	const batchSize = 100

	for {
		select {
		case <-ctx.Done():
			log.Println("[I] [Logger] Shutdown signal received. Draining channel...")
			ticker.Stop()
			for {
				select {
				case entry := <-pageViewChannel:
					batch = append(batch, entry)
					batch = flushBatchIfFull(batch, batchSize)
				default:
					log.Println("[I] [Logger] Flushing final batch...")
					flushVisitorBatchToDB(batch)
					log.Println("[I] [Logger] Shut down gracefully.")
					return
				}
			}

		case entry := <-pageViewChannel:
			batch = append(batch, entry)
			batch = flushBatchIfFull(batch, batchSize)

		case <-ticker.C:
			flushVisitorBatchToDB(batch)
			batch = nil
		}
	}
}
