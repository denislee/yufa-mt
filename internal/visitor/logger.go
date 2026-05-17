// Package visitor batches page-view records and inserts them into the
// visitors / page_views tables in 10-second intervals or whenever the
// batch reaches BatchSize, whichever comes first. The Track middleware
// enqueues views from the HTTP request path; Run consumes the channel.
package visitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// BatchSize is the in-memory threshold that triggers a flush.
const BatchSize = 100

// FlushInterval is the periodic flush cadence.
const FlushInterval = 10 * time.Second

// PageView is one row queued for the page_views table.
type PageView struct {
	VisitorHash string
	PageURI     string
	Timestamp   string
}

// Logger owns the bounded channel between request middleware and the
// background batch flusher. Construct one with New, install Track on
// public routes, and start Run in a background goroutine.
type Logger struct {
	db *sql.DB
	ch chan PageView
}

// New returns a Logger backed by db with a 1000-slot channel. A full
// channel drops events rather than blocking the request.
func New(db *sql.DB) *Logger {
	return &Logger{db: db, ch: make(chan PageView, 1000)}
}

// Track is the middleware that enqueues a PageView for each request and
// then calls next.
func (l *Logger) Track(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entry := PageView{
			VisitorHash: hashVisitor(r),
			PageURI:     r.URL.RequestURI(),
			Timestamp:   time.Now().Format(time.RFC3339),
		}
		select {
		case l.ch <- entry:
		default:
			log.Println("[W] [Logger] Page view log channel is full. Dropping a page view.")
		}
		next.ServeHTTP(w, r)
	}
}

// Run is the main loop. On ctx cancellation it drains the channel and
// flushes a final partial batch before returning.
func (l *Logger) Run(ctx context.Context) {
	var batch []PageView
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[I] [Logger] Shutdown signal received. Draining channel...")
			ticker.Stop()
			for {
				select {
				case entry := <-l.ch:
					batch = append(batch, entry)
					batch = l.flushIfFull(batch)
				default:
					log.Println("[I] [Logger] Flushing final batch...")
					l.flush(batch)
					log.Println("[I] [Logger] Shut down gracefully.")
					return
				}
			}

		case entry := <-l.ch:
			batch = append(batch, entry)
			batch = l.flushIfFull(batch)

		case <-ticker.C:
			l.flush(batch)
			batch = nil
		}
	}
}

func (l *Logger) flushIfFull(batch []PageView) []PageView {
	if len(batch) >= BatchSize {
		l.flush(batch)
		return nil
	}
	return batch
}

func (l *Logger) flush(batch []PageView) {
	if len(batch) == 0 {
		return
	}

	tx, err := l.db.Begin()
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

// hashVisitor returns a stable hash for the requesting visitor based on
// (X-Forwarded-For or RemoteAddr) + User-Agent.
func hashVisitor(r *http.Request) string {
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
