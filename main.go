package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal" // Added for graceful shutdown
	"strings"
	"syscall" // Added for graceful shutdown
	"time"

	"github.com/joho/godotenv"
)

func getVisitorHash(r *http.Request) string {
	// This function remains unchanged, but is included for context
	// as it's a small function in the same file.
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

type pageViewLog struct {
	VisitorHash string
	PageURI     string
	Timestamp   string
}

var pageViewChannel = make(chan pageViewLog, 1000)

// flushVisitorBatchToDB commits a batch of page views to the database.
// This version simplifies logging by batching error counts instead of
// logging every single failed insert, which is more performant.
func flushVisitorBatchToDB(batch []pageViewLog) {
	if len(batch) == 0 {
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ Visitor Logger: Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback() // Rollback on error

	visitorStmt, err := tx.Prepare(`
		INSERT INTO visitors (visitor_hash, first_visit, last_visit)
		VALUES (?, ?, ?)
		ON CONFLICT(visitor_hash) DO UPDATE SET
			last_visit = excluded.last_visit;
	`)
	if err != nil {
		log.Printf("❌ Visitor Logger: Failed to prepare visitor statement: %v", err)
		return
	}
	defer visitorStmt.Close()

	viewStmt, err := tx.Prepare(`
		INSERT INTO page_views (visitor_hash, page_path, view_timestamp)
		VALUES (?, ?, ?);
	`)
	if err != nil {
		log.Printf("❌ Visitor Logger: Failed to prepare page_view statement: %v", err)
		return
	}
	defer viewStmt.Close()

	visitorsProcessed := make(map[string]bool)
	var visitorErrors, viewErrors int

	for _, logEntry := range batch {
		// Only upsert the visitor's 'last_visit' time once per batch
		if !visitorsProcessed[logEntry.VisitorHash] {
			_, err := visitorStmt.Exec(logEntry.VisitorHash, logEntry.Timestamp, logEntry.Timestamp)
			if err != nil {
				// Don't log here, just count
				visitorErrors++
			}
			visitorsProcessed[logEntry.VisitorHash] = true
		}

		// Log the page view
		_, err := viewStmt.Exec(logEntry.VisitorHash, logEntry.PageURI, logEntry.Timestamp)
		if err != nil {
			// Don't log here, just count
			viewErrors++
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ Visitor Logger: Failed to commit batch: %v", err)
	} else {
		log.Printf(
			"📝 Visitor Logger: Flushed %d views. (Visitor upsert errors: %d, View insert errors: %d)",
			len(batch), visitorErrors, viewErrors,
		)
	}
}

// startVisitorLogger runs the main loop for batch-processing page views.
// This version's 'ctx.Done()' case is now functional and will be
// triggered by the graceful shutdown in main().
func startVisitorLogger(ctx context.Context) {
	var batch []pageViewLog
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	const batchSize = 100

	for {
		select {
		case <-ctx.Done():
			// Shutdown signal received.
			log.Println("🔌 Visitor Logger: Shutdown signal received. Draining channel...")

			// Stop the ticker immediately
			ticker.Stop()

			// Drain any remaining items in the channel
			for {
				select {
				case logEntry := <-pageViewChannel:
					batch = append(batch, logEntry)
					if len(batch) >= batchSize {
						flushVisitorBatchToDB(batch)
						batch = nil // Clear the batch
					}
				default:
					// Channel is empty
					log.Println("🔌 Visitor Logger: Flushing final batch...")
					flushVisitorBatchToDB(batch) // Flush the final partial batch
					log.Println("✅ Visitor Logger: Shut down gracefully.")
					return
				}
			}

		case logEntry := <-pageViewChannel:
			batch = append(batch, logEntry)
			if len(batch) >= batchSize {
				flushVisitorBatchToDB(batch)
				batch = nil // Clear the batch
			}

		case <-ticker.C:
			flushVisitorBatchToDB(batch)
			batch = nil // Clear the batch
		}
	}
}

func visitorTracker(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// This function remains unchanged
		visitorHash := getVisitorHash(r)
		now := time.Now().Format(time.RFC3339)
		pageURI := r.URL.RequestURI()

		logEntry := pageViewLog{
			VisitorHash: visitorHash,
			PageURI:     pageURI,
			Timestamp:   now,
		}

		select {
		case pageViewChannel <- logEntry:
			// Successfully queued
		default:
			// Channel is full, drop the view to avoid blocking the web request
			log.Println("⚠️ Page view log channel is full. Dropping a page view.")
		}

		next.ServeHTTP(w, r)
	}
}

// main is rewritten to support graceful shutdown.
func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("ℹ️ No .env file found, relying on system environment variables.")
	}

	var err error
	db, err = initDB("./market_data.db")
	if err != nil {
		log.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Create a context that gets cancelled on OS signals (SIGINT, SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine to listen for OS signals
	go func() {
		<-sigChan
		log.Println("🔌 Shutdown signal received. Initiating graceful shutdown...")
		cancel() // Trigger context cancellation
	}()

	// Admin Password
	adminPass = generateRandomPassword(16)
	if err := os.WriteFile("pwd.txt", []byte(adminPass), 0644); err != nil {
		log.Printf("⚠️ Could not write admin password to file: %v", err)
	} else {
		log.Println("🔑 Admin password saved to pwd.txt")
	}

	go func() {
		time.Sleep(5 * time.Second) // Give server time to start
		log.Println("==================================================")
		log.Printf("👤 Admin User: %s", adminUser)
		log.Printf("🔑 Admin Pass: %s", adminPass)
		log.Println("==================================================")
	}()

	// Start Background Services with the cancellable context
	go populateMissingCachesOnStartup() // This is short-lived, fine to run without context
	go startBackgroundJobs(ctx)
	go startDiscordBot(ctx)
	go startVisitorLogger(ctx)

	// --- Public Routes ---
	http.HandleFunc("/", visitorTracker(summaryHandler))
	http.HandleFunc("/full-list", visitorTracker(fullListHandler))
	http.HandleFunc("/item", visitorTracker(itemHistoryHandler))
	http.HandleFunc("/activity", visitorTracker(activityHandler))
	http.HandleFunc("/players", visitorTracker(playerCountHandler))
	http.HandleFunc("/characters", visitorTracker(characterHandler))
	http.HandleFunc("/guilds", visitorTracker(guildHandler))
	http.HandleFunc("/guild", visitorTracker(guildDetailHandler))
	http.HandleFunc("/mvp-kills", visitorTracker(mvpKillsHandler))
	http.HandleFunc("/character", visitorTracker(characterDetailHandler))
	http.HandleFunc("/character-changelog", visitorTracker(characterChangelogHandler))
	http.HandleFunc("/store", visitorTracker(storeDetailHandler))
	http.HandleFunc("/discord", visitorTracker(tradingPostListHandler))

	// --- Admin Dashboard & Tools ---
	http.HandleFunc("/admin", basicAuth(adminHandler))
	http.HandleFunc("/admin/parse-trade", basicAuth(adminParseTradeHandler))
	http.HandleFunc("/admin/views/delete-visitor", basicAuth(adminDeleteVisitorViewsHandler))
	http.HandleFunc("/admin/guild/update-emblem", basicAuth(adminUpdateGuildEmblemHandler))
	http.HandleFunc("/admin/character/clear-last-active", basicAuth(adminClearLastActiveHandler))
	http.HandleFunc("/admin/character/clear-mvp-kills", basicAuth(adminClearMvpKillsHandler))

	// --- Admin RMS Cache Management ---
	http.HandleFunc("/admin/cache", basicAuth(adminCacheActionHandler))
	http.HandleFunc("/admin/cache/delete-entry", basicAuth(adminDeleteCacheEntryHandler))
	http.HandleFunc("/admin/cache/save-entry", basicAuth(adminSaveCacheEntryHandler))

	// --- Admin Trading Post Management ---
	http.HandleFunc("/admin/trading-post/delete", basicAuth(adminDeleteTradingPostHandler))
	http.HandleFunc("/admin/trading-post/edit", basicAuth(adminEditTradingPostHandler))
	http.HandleFunc("/admin/trading-post/reparse", basicAuth(adminReparseTradingPostHandler))
	http.HandleFunc("/admin/trading/clear-items", basicAuth(adminClearTradingPostItemsHandler))
	http.HandleFunc("/admin/trading/clear-posts", basicAuth(adminClearTradingPostsHandler))

	// --- Admin Manual Scrape Triggers ---
	http.HandleFunc("/admin/scrape/market", basicAuth(adminTriggerScrapeHandler(scrapeData, "Market")))
	http.HandleFunc("/admin/scrape/players", basicAuth(adminTriggerScrapeHandler(scrapeAndStorePlayerCount, "Player-Count")))
	http.HandleFunc("/admin/scrape/characters", basicAuth(adminTriggerScrapeHandler(scrapePlayerCharacters, "Character")))
	http.HandleFunc("/admin/scrape/guilds", basicAuth(adminTriggerScrapeHandler(scrapeGuilds, "Guild")))
	http.HandleFunc("/admin/scrape/zeny", basicAuth(adminTriggerScrapeHandler(scrapeZeny, "Zeny")))
	http.HandleFunc("/admin/scrape/mvp", basicAuth(adminTriggerScrapeHandler(scrapeMvpKills, "MVP")))
	http.HandleFunc("/admin/scrape/rms-cache", basicAuth(adminTriggerScrapeHandler(runFullRMSCacheJob, "RMS-Cache-Refresh")))

	// --- Server Start and Shutdown ---
	port := "8080"
	server := &http.Server{Addr: ":" + port}

	// Goroutine to handle server shutdown when context is cancelled
	go func() {
		<-ctx.Done() // Wait for the cancel() signal
		log.Println("🔌 Shutting down web server...")

		// Create a new context for the shutdown, with a timeout
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("❌ Web server graceful shutdown failed: %v", err)
		}
	}()

	// Start server and block
	log.Printf("🚀 Web server started. Open http://localhost:%s in your browser.", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("❌ Web server failed to start: %v", err)
	}

	// This line will be reached after server.Shutdown() completes
	log.Println("✅ All services shut down. Exiting.")
}

