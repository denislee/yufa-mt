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

// flushBatchIfFull flushes the batch if it's full and returns a new (nil) batch.
func flushBatchIfFull(batch []pageViewLog, maxSize int) []pageViewLog {
	if len(batch) >= maxSize {
		flushVisitorBatchToDB(batch)
		return nil // Return a new, empty batch
	}
	return batch
}

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
		log.Printf("[E] [Logger] Failed to begin transaction: %v", err)
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
		log.Printf("[E] [Logger] Failed to commit batch: %v", err)
	} else {
		log.Printf(
			"[I] [Logger] Flushed %d views. (Visitor upsert errors: %d, View insert errors: %d)",
			len(batch), visitorErrors, viewErrors,
		)
	}
}

// startVisitorLogger runs the main loop for batch-processing page views.
func startVisitorLogger(ctx context.Context) {
	var batch []pageViewLog
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	const batchSize = 100

	for {
		select {
		case <-ctx.Done():
			// Shutdown signal received.
			log.Println("[I] [Logger] Shutdown signal received. Draining channel...")
			ticker.Stop()

			// Drain any remaining items in the channel
			for {
				select {
				case logEntry := <-pageViewChannel:
					batch = append(batch, logEntry)
					// Use the helper here as well
					batch = flushBatchIfFull(batch, batchSize)
				default:
					// Channel is empty
					log.Println("[I] [Logger] Flushing final batch...")
					flushVisitorBatchToDB(batch) // Flush the final partial batch
					log.Println("[I] [Logger] Shut down gracefully.")
					return
				}
			}

		case logEntry := <-pageViewChannel:
			batch = append(batch, logEntry)
			// Use the helper
			batch = flushBatchIfFull(batch, batchSize)

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
			log.Println("[W] [Logger] Page view log channel is full. Dropping a page view.")
		}

		next.ServeHTTP(w, r)
	}
}

// registerRoutes sets up all the HTTP handlers for the application.
func registerRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// --- Public Routes ---
	// Wrap public routes with the visitorTracker middleware
	mux.HandleFunc("/", visitorTracker(summaryHandler))
	mux.HandleFunc("/full-list", visitorTracker(fullListHandler))
	mux.HandleFunc("/item", visitorTracker(itemHistoryHandler))
	mux.HandleFunc("/activity", visitorTracker(activityHandler))
	mux.HandleFunc("/players", visitorTracker(playerCountHandler))
	mux.HandleFunc("/characters", visitorTracker(characterHandler))
	mux.HandleFunc("/guilds", visitorTracker(guildHandler))
	mux.HandleFunc("/guild", visitorTracker(guildDetailHandler))
	mux.HandleFunc("/mvp-kills", visitorTracker(mvpKillsHandler))
	mux.HandleFunc("/character", visitorTracker(characterDetailHandler))
	mux.HandleFunc("/character-changelog", visitorTracker(characterChangelogHandler))
	mux.HandleFunc("/store", visitorTracker(storeDetailHandler))
	mux.HandleFunc("/discord", visitorTracker(tradingPostListHandler))
	mux.HandleFunc("/woe", visitorTracker(woeRankingsHandler))
	mux.HandleFunc("/chat", visitorTracker(chatHandler))
	mux.HandleFunc("/xp-calculator", visitorTracker(xpCalculatorHandler))
	mux.HandleFunc("/about", visitorTracker(aboutHandler))
	mux.HandleFunc("/set-lang", setLangHandler)
	mux.HandleFunc("/search", visitorTracker(globalSearchHandler))
	mux.HandleFunc("/stats/drops", visitorTracker(dropStatsHandler))
	mux.HandleFunc("/stats/market", visitorTracker(marketStatsHandler))
	mux.HandleFunc("/stats/characters", visitorTracker(characterStatsHandler))

	// --- Admin Routes ---
	adminRouter := registerAdminRoutes()

	// Apply the basicAuth middleware to the entire admin router
	// Note the trailing slash on "/admin/" is important for sub-path matching
	mux.Handle("/admin/", basicAuth(http.StripPrefix("/admin", adminRouter)))

	return mux
}

// registerAdminRoutes creates a sub-router for all admin-facing endpoints.
func registerAdminRoutes() *http.ServeMux {
	adminRouter := http.NewServeMux()

	// Core Admin
	adminRouter.HandleFunc("/", adminHandler) // Handles /admin/
	adminRouter.HandleFunc("/parse-trade", adminParseTradeHandler)
	adminRouter.HandleFunc("/views/delete-visitor", adminDeleteVisitorViewsHandler)
	adminRouter.HandleFunc("/guild/update-emblem", adminUpdateGuildEmblemHandler)
	adminRouter.HandleFunc("/character/clear-last-active", adminClearLastActiveHandler)
	adminRouter.HandleFunc("/character/clear-mvp-kills", adminClearMvpKillsHandler)
	adminRouter.HandleFunc("/backfill/drops", adminBackfillDropLogsHandler)

	// Admin RMS Cache Management
	adminRouter.HandleFunc("/cache", adminCacheActionHandler)
	adminRouter.HandleFunc("/cache/delete-entry", adminDeleteCacheEntryHandler)
	adminRouter.HandleFunc("/cache/save-entry", adminSaveCacheEntryHandler)

	// Admin Trading Post Management
	adminRouter.HandleFunc("/trading-post/delete", adminDeleteTradingPostHandler)
	adminRouter.HandleFunc("/trading-post/edit", adminEditTradingPostHandler)
	adminRouter.HandleFunc("/trading-post/reparse", adminReparseTradingPostHandler)
	adminRouter.HandleFunc("/trading/clear-items", adminClearTradingPostItemsHandler)
	adminRouter.HandleFunc("/trading/clear-posts", adminClearTradingPostsHandler)

	// Admin Manual Scrape Triggers
	adminRouter.HandleFunc("/scrape/market", adminTriggerScrapeHandler(scrapeData, "Market"))
	adminRouter.HandleFunc("/scrape/players", adminTriggerScrapeHandler(scrapeAndStorePlayerCount, "Player-Count"))
	adminRouter.HandleFunc("/scrape/characters", adminTriggerScrapeHandler(scrapePlayerCharacters, "Character"))
	adminRouter.HandleFunc("/scrape/guilds", adminTriggerScrapeHandler(scrapeGuilds, "Guild"))
	adminRouter.HandleFunc("/scrape/zeny", adminTriggerScrapeHandler(scrapeZeny, "Zeny"))
	adminRouter.HandleFunc("/scrape/mvp", adminTriggerScrapeHandler(scrapeMvpKills, "MVP"))
	adminRouter.HandleFunc("/scrape/pt-names", adminTriggerScrapeHandler(populateMissingPortugueseNames, "PT-Name-Populator"))
	adminRouter.HandleFunc("/scrape/woe", adminTriggerScrapeHandler(scrapeWoeCharacterRankings, "WoE-Char-Rankings"))

	adminRouter.HandleFunc("/cleanup/guild-history", adminCleanupGuildHistoryHandler)

	return adminRouter
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[I] [Main] No .env file found, relying on system environment variables.")
	}

	var err error
	db, err = initDB("./market_data.db")
	if err != nil {
		log.Fatalf("[F] [DB] Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Run this synchronously on startup before starting other services
	populateItemDBOnStartup()

	// Create a context that gets cancelled on OS signals (SIGINT, SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine to listen for OS signals
	go func() {
		<-sigChan
		log.Println("[I] [Main] Shutdown signal received. Initiating graceful shutdown...")
		cancel() // Trigger context cancellation
	}()

	// --- Admin Password Logic ---
	adminPass = os.Getenv("ADMIN_PASSWORD")
	if adminPass == "" {
		log.Println("[I] [Main] ADMIN_PASSWORD not set. Generating a new random password.")
		adminPass = generateRandomPassword(16)
		if err := os.WriteFile("pwd.txt", []byte(adminPass), 0644); err != nil {
			log.Printf("[W] [Main] Could not write generated admin password to file: %v", err)
		} else {
			log.Println("[I] [Main] Generated admin password saved to pwd.txt")
		}
	} else {
		log.Println("[I] [Main] Loaded admin password from ADMIN_PASSWORD environment variable.")
	}

	go func() {
		time.Sleep(5 * time.Second) // Give server time to start
		log.Println("==================================================")
		log.Printf("[I] [Main] Admin User: %s", adminUser)
		log.Printf("[I] [Main] Admin Pass: %s", adminPass)
		log.Println("==================================================")
	}()

	// Start Background Services with the cancellable context
	go startBackgroundJobs(ctx)
	go startDiscordBot(ctx)
	go startVisitorLogger(ctx)

	// --- Setup Routers ---
	mux := registerRoutes()

	// --- Server Start and Shutdown ---
	port := "8080"
	server := &http.Server{Addr: ":" + port, Handler: mux}

	// Goroutine to handle server shutdown when context is cancelled
	go func() {
		<-ctx.Done() // Wait for the cancel() signal
		log.Println("[I] [HTTP] Shutting down web server...")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[E] [HTTP] Web server graceful shutdown failed: %v", err)
		}
	}()

	// Start server and block
	log.Printf("[I] [HTTP] Web server started. Open http://localhost:%s in your browser.", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[F] [HTTP] Web server failed to start: %v", err)
	}

	log.Println("[I] [Main] All services shut down. Exiting.")
}
