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
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

// getVisitorHash creates an anonymized hash for a visitor.
func getVisitorHash(r *http.Request) string {
	// Try to get the real IP from X-Forwarded-For, falling back to RemoteAddr
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	} else {
		// X-Forwarded-For can be a comma-separated list of IPs. The first one is the client.
		ip = strings.Split(ip, ",")[0]
	}

	// Combine IP and User-Agent for a more unique identifier
	ua := r.UserAgent()
	data := fmt.Sprintf("%s-%s", ip, ua)

	// Create a SHA256 hash
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

type pageViewLog struct {
	VisitorHash string
	PageURI     string
	Timestamp   string
}

var pageViewChannel = make(chan pageViewLog, 1000)

// startVisitorLogger is a background worker that batch-processes page views.
func startVisitorLogger(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	var batch []pageViewLog
	ticker := time.NewTicker(10 * time.Second) // Flush every 10 seconds
	defer ticker.Stop()

	const batchSize = 100 // Or flush when batch reaches 100

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		// Use a transaction for batch processing
		tx, err := db.Begin()
		if err != nil {
			log.Printf("❌ Visitor Logger: Failed to begin transaction: %v", err)
			return // Keep items in batch and retry next time
		}

		visitorStmt, err := tx.Prepare(`
			INSERT INTO visitors (visitor_hash, first_visit, last_visit)
			VALUES (?, ?, ?)
			ON CONFLICT(visitor_hash) DO UPDATE SET
				last_visit = excluded.last_visit;
		`)
		if err != nil {
			log.Printf("❌ Visitor Logger: Failed to prepare visitor statement: %v", err)
			tx.Rollback()
			return
		}
		defer visitorStmt.Close()

		viewStmt, err := tx.Prepare(`
			INSERT INTO page_views (visitor_hash, page_path, view_timestamp)
			VALUES (?, ?, ?);
		`)
		if err != nil {
			log.Printf("❌ Visitor Logger: Failed to prepare page_view statement: %v", err)
			tx.Rollback()
			return
		}
		defer viewStmt.Close()

		// Keep track of processed visitors in this batch to avoid duplicate UPSERTs
		visitorsProcessed := make(map[string]bool)

		for _, logEntry := range batch {
			if !visitorsProcessed[logEntry.VisitorHash] {
				_, err := visitorStmt.Exec(logEntry.VisitorHash, logEntry.Timestamp, logEntry.Timestamp)
				if err != nil {
					log.Printf("⚠️ Visitor Logger: Failed to exec visitor upsert: %v", err)
				}
				visitorsProcessed[logEntry.VisitorHash] = true
			}

			_, err := viewStmt.Exec(logEntry.VisitorHash, logEntry.PageURI, logEntry.Timestamp)
			if err != nil {
				log.Printf("⚠️ Visitor Logger: Failed to exec page_view insert: %v", err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Printf("❌ Visitor Logger: Failed to commit batch: %v", err)
			// On commit fail, items remain in batch and will be retried
		} else {
			log.Printf("📝 Visitor Logger: Flushed %d page views to database.", len(batch))
			batch = nil // Clear the batch on success
		}
	}

	for {
		select {
		case <-ctx.Done():
			// Shutdown signal received
			log.Println("🔌 Visitor Logger: Shutdown signal received. Draining channel...")
			// Drain any remaining items in the channel
			for logEntry := range pageViewChannel {
				batch = append(batch, logEntry)
				if len(batch) >= batchSize {
					flushBatch()
				}
			}
			log.Println("🔌 Visitor Logger: Flushing final batch...")
			flushBatch() // Flush anything left
			log.Println("✅ Visitor Logger: Shut down gracefully.")
			return
		case logEntry := <-pageViewChannel:
			batch = append(batch, logEntry)
			if len(batch) >= batchSize {
				flushBatch()
			}
		case <-ticker.C:
			// Time to flush whatever we have
			flushBatch()
		}
	}
}

// visitorTracker is a middleware to log unique visitors and their page views.
func visitorTracker(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// This part is fast
		visitorHash := getVisitorHash(r)
		now := time.Now().Format(time.RFC3339)
		pageURI := r.URL.RequestURI()

		// Send to the channel instead of writing to DB
		logEntry := pageViewLog{
			VisitorHash: visitorHash,
			PageURI:     pageURI,
			Timestamp:   now,
		}

		// Use a non-blocking send.
		// If the channel is full, we drop the log to prevent blocking the user's request.
		select {
		case pageViewChannel <- logEntry:
			// Sent successfully
		default:
			// Channel is full, drop the log.
			log.Println("⚠️ Page view log channel is full. Dropping a page view.")
		}

		// Call the next handler immediately
		next.ServeHTTP(w, r)
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("ℹ️ No .env file found, relying on system environment variables.")
	}

	var err error
	// Initialize the database connection. The 'db' variable is global in the 'db.go' file.
	db, err = initDB("./market_data.db")
	if err != nil {
		log.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer db.Close()

	// --- Graceful Shutdown Setup ---
	// Create a context that can be canceled.
	ctx, cancel := context.WithCancel(context.Background())
	// Create a WaitGroup to wait for all goroutines to finish.
	var wg sync.WaitGroup

	// Set up a channel to listen for OS signals (like CTRL-C).
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine that will wait for a signal and then cancel the context.
	go func() {
		<-sigChan // Block until a signal is received.
		log.Println("🚨 Shutdown signal received, initiating graceful shutdown...")
		cancel()       // Cancel the context to signal all dependent goroutines.
		close(sigChan) // Close the channel to signal shutdown
	}()

	// --- DYNAMIC PASSWORD GENERATION ---
	// Generate and set the dynamic admin password for this session.
	adminPass = generateRandomPassword(16) // Sets the package-level variable in admin_handlers.go

	// Write the password to a local file.
	err = os.WriteFile("pwd.txt", []byte(adminPass), 0644)
	if err != nil {
		log.Printf("⚠️ Could not write admin password to file: %v", err)
	} else {
		log.Println("🔑 Admin password saved to pwd.txt")
	}

	// Log the password after a 5-second delay.
	go func() {
		time.Sleep(5 * time.Second)
		log.Println("==================================================")
		log.Printf("👤 Admin User: %s", adminUser)
		log.Printf("🔑 Admin Pass: %s", adminPass)
		log.Println("==================================================")
	}()

	// Start background tasks.
	go populateMissingCachesOnStartup() // Verifies and populates the item details cache on startup.
	go startBackgroundJobs()            // Starts all recurring scrapers.

	// Increment WaitGroup for background workers that respect context.
	wg.Add(2)                       // One for DiscordBot, one for VisitorLogger
	go startDiscordBot(ctx, &wg)    // Starts the Discord bot listener.
	go startVisitorLogger(ctx, &wg) // Starts the async visitor logger.

	// Register all HTTP routes to their handler functions.
	// Public routes are wrapped with the visitorTracker middleware.
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

	// ... after other http.HandleFunc calls
	http.HandleFunc("/discord", visitorTracker(tradingPostListHandler))
	// REMOVED: Handlers for /trading-post/new and /trading-post/manage

	// REMOVED: API routes for item previews and search
	// http.HandleFunc("/api/item-details", visitorTracker(apiItemDetailsHandler))
	// http.HandleFunc("/api/item-search", visitorTracker(apiItemSearchHandler))

	// --- ADMIN ROUTES ---
	// Admin routes are NOT tracked.
	http.HandleFunc("/admin", basicAuth(adminHandler))
	http.HandleFunc("/admin/parse-trade", basicAuth(adminParseTradeHandler))
	http.HandleFunc("/admin/views/delete-visitor", basicAuth(adminDeleteVisitorViewsHandler))
	http.HandleFunc("/admin/cache", basicAuth(adminCacheActionHandler))
	http.HandleFunc("/admin/guild/update-emblem", basicAuth(adminUpdateGuildEmblemHandler))
	http.HandleFunc("/admin/character/clear-last-active", basicAuth(adminClearLastActiveHandler))
	http.HandleFunc("/admin/character/clear-mvp-kills", basicAuth(adminClearMvpKillsHandler))
	http.HandleFunc("/admin/trading-post/delete", basicAuth(adminDeleteTradingPostHandler))
	http.HandleFunc("/admin/trading-post/edit", basicAuth(adminEditTradingPostHandler))
	// ADDED NEW ROUTE HERE
	http.HandleFunc("/admin/trading-post/reparse", basicAuth(adminReparseTradingPostHandler))
	http.HandleFunc("/admin/trading/clear-items", basicAuth(adminClearTradingPostItemsHandler))
	http.HandleFunc("/admin/trading/clear-posts", basicAuth(adminClearTradingPostsHandler))
	// Manual Scraper Triggers
	http.HandleFunc("/admin/scrape/market", basicAuth(adminTriggerScrapeHandler(scrapeData, "Market")))
	http.HandleFunc("/admin/scrape/players", basicAuth(adminTriggerScrapeHandler(scrapeAndStorePlayerCount, "Player-Count")))
	http.HandleFunc("/admin/scrape/characters", basicAuth(adminTriggerScrapeHandler(scrapePlayerCharacters, "Character")))
	http.HandleFunc("/admin/scrape/guilds", basicAuth(adminTriggerScrapeHandler(scrapeGuilds, "Guild")))
	http.HandleFunc("/admin/scrape/zeny", basicAuth(adminTriggerScrapeHandler(scrapeZeny, "Zeny")))
	http.HandleFunc("/admin/scrape/mvp", basicAuth(adminTriggerScrapeHandler(scrapeMvpKills, "MVP")))
	http.HandleFunc("/admin/scrape/rms-cache", basicAuth(adminTriggerScrapeHandler(runFullRMSCacheJob, "RMS-Cache-Refresh")))
	// --- HTTP Server Setup and Shutdown ---
	port := "8080"
	server := &http.Server{Addr: ":" + port}

	// Start the server in a new goroutine.
	go func() {
		log.Printf("🚀 Web server started. Open http://localhost:%s in your browser.", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Failed to start web server: %v", err)
		}
	}()

	// Block here until the context is canceled (which happens when a signal is received).
	<-ctx.Done()

	// Context is canceled, so we begin the shutdown process.
	log.Println("🔌 Shutting down HTTP server...")

	// Create a new context for the server shutdown with a timeout.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Attempt to gracefully shut down the server.
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("⚠️ HTTP server shutdown error: %v", err)
	} else {
		log.Println("✅ HTTP server shut down gracefully.")
	}

	// Wait for all other background goroutines (the bot) to finish.
	log.Println("⏳ Waiting for background processes to shut down...")
	wg.Wait()
	log.Println("✅ All processes shut down cleanly. Exiting.")
}
