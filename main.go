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

// visitorTracker is a middleware to log unique visitors and their page views.
func visitorTracker(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		visitorHash := getVisitorHash(r)
		now := time.Now().Format(time.RFC3339)

		// Use an UPSERT to either insert a new visitor or update the last_visit time.
		_, err := db.Exec(`
			INSERT INTO visitors (visitor_hash, first_visit, last_visit)
			VALUES (?, ?, ?)
			ON CONFLICT(visitor_hash) DO UPDATE SET
				last_visit = excluded.last_visit;
		`, visitorHash, now, now)

		if err != nil {
			// Log the error but don't block the request. Tracking is a best-effort feature.
			log.Printf("⚠️ Visitor tracking error: %v", err)
		}

		// Log the specific page view, now including query parameters.
		pageURI := r.URL.RequestURI()
		_, err = db.Exec(`
			INSERT INTO page_views (visitor_hash, page_path, view_timestamp)
			VALUES (?, ?, ?);
		`, visitorHash, pageURI, now)

		if err != nil {
			log.Printf("⚠️ Page view tracking error for path %s: %v", pageURI, err)
		}

		// Call the next handler in the chain
		next.ServeHTTP(w, r)
	}
}

func main() {
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
		cancel() // Cancel the context to signal all dependent goroutines.
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
	wg.Add(1)
	go startDiscordBot(ctx, &wg) // Starts the Discord bot listener.

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
	http.HandleFunc("/trading-post", visitorTracker(tradingPostListHandler))
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
	http.HandleFunc("/admin/trading/clear-items", basicAuth(adminClearTradingPostItemsHandler))
	http.HandleFunc("/admin/trading/clear-posts", basicAuth(adminClearTradingPostsHandler))
	// Manual Scraper Triggers
	http.HandleFunc("/admin/scrape/market", basicAuth(adminTriggerScrapeHandler(scrapeData, "Market")))
	http.HandleFunc("/admin/scrape/players", basicAuth(adminTriggerScrapeHandler(scrapeAndStorePlayerCount, "Player-Count")))
	http.HandleFunc("/admin/scrape/characters", basicAuth(adminTriggerScrapeHandler(scrapePlayerCharacters, "Character")))
	http.HandleFunc("/admin/scrape/guilds", basicAuth(adminTriggerScrapeHandler(scrapeGuilds, "Guild")))
	http.HandleFunc("/admin/scrape/zeny", basicAuth(adminTriggerScrapeHandler(scrapeZeny, "Zeny")))
	http.HandleFunc("/admin/scrape/mvp", basicAuth(adminTriggerScrapeHandler(scrapeMvpKills, "MVP")))

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
