package main

import (
	"log"
	"net/http"
	"time"
)

func main() {
	var err error
	// Initialize the database connection. The 'db' variable is global in the 'db.go' file.
	db, err = initDB("./market_data.db")
	if err != nil {
		log.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer db.Close()

	// --- DYNAMIC PASSWORD GENERATION ---
	// Generate and set the dynamic admin password for this session.
	adminPass = generateRandomPassword(16) // Sets the package-level variable in admin_handlers.go

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

	// Register all HTTP routes to their handler functions.
	http.HandleFunc("/", summaryHandler)             // Main page showing item summaries.
	http.HandleFunc("/full-list", fullListHandler)   // Detailed view of all market listings.
	http.HandleFunc("/item", itemHistoryHandler)     // Historical price chart and details for a single item.
	http.HandleFunc("/activity", activityHandler)    // Log of recent market events (items added/removed).
	http.HandleFunc("/players", playerCountHandler)  // Shows a graph of online player counts over time.
	http.HandleFunc("/characters", characterHandler) // Shows player characters.
	http.HandleFunc("/guilds", guildHandler)         // Shows guild rankings.
	http.HandleFunc("/guild", guildDetailHandler)    // ADDED: Shows details for a single guild.
	http.HandleFunc("/mvp-kills", mvpKillsHandler)   // Shows MVP kill rankings.
	http.HandleFunc("/character", characterDetailHandler)
	http.HandleFunc("/character-changelog", characterChangelogHandler)

	// --- ADMIN ROUTES ---
	http.HandleFunc("/admin", basicAuth(adminHandler))
	http.HandleFunc("/admin/cache", basicAuth(adminCacheActionHandler))
	http.HandleFunc("/admin/guild/update-emblem", basicAuth(adminUpdateGuildEmblemHandler)) // ADDED THIS LINE
	// Manual Scraper Triggers
	http.HandleFunc("/admin/scrape/market", basicAuth(adminTriggerScrapeHandler(scrapeData, "Market")))
	http.HandleFunc("/admin/scrape/players", basicAuth(adminTriggerScrapeHandler(scrapeAndStorePlayerCount, "Player-Count")))
	http.HandleFunc("/admin/scrape/characters", basicAuth(adminTriggerScrapeHandler(scrapePlayerCharacters, "Character")))
	http.HandleFunc("/admin/scrape/guilds", basicAuth(adminTriggerScrapeHandler(scrapeGuilds, "Guild")))
	http.HandleFunc("/admin/scrape/zeny", basicAuth(adminTriggerScrapeHandler(scrapeZeny, "Zeny")))
	http.HandleFunc("/admin/scrape/mvp", basicAuth(adminTriggerScrapeHandler(scrapeMvpKills, "MVP")))

	// Start the web server.
	port := "8080"
	log.Printf("🚀 Web server started. Open http://localhost:%s in your browser.", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("❌ Failed to start web server: %v", err)
	}
}
