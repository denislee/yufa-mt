package main

import (
	"log"
	"net/http"
)

func main() {
	var err error
	// Initialize the database connection. The 'db' variable is global in the 'db.go' file.
	db, err = initDB("./market_data.db")
	if err != nil {
		log.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer db.Close()

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

	// Start the web server.
	port := "8080"
	log.Printf("🚀 Web server started. Open http://localhost:%s in your browser.", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("❌ Failed to start web server: %v", err)
	}
}

