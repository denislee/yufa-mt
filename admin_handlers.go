package main

import (
	"crypto/subtle"
	"fmt"
	"html/template"
	"log"
	"net/http"
)

// The admin user remains constant.
const adminUser = "admin"

// The admin password will now be a variable, set at runtime.
var adminPass string

// basicAuth is a middleware function to protect admin routes.
func basicAuth(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()

		// Use subtle.ConstantTimeCompare to prevent timing attacks.
		// It now compares against the 'adminPass' variable.
		if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(adminUser)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(adminPass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintln(w, "Unauthorized.")
			return
		}

		handler(w, r)
	}
}

func adminHandler(w http.ResponseWriter, r *http.Request) {
	stats := AdminDashboardData{
		Message: r.URL.Query().Get("msg"),
	}

	// Gather statistics from the database
	_ = db.QueryRow("SELECT COUNT(*) FROM items").Scan(&stats.TotalItems)
	_ = db.QueryRow("SELECT COUNT(*) FROM items WHERE is_available = 1").Scan(&stats.AvailableItems)
	_ = db.QueryRow("SELECT COUNT(DISTINCT name_of_the_item) FROM items").Scan(&stats.UniqueItems)
	_ = db.QueryRow("SELECT COUNT(*) FROM rms_item_cache").Scan(&stats.CachedItems)
	_ = db.QueryRow("SELECT COUNT(*) FROM characters").Scan(&stats.TotalCharacters)
	_ = db.QueryRow("SELECT COUNT(*) FROM guilds").Scan(&stats.TotalGuilds)
	_ = db.QueryRow("SELECT COUNT(*) FROM player_history").Scan(&stats.PlayerHistoryEntries)
	_ = db.QueryRow("SELECT COUNT(*) FROM market_events").Scan(&stats.MarketEvents)
	_ = db.QueryRow("SELECT COUNT(*) FROM character_changelog").Scan(&stats.ChangelogEntries)

	// Get last scrape times
	stats.LastMarketScrape = getLastScrapeTime()
	stats.LastPlayerCountScrape = getLastPlayerCountTime()
	stats.LastCharacterScrape = getLastCharacterScrapeTime()
	stats.LastGuildScrape = getLastGuildScrapeTime()

	tmpl, err := template.ParseFiles("admin.html")
	if err != nil {
		http.Error(w, "Could not load admin template", http.StatusInternalServerError)
		log.Printf("❌ Could not load admin.html template: %v", err)
		return
	}

	tmpl.Execute(w, stats)
}

// adminTriggerScrapeHandler creates a handler for a specific scraper function.
func adminTriggerScrapeHandler(scraperFunc func(), name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/admin", http.StatusSeeOther)
			return
		}
		log.Printf("👤 Admin triggered '%s' scrape manually.", name)
		go scraperFunc() // Run in a goroutine so it doesn't block the HTTP response
		http.Redirect(w, r, fmt.Sprintf("/admin?msg=%s+scrape+started.", name), http.StatusSeeOther)
	}
}

// adminCacheActionHandler handles actions related to the RMS item cache.
func adminCacheActionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	action := r.FormValue("action")
	var msg string

	switch action {
	case "clear":
		log.Println("👤 Admin triggered cache clear.")
		_, err := db.Exec("DELETE FROM rms_item_cache")
		if err != nil {
			msg = "Error+clearing+cache."
			log.Printf("❌ Failed to clear RMS cache: %v", err)
		} else {
			msg = "Item+cache+cleared+successfully."
		}
	case "repopulate":
		log.Println("👤 Admin triggered cache repopulation.")
		go populateMissingCachesOnStartup()
		msg = "Cache+repopulation+started+in+background."
	default:
		msg = "Unknown+cache+action."
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}
