package main

import (
	"crypto/subtle"
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const adminUser = "admin"

var adminPass string

func basicAuth(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()

		if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(adminUser)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(adminPass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintln(w, "Unauthorized.")
			return
		}

		handler.ServeHTTP(w, r)
	})
}

// adminRedirectURL builds an admin redirect URL that includes the active tab.
func adminRedirectURL(r *http.Request, msg string) string {
	// PostFormValue ensures the form is parsed and reads from the body
	tab := r.PostFormValue("tab")
	redirectURL := "/admin?msg=" + url.QueryEscape(msg)
	if tab != "" {
		redirectURL += "&tab=" + url.QueryEscape(tab)
	}
	return redirectURL
}

// Define a struct for caching stats
var dashboardStatsCache struct {
	sync.RWMutex
	data   AdminDashboardData
	expiry time.Time
}

// copyDashboardStats copies the cacheable stats fields from src to dst.
func copyDashboardStats(dst, src *AdminDashboardData) {
	dst.TotalItems = src.TotalItems
	dst.AvailableItems = src.AvailableItems
	dst.UniqueItems = src.UniqueItems
	dst.CachedItems = src.CachedItems
	dst.TotalCharacters = src.TotalCharacters
	dst.TotalGuilds = src.TotalGuilds
	dst.PlayerHistoryEntries = src.PlayerHistoryEntries
	dst.MarketEvents = src.MarketEvents
	dst.ChangelogEntries = src.ChangelogEntries
	dst.TotalVisitors = src.TotalVisitors
	dst.VisitorsToday = src.VisitorsToday
}

// getDashboardStats populates the main database statistics, utilizing a TTL cache.
func getDashboardStats(stats *AdminDashboardData) error {
	// 1. Check Cache
	dashboardStatsCache.RLock()
	if time.Now().Before(dashboardStatsCache.expiry) {
		copyDashboardStats(stats, &dashboardStatsCache.data)
		dashboardStatsCache.RUnlock()
		return nil
	}
	dashboardStatsCache.RUnlock()

	// 2. Cache expired or empty, run queries concurrently
	var g errgroup.Group
	var newStats AdminDashboardData

	runQuery := func(query string, target *int) func() error {
		return func() error {
			var count sql.NullInt64
			if err := db.QueryRow(query).Scan(&count); err != nil {
				log.Printf("[W] [Admin/Stats] Dashboard stats query failed (%s): %v", query, err)
				return err
			}
			*target = int(count.Int64)
			return nil
		}
	}

	g.Go(runQuery("SELECT COUNT(*) FROM items", &newStats.TotalItems))
	g.Go(runQuery("SELECT COUNT(*) FROM items WHERE is_available = 1", &newStats.AvailableItems))
	g.Go(runQuery("SELECT COUNT(DISTINCT name_of_the_item) FROM items", &newStats.UniqueItems))
	g.Go(runQuery("SELECT COUNT(*) FROM internal_item_db", &newStats.CachedItems))
	g.Go(runQuery("SELECT COUNT(*) FROM characters", &newStats.TotalCharacters))
	g.Go(runQuery("SELECT COUNT(*) FROM guilds", &newStats.TotalGuilds))
	g.Go(runQuery("SELECT COUNT(*) FROM player_history", &newStats.PlayerHistoryEntries))
	g.Go(runQuery("SELECT COUNT(*) FROM market_events", &newStats.MarketEvents))
	g.Go(runQuery("SELECT COUNT(*) FROM character_changelog", &newStats.ChangelogEntries))
	g.Go(runQuery("SELECT COUNT(*) FROM visitors", &newStats.TotalVisitors))
	g.Go(runQuery("SELECT COUNT(*) FROM visitors WHERE date(last_visit) = date('now', 'localtime')", &newStats.VisitorsToday))

	if err := g.Wait(); err != nil {
		return fmt.Errorf("could not query for one or more dashboard stats: %w", err)
	}

	// 3. Update Cache
	dashboardStatsCache.Lock()
	dashboardStatsCache.data = newStats
	dashboardStatsCache.expiry = time.Now().Add(30 * time.Second)
	dashboardStatsCache.Unlock()

	// 4. Copy to output
	copyDashboardStats(stats, &newStats)
	return nil
}

// getDashboardPageVisitCounts populates the page view summary.
func getDashboardPageVisitCounts(stats *AdminDashboardData) error {
	rows, err := db.Query(`
		SELECT page_path, COUNT(page_path) as Cnt
		FROM page_views
		GROUP BY page_path
		ORDER BY Cnt DESC
		LIMIT 25
	`)
	if err != nil {
		return fmt.Errorf("could not query for page view counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entry PageViewSummary
		if err := rows.Scan(&entry.Path, &entry.Hits); err != nil {
			log.Printf("[W] [Admin/Stats] Failed to scan page view count row: %v", err)
			continue
		}
		stats.PageVisitCounts = append(stats.PageVisitCounts, entry)
	}
	return nil
}

// getDashboardGuilds populates the guild list for the emblem editor.
func getDashboardGuilds(stats *AdminDashboardData) error {
	guildRows, err := db.Query("SELECT name, COALESCE(emblem_url, '') FROM guilds ORDER BY name ASC")
	if err != nil {
		return fmt.Errorf("could not query for guild list for admin page: %w", err)
	}
	defer guildRows.Close()

	for guildRows.Next() {
		var info GuildInfo
		if err := guildRows.Scan(&info.Name, &info.EmblemURL); err != nil {
			log.Printf("[W] [Admin/Stats] Failed to scan guild info for admin page: %v", err)
			continue
		}
		stats.AllGuilds = append(stats.AllGuilds, info)
	}
	return nil
}

// getDashboardPageViews populates the recent page views and analytics.
func getDashboardPageViews(r *http.Request, stats *AdminDashboardData) {
	err := db.QueryRow(`
		SELECT page_path, COUNT(page_path) as Cnt
		FROM page_views
		GROUP BY page_path
		ORDER BY Cnt DESC
		LIMIT 1
	`).Scan(&stats.MostVisitedPage, &stats.MostVisitedPageCount)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("[W] [Admin/Stats] Could not query for most visited page: %v", err)
		stats.MostVisitedPage = "Error"
	}

	const viewsPerPage = 20
	pageStr := r.URL.Query().Get("page")
	page, _ := strconv.Atoi(pageStr)
	if page < 1 {
		page = 1
	}
	var totalViews int
	db.QueryRow("SELECT COUNT(*) FROM page_views").Scan(&totalViews)
	stats.PageViewsTotal = totalViews
	stats.PageViewsTotalPages = (totalViews + viewsPerPage - 1) / viewsPerPage
	stats.PageViewsCurrentPage = page
	if page > 1 {
		stats.PageViewsHasPrevPage = true
		stats.PageViewsPrevPage = page - 1
	}
	if page < stats.PageViewsTotalPages {
		stats.PageViewsHasNextPage = true
		stats.PageViewsNextPage = page + 1
	}
	offset := (page - 1) * viewsPerPage
	viewRows, err := db.Query(`SELECT page_path, view_timestamp, visitor_hash FROM page_views ORDER BY view_timestamp DESC LIMIT ? OFFSET ?`, viewsPerPage, offset)
	if err != nil {
		log.Printf("[W] [Admin/Stats] Could not query for recent page views: %v", err)
		return
	}
	defer viewRows.Close()

	for viewRows.Next() {
		var entry PageViewEntry
		var timestampStr string
		if err := viewRows.Scan(&entry.Path, &timestampStr, &entry.VisitorHash); err == nil {
			parsedTime, _ := time.Parse(time.RFC3339, timestampStr)
			entry.Timestamp = parsedTime.Format("15:04:05")
			stats.RecentPageViews = append(stats.RecentPageViews, entry)
		}
	}
}

// getDashboardTradingPosts populates the recent trading posts list.
func getDashboardTradingPosts(r *http.Request, stats *AdminDashboardData) {
	const postsPerPage = 10
	tpPageStr := r.URL.Query().Get("tp_page")
	tpPage, _ := strconv.Atoi(tpPageStr)
	if tpPage < 1 {
		tpPage = 1
	}
	var totalPosts int
	db.QueryRow("SELECT COUNT(*) FROM trading_posts").Scan(&totalPosts)
	stats.TradingPostTotal = totalPosts
	stats.TradingPostTotalPages = (totalPosts + postsPerPage - 1) / postsPerPage
	stats.TradingPostCurrentPage = tpPage
	if tpPage > 1 {
		stats.TradingPostHasPrevPage = true
		stats.TradingPostPrevPage = tpPage - 1
	}
	if tpPage < stats.TradingPostTotalPages {
		stats.TradingPostHasNextPage = true
		stats.TradingPostNextPage = tpPage + 1
	}
	tpOffset := (tpPage - 1) * postsPerPage

	postRows, err := db.Query(`SELECT id, post_type, character_name, contact_info, created_at, notes FROM trading_posts ORDER BY created_at DESC LIMIT ? OFFSET ?`, postsPerPage, tpOffset)
	if err != nil {
		log.Printf("[W] [Admin/Stats] Admin Trading Post query error: %v", err)
		return
	}
	defer postRows.Close()

	var posts []TradingPost
	postMap := make(map[int]int)
	var postIDs []interface{}
	for postRows.Next() {
		var post TradingPost
		if err := postRows.Scan(&post.ID, &post.PostType, &post.CharacterName, &post.ContactInfo, &post.CreatedAt, &post.Notes); err == nil {
			post.Items = []TradingPostItem{}
			posts = append(posts, post)
			postMap[post.ID] = len(posts) - 1
			postIDs = append(postIDs, post.ID)
		}
	}

	if len(postIDs) > 0 {
		placeholders := strings.Repeat("?,", len(postIDs)-1) + "?"
		itemQuery := fmt.Sprintf(`
			SELECT post_id, item_name, quantity, price_zeny, price_rmt, refinement, card1 
			FROM trading_post_items WHERE post_id IN (%s)
		`, placeholders)
		itemRows, _ := db.Query(itemQuery, postIDs...)
		if itemRows != nil {
			defer itemRows.Close()
			for itemRows.Next() {
				var item TradingPostItem
				var postID int
				if err := itemRows.Scan(&postID, &item.ItemName, &item.Quantity, &item.PriceZeny, &item.PriceRMT, &item.Refinement, &item.Card1); err == nil {
					if index, ok := postMap[postID]; ok {
						posts[index].Items = append(posts[index].Items, item)
					}
				}
			}
		}
	}
	stats.RecentTradingPosts = posts
}

// performRMSCacheSearch performs the FTS search on the local RMS cache.
func performRMSCacheSearch(r *http.Request, stats *AdminDashboardData) {
	rmsQuery := r.URL.Query().Get("rms_query")
	stats.RMSCacheSearchQuery = rmsQuery
	if rmsQuery == "" {
		return
	}

	likeQuery := "%" + strings.ReplaceAll(rmsQuery, " ", "%") + "%"
	searchRows, err := db.Query(`
		SELECT item_id, name, name_pt 
		FROM internal_item_db 
		WHERE name LIKE ? OR name_pt LIKE ?
		ORDER BY item_id 
		LIMIT 50`, likeQuery, likeQuery)
	if err != nil {
		log.Printf("[W] [Admin/Stats] Admin Internal DB query error: %v", err)
		return
	}
	defer searchRows.Close()

	for searchRows.Next() {
		var result RMSCacheSearchResult
		if err := searchRows.Scan(&result.ItemID, &result.Name, &result.NamePT); err == nil {
			stats.RMSCacheSearchResults = append(stats.RMSCacheSearchResults, result)
		}
	}
}

// performRMSLiveSearch performs the concurrent live search on RMS and RODB.
func performRMSLiveSearch(r *http.Request, stats *AdminDashboardData) {
	rmsLiveSearchQuery := r.URL.Query().Get("rms_live_search")
	stats.RMSLiveSearchQuery = rmsLiveSearchQuery
	if rmsLiveSearchQuery == "" {
		return
	}

	log.Printf("[I] [Admin] Admin performing live search for: '%s'", rmsLiveSearchQuery)
	var wg sync.WaitGroup
	var rodbResults []ItemSearchResult
	var rodbErr error

	wg.Add(1)

	go func() {
		defer wg.Done()
		rodbResults, rodbErr = scrapeRODatabaseSearch(rmsLiveSearchQuery, 0)
		if rodbErr != nil {
			log.Printf("[W] [Admin/Search] Admin RMS Live Search (RODB) query error: %v", rodbErr)
		}
	}()
	wg.Wait()

	combinedResults := make([]ItemSearchResult, 0)
	seenIDs := make(map[int]bool)

	if rodbResults != nil {
		for _, res := range rodbResults {
			if !seenIDs[res.ID] {
				combinedResults = append(combinedResults, res)
				seenIDs[res.ID] = true
			}
		}
	}

	stats.RMSLiveSearchResults = combinedResults
	log.Printf("[I] [Admin] Admin live search for '%s' found %d combined results.", rmsLiveSearchQuery, len(combinedResults))
}

// getAdminDashboardData orchestrates fetching all data for the admin dashboard concurrently.
// This version is refactored to use an errgroup for simpler concurrent error handling.
func getAdminDashboardData(r *http.Request) (AdminDashboardData, error) {
	stats := AdminDashboardData{
		Message: r.URL.Query().Get("msg"),
	}

	// An errgroup simplifies managing multiple concurrent tasks and their errors.
	var g errgroup.Group

	// --- Run data-fetching tasks concurrently ---

	// Task 1: Main Stats (Critical)
	g.Go(func() error {
		if err := getDashboardStats(&stats); err != nil {
			log.Printf("[E] [Admin] Failed to load dashboard stats: %v", err)
			// Returning an error here will cause g.Wait() to return this error.
			return err
		}
		return nil
	})

	// Task 2: Guilds (Not Critical)
	g.Go(func() error {
		if err := getDashboardGuilds(&stats); err != nil {
			// Log but don't fail the whole page.
			log.Printf("[W] [Admin] Could not load dashboard guilds: %v", err)
		}
		return nil // Always return nil so this doesn't fail the group.
	})

	// Task 3: Page Views (Not Critical)
	g.Go(func() error {
		getDashboardPageViews(r, &stats)
		return nil
	})

	// Task 4: Trading Posts (Not Critical)
	g.Go(func() error {
		getDashboardTradingPosts(r, &stats)
		return nil
	})

	// Task 5: RMS Cache Search (Not Critical)
	g.Go(func() error {
		performRMSCacheSearch(r, &stats)
		return nil
	})

	// Task 6: RMS Live Search (Not Critical)
	g.Go(func() error {
		performRMSLiveSearch(r, &stats)
		return nil
	})

	// Task 7: Chat Messages (Only if tab is chat)
	if r.URL.Query().Get("tab") == "chat" {
		g.Go(func() error {
			getAdminChatMessages(r, &stats)
			return nil
		})
	}

	g.Go(func() error {
		if err := getDashboardPageVisitCounts(&stats); err != nil {
			// Log but don't fail the whole page.
			log.Printf("[W] [Admin] Could not load page visit counts: %v", err)
		}
		return nil
	})

	// Wait for all concurrent tasks to finish.
	// mainErr will be the first non-nil error returned from any g.Go() func.
	if mainErr := g.Wait(); mainErr != nil {
		return stats, mainErr // Return if a critical task failed
	}

	// --- Scrape times (these are fast, run sequentially) ---
	stats.LastMarketScrape = GetLastScrapeTime()
	stats.LastPlayerCountScrape = GetLastPlayerCountTime()
	stats.LastCharacterScrape = GetLastCharacterScrapeTime()
	stats.LastGuildScrape = GetLastGuildScrapeTime()

	return stats, nil
}

func adminSaveCacheEntryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin?msg=Error+parsing+form.&tab=cache", http.StatusSeeOther)
		return
	}

	// Get params for the redirect
	tab := r.PostFormValue("tab")
	liveSearchQuery := r.PostFormValue("rms_live_search")

	// Get the item ID to update
	itemIDStr := r.PostFormValue("item_id")
	itemID, err := strconv.Atoi(itemIDStr)
	if err != nil {
		msg := "Error: Invalid item ID."
		// Build redirect URL manually
		redirectURL := "/admin?msg=" + url.QueryEscape(msg)
		if tab != "" {
			redirectURL += "&tab=" + url.QueryEscape(tab)
		}
		if liveSearchQuery != "" {
			redirectURL += "&rms_live_search=" + url.QueryEscape(liveSearchQuery)
		}
		http.Redirect(w, r, redirectURL, http.StatusSeeOther)
		return
	}

	// --- NEW LOGIC: Try to fetch and update the PT name ---
	var msg string
	fetchedName, err := fetchAndUpdatePortugueseName(itemID)
	if err != nil {
		// This error includes "already exists" and "not found"
		msg = fmt.Sprintf("Error: %v", err)
	} else {
		msg = fmt.Sprintf("Successfully updated item %d with PT name: %s", itemID, fetchedName)
	}
	// --- END NEW LOGIC ---

	// Build redirect URL manually to include all params
	redirectURL := "/admin?msg=" + url.QueryEscape(msg)
	if tab != "" {
		redirectURL += "&tab=" + url.QueryEscape(tab)
	}
	if liveSearchQuery != "" {
		redirectURL += "&rms_live_search=" + url.QueryEscape(liveSearchQuery)
	}
	http.Redirect(w, r, redirectURL, http.StatusSeeOther)
}

func adminDeleteCacheEntryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	log.Printf("[WW] [Admin] Admin attempted to use 'Delete Cache Entry', which is disabled (internal_item_db is YAML-based).")
	msg := "Error: Cannot manually delete entry. Item database is now populated from YAML files."
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

func adminHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := getAdminDashboardData(r)
	if err != nil {
		http.Error(w, "Could not load dashboard data", http.StatusInternalServerError)
		log.Printf("[E] [Admin] Failed to get admin dashboard data: %v", err)
		return
	}

	tmpl, err := template.New("admin.html").Funcs(templateFuncs).ParseFiles("admin.html")
	if err != nil {
		http.Error(w, "Could not load admin template", http.StatusInternalServerError)
		log.Printf("[E] [HTTP] Could not load admin.html template: %v", err)
		return
	}

	tmpl.Execute(w, stats)
}

func adminParseTradeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	stats, err := getAdminDashboardData(r)
	if err != nil {
		http.Error(w, "Could not load dashboard data", http.StatusInternalServerError)
		log.Printf("[E] [Admin] Failed to get admin dashboard data for trade parse: %v", err)
		return
	}

	if err := r.ParseForm(); err != nil {
		stats.TradeParseError = "Could not parse form input."
	} else {
		message := r.FormValue("message")
		stats.OriginalTradeMessage = message

		if strings.TrimSpace(message) != "" {

			geminiResult, geminiErr := parseTradeMessageWithGemini(message)
			if geminiErr != nil {
				stats.TradeParseError = geminiErr.Error()
			} else {
				stats.TradeParseResult = geminiResult
			}
		} else {
			stats.TradeParseError = "Message cannot be empty."
		}
	}

	tmpl, err := template.New("admin.html").Funcs(templateFuncs).ParseFiles("admin.html")
	if err != nil {
		http.Error(w, "Could not load admin template", http.StatusInternalServerError)
		log.Printf("[E] [HTTP] Could not load admin.html template: %v", err)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(w, stats)
}

func adminTriggerScrapeHandler(scraperFunc func(), name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/admin", http.StatusSeeOther)
			return
		}
		log.Printf("[I] [Admin] Admin triggered '%s' scrape manually.", name)
		go scraperFunc()
		msg := fmt.Sprintf("%s scrape started.", name)
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
	}
}

func adminCacheActionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error parsing form."), http.StatusSeeOther)
		return
	}

	action := r.FormValue("action")
	var msg string

	// Note: No mutex is needed here as this is an admin action
	// and we are not using rmsCacheMutex anymore.
	switch action {
	case "clear":
		log.Println("[I] [Admin] Admin triggered internal item DB clear.")
		_, err := db.Exec("DELETE FROM internal_item_db")
		if err != nil {
			msg = "Error clearing internal item db."
			log.Printf("[E] [Admin] Failed to clear internal_item_db: %v", err)
		} else {
			msg = "Internal item db cleared successfully."
		}
	case "drop":
		log.Println("[I] [Admin] Admin triggered internal item DB table drop.")
		_, err := db.Exec("DROP TABLE IF EXISTS internal_item_db")
		if err != nil {
			msg = "Error dropping internal item db table."
			log.Printf("[E] [Admin] Failed to drop internal_item_db table: %v", err)
		} else {
			msg = "Internal item db table dropped successfully. Restart app to recreate."
		}
	case "repopulate":
		log.Println("[I] [Admin] Admin triggered internal item DB repopulation from YAMLs.")
		go populateItemDBOnStartup()
		msg = "Internal item db repopulation from YAMLs started in background."
	default:
		msg = "Unknown cache action."
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

func adminUpdateGuildEmblemHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error parsing form."), http.StatusSeeOther)
		return
	}

	guildName := r.FormValue("guild_name")
	emblemURL := r.FormValue("emblem_url")
	var msg string

	if guildName == "" || emblemURL == "" {
		msg = "Guild name and URL cannot be empty."
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
		return
	}

	result, err := db.Exec("UPDATE guilds SET emblem_url = ? WHERE name = ?", emblemURL, guildName)
	if err != nil {
		log.Printf("[E] [Admin] Failed to update emblem for guild '%s': %v", guildName, err)
		msg = "Database error occurred."
	} else {
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			msg = fmt.Sprintf("Guild '%s' not found.", guildName)
		} else {
			msg = fmt.Sprintf("Emblem for '%s' updated successfully.", guildName)
		}
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

func adminClearLastActiveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	var msg string

	zeroTime := time.Time{}.Format(time.RFC3339)

	result, err := db.Exec("UPDATE characters SET last_active = ?", zeroTime)
	if err != nil {
		log.Printf("[E] [Admin] Failed to clear last_active times: %v", err)
		msg = "Database error while clearing activity times."
	} else {
		rowsAffected, _ := result.RowsAffected()
		msg = fmt.Sprintf("Successfully reset last_active time for %d characters.", rowsAffected)
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

func adminClearMvpKillsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	var msg string
	result, err := db.Exec("DELETE FROM character_mvp_kills")
	if err != nil {
		log.Printf("[E] [Admin] Failed to clear MVP kills table: %v", err)
		msg = "Database error while clearing MVP kills."
	} else {
		rowsAffected, _ := result.RowsAffected()
		msg = fmt.Sprintf("Successfully deleted %d MVP kill records.", rowsAffected)
		log.Printf("[I] [Admin] Admin cleared all MVP kill data (%d records).", rowsAffected)
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

func adminDeleteVisitorViewsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error parsing form."), http.StatusSeeOther)
		return
	}

	visitorHash := r.FormValue("visitor_hash")
	var msg string

	if visitorHash == "" {
		msg = "Error: Missing visitor hash."
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("[E] [Admin] Failed to begin transaction for deleting visitor: %v", err)
		http.Redirect(w, r, adminRedirectURL(r, "Database error occurred."), http.StatusSeeOther)
		return
	}

	_, err = tx.Exec("DELETE FROM page_views WHERE visitor_hash = ?", visitorHash)
	if err != nil {
		tx.Rollback()
		log.Printf("[E] [Admin] Failed to delete from page_views for hash %s: %v", visitorHash, err)
		http.Redirect(w, r, adminRedirectURL(r, "Database error on page_views."), http.StatusSeeOther)
		return
	}

	result, err := tx.Exec("DELETE FROM visitors WHERE visitor_hash = ?", visitorHash)
	if err != nil {
		tx.Rollback()
		log.Printf("[ECode] [Admin] Failed to delete from visitors for hash %s: %v", visitorHash, err)
		http.Redirect(w, r, adminRedirectURL(r, "Database error on visitors."), http.StatusSeeOther)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[E] [Admin] Failed to commit transaction for deleting visitor: %v", err)
		http.Redirect(w, r, adminRedirectURL(r, "Database commit error."), http.StatusSeeOther)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		msg = "Visitor records removed successfully."
		log.Printf("[I] [Admin] Admin removed all data for visitor with hash starting with %s...", visitorHash[:12])
	} else {
		msg = "Visitor not found or already removed."
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

func adminDeleteTradingPostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error parsing form."), http.StatusSeeOther)
		return
	}

	postID := r.FormValue("post_id")
	var msg string

	if postID == "" {
		msg = "Error: Missing post ID."
	} else {

		result, err := db.Exec("DELETE FROM trading_posts WHERE id = ?", postID)
		if err != nil {
			msg = "Database error occurred while deleting post."
			log.Printf("[E] [Admin] Failed to delete trading post with ID %s: %v", postID, err)
		} else {
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				msg = "Trading post deleted successfully."
				log.Printf("[I] [Admin] Admin deleted trading post with ID %s.", postID)
			} else {
				msg = "Trading post not found."
			}
		}
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// reparseTradingPostItems handles the database transaction for updating items.
func reparseTradingPostItems(postID int, itemsToUpdate []GeminiTradeItem) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to start database transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Delete all old items
	_, err = tx.Exec("DELETE FROM trading_post_items WHERE post_id = ?", postID)
	if err != nil {
		return 0, fmt.Errorf("failed to clear old items: %w", err)
	}

	if len(itemsToUpdate) == 0 {
		// No new items to add, just commit the deletion
		return 0, tx.Commit()
	}

	// 2. Prepare statement for new items
	stmt, err := tx.Prepare(`
		INSERT INTO trading_post_items 
		(post_id, item_name, item_id, quantity, price_zeny, price_rmt, payment_methods, refinement, slots, card1, card2, card3, card4) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, fmt.Errorf("database preparation failed: %w", err)
	}
	defer stmt.Close()

	// 3. Insert new items
	for _, item := range itemsToUpdate {
		itemName := sanitizeString(item.Name, itemSanitizer)
		if strings.TrimSpace(itemName) == "" {
			continue
		}

		itemID, findErr := findItemIDByName(itemName, true, item.Slots)
		if findErr != nil {
			log.Printf("[W] [Admin/Reparse] Error finding item ID for '%s' during re-parse: %v. Proceeding without ID.", itemName, findErr)
		}

		paymentMethods := "zeny"
		if item.PaymentMethods == "rmt" || item.PaymentMethods == "both" {
			paymentMethods = item.PaymentMethods
		}

		card1 := sql.NullString{String: item.Card1, Valid: item.Card1 != ""}
		card2 := sql.NullString{String: item.Card2, Valid: item.Card2 != ""}
		card3 := sql.NullString{String: item.Card3, Valid: item.Card3 != ""}
		card4 := sql.NullString{String: item.Card4, Valid: item.Card4 != ""}

		_, err := stmt.Exec(postID, itemName, itemID, item.Quantity, item.PriceZeny, item.PriceRMT, paymentMethods, item.Refinement, item.Slots, card1, card2, card3, card4)
		if err != nil {
			return 0, fmt.Errorf("failed to save item '%s': %w", itemName, err)
		}
	}

	// 4. Commit
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to finalize transaction: %w", err)
	}

	return len(itemsToUpdate), nil
}

// adminReparseTradingPostHandler now orchestrates the re-parse.
func adminReparseTradingPostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error parsing form."), http.StatusSeeOther)
		return
	}

	postIDStr := r.FormValue("post_id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Error: Invalid post ID."), http.StatusSeeOther)
		return
	}

	var msg string
	var originalMessage, originalPostType, characterName sql.NullString

	// 1. Fetch the post to re-parse
	err = db.QueryRow("SELECT notes, post_type, character_name FROM trading_posts WHERE id = ?", postID).Scan(&originalMessage, &originalPostType, &characterName)
	if err != nil {
		if err == sql.ErrNoRows {
			msg = "Error: Post not found."
		} else {
			msg = "Error: Database query failed."
			log.Printf("[E] [Admin/Reparse] Failed to fetch post %d for re-parse: %v", postID, err)
		}
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
		return
	}

	// 2. Validate the post data
	if !originalMessage.Valid || originalMessage.String == "" {
		msg = "Error: Post has no original message (notes) to re-parse."
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
		return
	}
	if !originalPostType.Valid || (originalPostType.String != "buying" && originalPostType.String != "selling") {
		msg = "Error: Post has an invalid type."
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
		return
	}

	// 3. Parse with Gemini
	geminiResult, geminiErr := parseTradeMessageWithGemini(originalMessage.String)
	if geminiErr != nil {
		msg = fmt.Sprintf("Error: Gemini parse failed: %s", geminiErr.Error())
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
		return
	}

	// 4. Filter items that match the post type (e.g., only "selling" items for a "selling" post)
	var itemsToUpdate []GeminiTradeItem
	for _, item := range geminiResult.Items {
		if item.Action == originalPostType.String {
			itemsToUpdate = append(itemsToUpdate, item)
		}
	}

	if len(itemsToUpdate) == 0 {
		log.Printf("[I] [Admin/Reparse] Admin re-parsed post %d. No items matching type '%s' were found by Gemini. Clearing items.", postID, originalPostType.String)
	}

	// 5. Execute the database transaction
	itemsUpdated, err := reparseTradingPostItems(postID, itemsToUpdate)
	if err != nil {
		msg = fmt.Sprintf("Error: Database update failed: %s", err.Error())
		log.Printf("[E] [Admin/Reparse] Failed to re-parse post %d: %v", postID, err)
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
		return
	}

	// 6. Success
	log.Printf("[I] [Admin] Admin successfully re-parsed trading post %d (%s) with %d items.", postID, characterName.String, itemsUpdated)
	msg = fmt.Sprintf("Successfully re-parsed post %d. Found %d items.", postID, itemsUpdated)
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// adminShowEditTradingPostPage handles the GET request to show the edit form.
func adminShowEditTradingPostPage(w http.ResponseWriter, r *http.Request, postID int) {
	var post TradingPost
	var createdAtStr string
	err := db.QueryRow(`
		SELECT id, post_type, character_name, contact_info, created_at, notes 
		FROM trading_posts WHERE id = ?
	`, postID).Scan(&post.ID, &post.PostType, &post.CharacterName, &post.ContactInfo, &createdAtStr, &post.Notes)

	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Post not found", http.StatusNotFound)
		} else {
			http.Error(w, "Database query failed", http.StatusInternalServerError)
		}
		return
	}
	post.CreatedAt = createdAtStr

	itemRows, err := db.Query(`
		SELECT i.item_name, i.item_id, i.quantity, i.price_zeny, i.price_rmt, i.payment_methods, 
		       i.refinement, i.slots, i.card1, i.card2, i.card3, i.card4, local_db.name_pt
		FROM trading_post_items i
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
		WHERE i.post_id = ?
	`, postID)
	if err != nil {
		http.Error(w, "Database item query failed", http.StatusInternalServerError)
		return
	}
	defer itemRows.Close()

	for itemRows.Next() {
		var item TradingPostItem
		if err := itemRows.Scan(
			&item.ItemName, &item.ItemID, &item.Quantity, &item.PriceZeny, &item.PriceRMT, &item.PaymentMethods,
			&item.Refinement, &item.Slots, &item.Card1, &item.Card2, &item.Card3, &item.Card4, &item.NamePT, // Added NamePT
		); err != nil {
			log.Printf("[W] [Admin/Edit] Failed to scan trading post item row for edit: %v", err)
			continue
		}
		post.Items = append(post.Items, item)
	}

	tmpl, err := template.ParseFiles("admin_edit_post.html")
	if err != nil {
		http.Error(w, "Could not load edit template", http.StatusInternalServerError)
		log.Printf("[E] [HTTP] Could not load admin_edit_post.html: %v", err)
		return
	}

	data := AdminEditPostPageData{
		Post:           post,
		LastScrapeTime: GetLastScrapeTime(),
	}
	tmpl.Execute(w, data)
}

// adminHandleEditTradingPost handles the POST request to save changes.
func adminHandleEditTradingPost(w http.ResponseWriter, r *http.Request, postID int) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Failed to start database transaction.", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// 1. Update the main post details
	_, err = tx.Exec(`
		UPDATE trading_posts SET post_type=?, character_name=?, contact_info=?, notes=? WHERE id=?
	`, r.FormValue("post_type"), r.FormValue("character_name"), r.FormValue("contact_info"), r.FormValue("notes"), postID)
	if err != nil {
		http.Error(w, "Failed to update post.", http.StatusInternalServerError)
		log.Printf("[E] [Admin/Edit] Failed to update trading post %d: %v", postID, err)
		return
	}

	// 2. Clear all old items for this post
	_, err = tx.Exec("DELETE FROM trading_post_items WHERE post_id = ?", postID)
	if err != nil {
		http.Error(w, "Failed to clear old items.", http.StatusInternalServerError)
		log.Printf("[E] [Admin/Edit] Failed to delete old items for post %d: %v", postID, err)
		return
	}

	// 3. Insert all new/edited items from the form
	itemNames := r.Form["item_name[]"]
	if len(itemNames) > 0 {
		err := insertTradingPostItemsFromForm(tx, postID, r.Form)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("[E] [Admin/Edit] Failed to insert items for post %d: %v", postID, err)
			return
		}
	}

	// 4. Commit transaction
	if err := tx.Commit(); err != nil {
		http.Error(w, "Failed to finalize transaction.", http.StatusInternalServerError)
		return
	}

	log.Printf("[I] [Admin] Admin edited trading post with ID %d.", postID)
	http.Redirect(w, r, adminRedirectURL(r, "Trading post updated successfully."), http.StatusSeeOther)
}

// adminEditTradingPostHandler is now just a router for GET/POST.
func adminEditTradingPostHandler(w http.ResponseWriter, r *http.Request) {
	postIDStr := r.URL.Query().Get("id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		http.Error(w, "Invalid Post ID", http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodPost {
		adminHandleEditTradingPost(w, r, postID)
	} else {
		adminShowEditTradingPostPage(w, r, postID)
	}
}

func adminClearTradingPostItemsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	var msg string

	_, err := db.Exec("DROP TABLE IF EXISTS trading_post_items")
	if err != nil {
		log.Printf("[E] [Admin] Failed to drop trading_post_items table: %v", err)
		msg = "Database error while dropping trading post items table."
	} else {
		msg = "Successfully dropped the trading_post_items table."
		log.Printf("[I] [Admin] Admin dropped the trading_post_items table.")
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

func adminClearTradingPostsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	var msg string
	var err error

	_, err = db.Exec("DROP TABLE IF EXISTS trading_post_items")
	if err != nil {
		log.Printf("[E] [Admin] Failed to drop trading_post_items table (dependency): %v", err)
		msg = "Database error while dropping dependent items table."
		http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
		return
	}

	_, err = db.Exec("DROP TABLE IF EXISTS trading_posts")
	if err != nil {
		log.Printf("[E... ] [Admin] Failed to drop trading_posts table: %v", err)
		msg = "Database error while dropping trading_posts table."
	} else {
		msg = "Successfully dropped the trading_posts and trading_post_items tables."
		log.Printf("[I] [Admin] Admin dropped the trading_posts and trading_post_items tables.")
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// insertTradingPostItemsFromForm processes form data and inserts items into the DB.
func insertTradingPostItemsFromForm(tx *sql.Tx, postID int, form url.Values) error {
	itemNames := form["item_name[]"]
	quantities := form["quantity[]"]
	pricesZeny := form["price_zeny[]"]
	pricesRMT := form["price_rmt[]"]
	paymentMethods := form["payment_methods[]"]
	refinements := form["refinement[]"]
	slots := form["slots[]"]
	cards1 := form["card1[]"]
	cards2 := form["card2[]"]
	cards3 := form["card3[]"]
	cards4 := form["card4[]"]

	// Basic validation: ensure all arrays have the same length
	numItems := len(itemNames)
	if len(quantities) != numItems || len(pricesZeny) != numItems || len(pricesRMT) != numItems ||
		len(paymentMethods) != numItems || len(refinements) != numItems || len(slots) != numItems ||
		len(cards1) != numItems || len(cards2) != numItems || len(cards3) != numItems || len(cards4) != numItems {
		return fmt.Errorf("form data mismatch: item arrays have different lengths")
	}

	stmt, err := tx.Prepare(`
		INSERT INTO trading_post_items 
		(post_id, item_name, item_id, quantity, price_zeny, price_rmt, payment_methods, refinement, slots, card1, card2, card3, card4) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("database preparation failed: %w", err)
	}
	defer stmt.Close()

	for i := 0; i < numItems; i++ {
		itemName := sanitizeString(itemNames[i], itemSanitizer)
		if strings.TrimSpace(itemName) == "" {
			continue // Skip empty item names
		}

		qty, _ := strconv.Atoi(quantities[i])
		priceZ, _ := strconv.ParseInt(pricesZeny[i], 10, 64)
		priceR, _ := strconv.ParseInt(pricesRMT[i], 10, 64)
		refine, _ := strconv.Atoi(refinements[i])
		slotCount, _ := strconv.Atoi(slots[i])

		payment := "zeny" // Default
		if len(paymentMethods) > i {
			p := strings.ToLower(paymentMethods[i])
			if p == "rmt" || p == "both" {
				payment = p
			}
		}

		// Find Item ID (best effort)
		itemID, findErr := findItemIDByName(itemName, true, slotCount)
		if findErr != nil {
			log.Printf("[W] [Admin/Edit] Error finding item ID for '%s': %v. Proceeding without ID.", itemName, findErr)
		}

		card1 := sql.NullString{String: cards1[i], Valid: cards1[i] != ""}
		card2 := sql.NullString{String: cards2[i], Valid: cards2[i] != ""}
		card3 := sql.NullString{String: cards3[i], Valid: cards3[i] != ""}
		card4 := sql.NullString{String: cards4[i], Valid: cards4[i] != ""}

		_, err := stmt.Exec(
			postID, itemName, itemID, qty, priceZ, priceR, payment, refine, slotCount,
			card1, card2, card3, card4,
		)
		if err != nil {
			// Log the specific item that failed, makes debugging easier
			log.Printf("[E] [Admin/Edit] Failed to save item '%s' (index %d) for post %d: %v", itemName, i, postID, err)
			return fmt.Errorf("failed to save item '%s': %w", itemName, err)
		}
	}

	return nil
}

// --- NEW: Admin handler to trigger the backfill ---
func adminBackfillDropLogsHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("[I] [Admin] Manual backfill of drop logs triggered.")

	count, err := backfillDropLogsToChangelog()
	if err != nil {
		log.Printf("[E] [Admin] Drop log backfill failed: %v", err)
		// Use the existing redirect helper
		http.Redirect(w, r, adminRedirectURL(r, "Drop backfill failed."), http.StatusSeeOther)
		return
	}

	msg := fmt.Sprintf("Drop log backfill complete. %d new entries added.", count)
	log.Printf("[I] [Admin] %s", msg)
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// --- NEW: Core logic to perform the backfill ---
func backfillDropLogsToChangelog() (int64, error) {
	log.Println("[I] [Backfill] Starting drop log backfill process...")

	// 1. Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback on error

	// 2. Delete all existing drop logs to prevent duplicates.
	// This makes the operation idempotent (safe to run multiple times).
	delRes, err := tx.Exec("DELETE FROM character_changelog WHERE activity_description LIKE 'Dropped item: %'")
	if err != nil {
		return 0, fmt.Errorf("failed to delete old drop logs: %w", err)
	}
	deletedCount, _ := delRes.RowsAffected()
	log.Printf("[I] [Backfill] Deleted %d old drop log entries.", deletedCount)

	// 3. Query all drop messages from the chat table
	rows, err := tx.Query("SELECT message, timestamp FROM chat WHERE channel = 'Drop' AND character_name = 'System'")
	if err != nil {
		return 0, fmt.Errorf("failed to query chat table for drops: %w", err)
	}
	defer rows.Close()

	// 4. Prepare the INSERT statement
	stmt, err := tx.Prepare("INSERT INTO character_changelog (character_name, change_time, activity_description) VALUES (?, ?, ?)")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	var newEntriesCount int64 = 0
	// --- ADDED: Counters for logging ---
	var processedRows int = 0
	var failedRegex1 int = 0
	var failedRegex2 int = 0
	var failedInsert int = 0
	// --- END ADDED ---

	// 5. Loop, Parse, and Insert
	for rows.Next() {
		processedRows++ // <-- ADDED
		var msg, timestampStr string
		if err := rows.Scan(&msg, &timestampStr); err != nil {
			log.Printf("[W] [Backfill] Failed to scan drop row: %v", err)
			continue
		}

		// Parse the player name using the existing regex
		dropMatches := dropMessageRegex.FindStringSubmatch(msg)
		var itemMsgFragment, playerName string
		if len(dropMatches) == 4 {
			playerName = dropMatches[1]
			itemMsgFragment = dropMatches[3]
			// --- ADDED LOG ---
			log.Printf("[D] [Backfill] Parsed message: Player='%s', Fragment='%s'", playerName, itemMsgFragment)
		} else {
			// --- ADDED LOG ---
			log.Printf("[D] [Backfill] dropMessageRegex FAILED for msg: %s", msg)
			failedRegex1++
			continue // Not a valid drop message
		}

		// Parse the item name using the existing regex
		itemMatches := reItemFromDrop.FindStringSubmatch(itemMsgFragment)
		var itemName string
		if len(itemMatches) == 4 {
			if itemMatches[1] != "" {
				itemName = itemMatches[1]
			} else if itemMatches[2] != "" {
				itemName = itemMatches[2]
			} else if itemMatches[3] != "" {
				itemName = itemMatches[3]
			}
		}
		itemName = strings.TrimSpace(itemName)
		if itemName == "" {
			// --- ADDED LOG ---
			log.Printf("[D] [Backfill] reItemFromDrop FAILED for fragment: %s", itemMsgFragment)
			failedRegex2++
			continue // Couldn't parse item name
		}

		// --- ADDED LOG ---
		log.Printf("[D] [Backfill] Parsed item name: '%s'", itemName)

		// Create the new activity description
		activityDesc := fmt.Sprintf("Dropped item: %s", itemName)

		// --- ADDED LOG ---
		log.Printf("[D] [Backfill] Attempting to insert: CHAR='%s', TIME='%s', DESC='%s'", playerName, timestampStr, activityDesc)
		_, err := stmt.Exec(playerName, timestampStr, activityDesc)
		if err != nil {
			log.Printf("[W] [Backfill] FAILED to insert log for '%s' (time: %s, item: %s). Error: %v", playerName, timestampStr, itemName, err)
			failedInsert++
			continue
		}
		newEntriesCount++
	}

	// 6. Commit transaction
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// --- ADDED: Final Summary Log ---
	log.Printf("[I] [Backfill] ----- Backfill Summary -----")
	log.Printf("[I] [Backfill] Total Rows Processed: %d", processedRows)
	log.Printf("[I] [Backfill] Failed Player/Verb Regex: %d", failedRegex1)
	log.Printf("[I] [Backfill] Failed Item Name Regex: %d", failedRegex2)
	log.Printf("[I] [Backfill] Failed DB Inserts (FK error?): %d", failedInsert)
	log.Printf("[I] [Backfill] Successfully Inserted: %d", newEntriesCount)
	log.Printf("[I] [Backfill] ------------------------------")
	// --- END ADDED ---

	return newEntriesCount, nil
}

// --- NEW: Handler to cleanup redundant guild history ---
func adminCleanupGuildHistoryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	log.Println("[I] [Admin] Manual cleanup of guild history duplicates triggered.")

	// 1. Query all guild-related logs, ordered by character and then time
	// We need strictly ordered logs to compare current vs previous row.
	rows, err := db.Query(`
		SELECT id, character_name, activity_description 
		FROM character_changelog 
		WHERE activity_description LIKE '%guild%' 
		ORDER BY character_name ASC, change_time ASC, id ASC
	`)
	if err != nil {
		log.Printf("[E] [Admin] Failed to query guild logs for cleanup: %v", err)
		http.Redirect(w, r, adminRedirectURL(r, "Database error querying logs."), http.StatusSeeOther)
		return
	}
	defer rows.Close()

	var idsToDelete []interface{}
	var lastChar string
	var lastDesc string

	// 2. Iterate and find sequential duplicates
	for rows.Next() {
		var id int
		var charName, desc string
		if err := rows.Scan(&id, &charName, &desc); err != nil {
			continue
		}

		// If we switched characters, reset the tracker
		if charName != lastChar {
			lastChar = charName
			lastDesc = desc
			continue
		}

		// Same character. Check if description is identical to the previous one.
		// This catches:
		// 1. "Joined guild A" -> "Joined guild A" (Delete 2nd)
		// 2. "Left guild B" -> "Left guild B" (Delete 2nd)
		if desc == lastDesc {
			idsToDelete = append(idsToDelete, id)
		} else {
			// Update lastDesc only if it wasn't a duplicate (keep the "original" as the comparison point)
			lastDesc = desc
		}
	}

	if len(idsToDelete) == 0 {
		http.Redirect(w, r, adminRedirectURL(r, "No duplicate guild entries found."), http.StatusSeeOther)
		return
	}

	// 3. Perform Deletion (in batches to be safe with SQL variable limits)
	tx, err := db.Begin()
	if err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Transaction error."), http.StatusSeeOther)
		return
	}
	defer tx.Rollback()

	batchSize := 50
	totalDeleted := 0

	for i := 0; i < len(idsToDelete); i += batchSize {
		end := i + batchSize
		if end > len(idsToDelete) {
			end = len(idsToDelete)
		}
		batch := idsToDelete[i:end]

		placeholders := strings.Repeat("?,", len(batch)-1) + "?"
		query := fmt.Sprintf("DELETE FROM character_changelog WHERE id IN (%s)", placeholders)

		if _, err := tx.Exec(query, batch...); err != nil {
			log.Printf("[E] [Admin] Failed to delete batch of duplicate logs: %v", err)
		} else {
			totalDeleted += len(batch)
		}
	}

	if err := tx.Commit(); err != nil {
		http.Redirect(w, r, adminRedirectURL(r, "Commit error."), http.StatusSeeOther)
		return
	}

	msg := fmt.Sprintf("Successfully removed %d duplicate guild history entries.", totalDeleted)
	log.Printf("[I] [Admin] %s", msg)
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// 1. New Helper: Fetch Chat Messages for Admin
func getAdminChatMessages(r *http.Request, stats *AdminDashboardData) {
	const messagesPerPage = 50
	pageStr := r.URL.Query().Get("chat_page")
	page, _ := strconv.Atoi(pageStr)
	if page < 1 {
		page = 1
	}
	searchQuery := r.URL.Query().Get("chat_query")
	stats.ChatSearchQuery = searchQuery

	// Build WHERE clause
	whereClause := "1=1"
	var params []interface{}
	if searchQuery != "" {
		whereClause += " AND (message LIKE ? OR character_name LIKE ?)"
		params = append(params, "%"+searchQuery+"%", "%"+searchQuery+"%")
	}

	// Count total
	var total int
	db.QueryRow("SELECT COUNT(*) FROM chat WHERE "+whereClause, params...).Scan(&total)
	stats.ChatTotalMessages = total
	stats.ChatTotalPages = (total + messagesPerPage - 1) / messagesPerPage
	stats.ChatCurrentPage = page

	if page > 1 {
		stats.ChatHasPrevPage = true
		stats.ChatPrevPage = page - 1
	}
	if page < stats.ChatTotalPages {
		stats.ChatHasNextPage = true
		stats.ChatNextPage = page + 1
	}

	offset := (page - 1) * messagesPerPage

	// Fetch Data
	query := fmt.Sprintf(`
		SELECT id, timestamp, channel, character_name, message 
		FROM chat 
		WHERE %s 
		ORDER BY timestamp DESC 
		LIMIT ? OFFSET ?`, whereClause)

	params = append(params, messagesPerPage, offset)

	rows, err := db.Query(query, params...)
	if err != nil {
		log.Printf("[E] [Admin/Chat] Failed to query chat messages: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var msg ChatMessage
		var ts string
		if err := rows.Scan(&msg.ID, &ts, &msg.Channel, &msg.CharacterName, &msg.Message); err == nil {
			if t, parseErr := time.Parse(time.RFC3339, ts); parseErr == nil {
				msg.Timestamp = t.Format("2006-01-02 15:04:05")
			} else {
				msg.Timestamp = ts
			}
			stats.ChatMessages = append(stats.ChatMessages, msg)
		}
	}
}

// 2. Update getAdminDashboardData to call the helper when on the 'chat' tab
// Find the existing getAdminDashboardData function and add this block:
/*
	// ... existing tasks ...

*/

// 3. New Handler: Delete Chat Message
func adminDeleteChatHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin?tab=chat", http.StatusSeeOther)
		return
	}

	idStr := r.FormValue("id")
	if idStr == "" {
		http.Redirect(w, r, adminRedirectURL(r, "Error: Missing ID."), http.StatusSeeOther)
		return
	}

	_, err := db.Exec("DELETE FROM chat WHERE id = ?", idStr)
	msg := "Chat message deleted."
	if err != nil {
		log.Printf("[E] [Admin] Failed to delete chat message %s: %v", idStr, err)
		msg = "Error deleting message."
	} else {
		log.Printf("[I] [Admin] Deleted chat message ID %s.", idStr)
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// 4. New Handler: Edit Chat Message
func adminEditChatHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin?tab=chat", http.StatusSeeOther)
		return
	}

	idStr := r.FormValue("id")
	charName := r.FormValue("character_name")
	message := r.FormValue("message")
	channel := r.FormValue("channel")

	if idStr == "" || message == "" {
		http.Redirect(w, r, adminRedirectURL(r, "Error: Missing ID or Message."), http.StatusSeeOther)
		return
	}

	_, err := db.Exec("UPDATE chat SET character_name = ?, message = ?, channel = ? WHERE id = ?", charName, message, channel, idStr)
	msg := "Chat message updated."
	if err != nil {
		log.Printf("[E] [Admin] Failed to update chat message %s: %v", idStr, err)
		msg = "Error updating message."
	} else {
		log.Printf("[I] [Admin] Updated chat message ID %s.", idStr)
	}

	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}
