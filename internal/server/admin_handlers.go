package server

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// adminUser, adminPass, and basicAuth moved to middleware.go.

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

	rodbResults, rodbErr := scrapeRODatabaseSearch(rmsLiveSearchQuery, 0)
	if rodbErr != nil {
		log.Printf("[W] [Admin/Search] Admin RMS Live Search (RODB) query error: %v", rodbErr)
	}

	combinedResults := make([]ItemSearchResult, 0)
	seenIDs := make(map[int]bool)

	for _, res := range rodbResults {
		if !seenIDs[res.ID] {
			combinedResults = append(combinedResults, res)
			seenIDs[res.ID] = true
		}
	}

	stats.RMSLiveSearchResults = combinedResults
	log.Printf("[I] [Admin] Admin live search for '%s' found %d combined results.", rmsLiveSearchQuery, len(combinedResults))
}

// getAdminDashboardData orchestrates fetching all data for the admin dashboard concurrently.
// Each task writes to its own local AdminDashboardData so the goroutines never share
// memory; results are merged into stats after g.Wait() returns.
func getAdminDashboardData(r *http.Request) (AdminDashboardData, error) {
	stats := AdminDashboardData{
		Message: r.URL.Query().Get("msg"),
	}

	var (
		g                                                                  errgroup.Group
		statsR, guildsR, pageViewsR, tpR, rmsCacheR, rmsLiveR, visitsR, chatR AdminDashboardData
	)

	// Task 1: Main Stats (Critical)
	g.Go(func() error {
		if err := getDashboardStats(&statsR); err != nil {
			log.Printf("[E] [Admin] Failed to load dashboard stats: %v", err)
			return err
		}
		return nil
	})

	// Task 2: Guilds (Not Critical)
	g.Go(func() error {
		if err := getDashboardGuilds(&guildsR); err != nil {
			log.Printf("[W] [Admin] Could not load dashboard guilds: %v", err)
		}
		return nil
	})

	// Task 3: Page Views (Not Critical)
	g.Go(func() error {
		getDashboardPageViews(r, &pageViewsR)
		return nil
	})

	// Task 4: Trading Posts (Not Critical)
	g.Go(func() error {
		getDashboardTradingPosts(r, &tpR)
		return nil
	})

	// Task 5: RMS Cache Search (Not Critical)
	g.Go(func() error {
		performRMSCacheSearch(r, &rmsCacheR)
		return nil
	})

	// Task 6: RMS Live Search (Not Critical)
	g.Go(func() error {
		performRMSLiveSearch(r, &rmsLiveR)
		return nil
	})

	// Task 7: Chat Messages (Only if tab is chat)
	if r.URL.Query().Get("tab") == "chat" {
		g.Go(func() error {
			getAdminChatMessages(r, &chatR)
			return nil
		})
	}

	g.Go(func() error {
		if err := getDashboardPageVisitCounts(&visitsR); err != nil {
			log.Printf("[W] [Admin] Could not load page visit counts: %v", err)
		}
		return nil
	})

	if mainErr := g.Wait(); mainErr != nil {
		return stats, mainErr
	}

	// Merge disjoint fields back into stats.
	copyDashboardStats(&stats, &statsR)
	stats.AllGuilds = guildsR.AllGuilds
	stats.MostVisitedPage = pageViewsR.MostVisitedPage
	stats.MostVisitedPageCount = pageViewsR.MostVisitedPageCount
	stats.PageViewsTotal = pageViewsR.PageViewsTotal
	stats.PageViewsTotalPages = pageViewsR.PageViewsTotalPages
	stats.PageViewsCurrentPage = pageViewsR.PageViewsCurrentPage
	stats.PageViewsHasPrevPage = pageViewsR.PageViewsHasPrevPage
	stats.PageViewsPrevPage = pageViewsR.PageViewsPrevPage
	stats.PageViewsHasNextPage = pageViewsR.PageViewsHasNextPage
	stats.PageViewsNextPage = pageViewsR.PageViewsNextPage
	stats.RecentPageViews = pageViewsR.RecentPageViews
	stats.TradingPostTotal = tpR.TradingPostTotal
	stats.TradingPostTotalPages = tpR.TradingPostTotalPages
	stats.TradingPostCurrentPage = tpR.TradingPostCurrentPage
	stats.TradingPostHasPrevPage = tpR.TradingPostHasPrevPage
	stats.TradingPostPrevPage = tpR.TradingPostPrevPage
	stats.TradingPostHasNextPage = tpR.TradingPostHasNextPage
	stats.TradingPostNextPage = tpR.TradingPostNextPage
	stats.RecentTradingPosts = tpR.RecentTradingPosts
	stats.RMSCacheSearchQuery = rmsCacheR.RMSCacheSearchQuery
	stats.RMSCacheSearchResults = rmsCacheR.RMSCacheSearchResults
	stats.RMSLiveSearchQuery = rmsLiveR.RMSLiveSearchQuery
	stats.RMSLiveSearchResults = rmsLiveR.RMSLiveSearchResults
	stats.PageVisitCounts = visitsR.PageVisitCounts
	stats.ChatSearchQuery = chatR.ChatSearchQuery
	stats.ChatTotalMessages = chatR.ChatTotalMessages
	stats.ChatTotalPages = chatR.ChatTotalPages
	stats.ChatCurrentPage = chatR.ChatCurrentPage
	stats.ChatHasPrevPage = chatR.ChatHasPrevPage
	stats.ChatPrevPage = chatR.ChatPrevPage
	stats.ChatHasNextPage = chatR.ChatHasNextPage
	stats.ChatNextPage = chatR.ChatNextPage
	stats.ChatMessages = chatR.ChatMessages

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

	tmpl, ok := templateCache["admin.html"]
	if !ok {
		http.Error(w, "Could not load admin template", http.StatusInternalServerError)
		log.Println("[E] [HTTP] admin.html template missing from cache")
		return
	}

	if err := tmpl.Execute(w, stats); err != nil {
		log.Printf("[E] [HTTP] Could not execute admin.html template: %v", err)
	}
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

	tmpl, ok := templateCache["admin.html"]
	if !ok {
		http.Error(w, "Could not load admin template", http.StatusInternalServerError)
		log.Println("[E] [HTTP] admin.html template missing from cache")
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, stats); err != nil {
		log.Printf("[E] [HTTP] Could not execute admin.html template: %v", err)
	}
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
