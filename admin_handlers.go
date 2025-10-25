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

// getDashboardStats populates the main database statistics concurrently.
func getDashboardStats(stats *AdminDashboardData) error {
	var wg sync.WaitGroup
	var queryErr error
	var mu sync.Mutex // To protect queryErr

	// Helper function to run a query and assign the result
	runQuery := func(query string, target *int) {
		defer wg.Done()
		var count sql.NullInt64
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			log.Printf("[W] [Admin/Stats] Dashboard stats query failed (%s): %v", query, err)
			mu.Lock()
			if queryErr == nil { // Store only the first error
				queryErr = err
			}
			mu.Unlock()
		}
		*target = int(count.Int64)
	}

	wg.Add(11)
	go runQuery("SELECT COUNT(*) FROM items", &stats.TotalItems)
	go runQuery("SELECT COUNT(*) FROM items WHERE is_available = 1", &stats.AvailableItems)
	go runQuery("SELECT COUNT(DISTINCT name_of_the_item) FROM items", &stats.UniqueItems)
	go runQuery("SELECT COUNT(*) FROM rms_item_cache", &stats.CachedItems)
	go runQuery("SELECT COUNT(*) FROM characters", &stats.TotalCharacters)
	go runQuery("SELECT COUNT(*) FROM guilds", &stats.TotalGuilds)
	go runQuery("SELECT COUNT(*) FROM player_history", &stats.PlayerHistoryEntries)
	go runQuery("SELECT COUNT(*) FROM market_events", &stats.MarketEvents)
	go runQuery("SELECT COUNT(*) FROM character_changelog", &stats.ChangelogEntries)
	go runQuery("SELECT COUNT(*) FROM visitors", &stats.TotalVisitors)
	go runQuery("SELECT COUNT(*) FROM visitors WHERE date(last_visit) = date('now', 'localtime')", &stats.VisitorsToday)

	wg.Wait()

	if queryErr != nil {
		return fmt.Errorf("could not query for one or more dashboard stats: %w", queryErr)
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

	ftsQuery := rmsQuery + "*"
	searchRows, err := db.Query(`
		SELECT rowid, name, name_pt 
		FROM rms_item_cache_fts 
		WHERE rms_item_cache_fts MATCH ? 
		ORDER BY rank 
		LIMIT 50`, ftsQuery)
	if err != nil {
		log.Printf("[W] [Admin/Stats] Admin RMS Cache FTS query error: %v", err)
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
	var rmsResults, rodbResults []ItemSearchResult
	var rmsErr, rodbErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		rmsResults, rmsErr = scrapeRMSItemSearch(rmsLiveSearchQuery)
		if rmsErr != nil {
			log.Printf("[W] [Admin/Search] Admin RMS Live Search (RMS) query error: %v", rmsErr)
		}
	}()
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

	if rmsResults != nil {
		for _, res := range rmsResults {
			if !seenIDs[res.ID] {
				combinedResults = append(combinedResults, res)
				seenIDs[res.ID] = true
			}
		}
	}
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
		http.Redirect(w, r, "/admin?msg=Error+parsing+form.", http.StatusSeeOther)
		return
	}

	itemIDStr := r.FormValue("item_id")
	itemName := r.FormValue("item_name")
	itemID, err := strconv.Atoi(itemIDStr)
	if err != nil {
		http.Redirect(w, r, "/admin?msg=Error:+Invalid+item+ID.", http.StatusSeeOther)
		return
	}

	if itemID <= 0 {
		http.Redirect(w, r, "/admin?msg=Error:+Invalid+item+ID.", http.StatusSeeOther)
		return
	}

	go scrapeAndCacheItemIfNotExists(itemID, itemName)

	msg := fmt.Sprintf("Caching+for+item+%d+(%s)+started+in+background.", itemID, url.QueryEscape(itemName))
	log.Printf("[I] [Admin] Admin triggered manual cache for item ID %d (%s).", itemID, itemName)

	http.Redirect(w, r, "/admin?rms_live_search="+url.QueryEscape(r.FormValue("rms_live_search"))+"&msg="+msg, http.StatusSeeOther)
}

func adminDeleteCacheEntryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin?msg=Error+parsing+form.", http.StatusSeeOther)
		return
	}

	itemIDs := r.Form["item_id[]"]
	if len(itemIDs) == 0 {
		http.Redirect(w, r, "/admin?msg=Error:+No+items+selected+for+deletion.", http.StatusSeeOther)
		return
	}

	query := "DELETE FROM rms_item_cache WHERE item_id IN (?" + strings.Repeat(",?", len(itemIDs)-1) + ")"

	args := make([]interface{}, len(itemIDs))
	for i, idStr := range itemIDs {
		args[i] = idStr
	}

	// --- THE FIX IS HERE ---
	rmsCacheMutex.Lock()
	result, err := db.Exec(query, args...)
	rmsCacheMutex.Unlock()
	// --- END FIX ---

	if err != nil {
		log.Printf("[E] [Admin] Failed to delete RMS cache entries: %v", err)
		http.Redirect(w, r, "/admin?msg=Database+error+while+deleting+entries.", http.StatusSeeOther)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	msg := fmt.Sprintf("Successfully+deleted+%d+cache+entries.", rowsAffected)
	log.Printf("[I] [Admin] Admin deleted %d RMS cache entries: %v", rowsAffected, itemIDs)

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
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
		http.Redirect(w, r, fmt.Sprintf("/admin?msg=%s+scrape+started.", name), http.StatusSeeOther)
	}
}

func adminCacheActionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	action := r.FormValue("action")
	var msg string

	// --- THE FIX IS HERE ---
	// Lock for all write actions (clear, drop)
	rmsCacheMutex.Lock()
	// --- END FIX ---

	switch action {
	case "clear":
		log.Println("[I] [Admin] Admin triggered cache clear.")
		_, err := db.Exec("DELETE FROM rms_item_cache")
		if err != nil {
			msg = "Error+clearing+cache."
			log.Printf("[E] [Admin] Failed to clear RMS cache: %v", err)
		} else {
			msg = "Item+cache+cleared+successfully."
		}
	case "drop":
		log.Println("[I] [Admin] Admin triggered cache table drop.")
		_, err := db.Exec("DROP TABLE IF EXISTS rms_item_cache")
		if err != nil {
			msg = "Error+dropping+cache+table."
			log.Printf("[E] [Admin] Failed to drop RMS cache table: %v", err)
		} else {
			msg = "Item+cache+table+dropped+successfully.+Restart+app+to+recreate."
		}
	case "repopulate":
		log.Println("[I] [Admin] Admin triggered cache repopulation.")
		go populateMissingCachesOnStartup()
		msg = "Cache+repopulation+started+in+background."
	default:
		msg = "Unknown+cache+action."
	}

	// --- THE FIX IS HERE ---
	rmsCacheMutex.Unlock()
	// --- END FIX ---

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}

func adminUpdateGuildEmblemHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin?msg=Error+parsing+form.", http.StatusSeeOther)
		return
	}

	guildName := r.FormValue("guild_name")
	emblemURL := r.FormValue("emblem_url")
	var msg string

	if guildName == "" || emblemURL == "" {
		msg = "Guild+name+and+URL+cannot+be+empty."
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	result, err := db.Exec("UPDATE guilds SET emblem_url = ? WHERE name = ?", emblemURL, guildName)
	if err != nil {
		log.Printf("[E] [Admin] Failed to update emblem for guild '%s': %v", guildName, err)
		msg = "Database+error+occurred."
	} else {
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			msg = fmt.Sprintf("Guild+'%s'+not+found.", url.QueryEscape(guildName))
		} else {
			msg = fmt.Sprintf("Emblem+for+'%s'+updated+successfully.", url.QueryEscape(guildName))
		}
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
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
		msg = "Database+error+while+clearing+activity+times."
	} else {
		rowsAffected, _ := result.RowsAffected()
		msg = fmt.Sprintf("Successfully+reset+last_active+time+for+%d+characters.", rowsAffected)
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
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
		msg = "Database+error+while+clearing+MVP+kills."
	} else {
		rowsAffected, _ := result.RowsAffected()
		msg = fmt.Sprintf("Successfully+deleted+%d+MVP+kill+records.", rowsAffected)
		log.Printf("[I] [Admin] Admin cleared all MVP kill data (%d records).", rowsAffected)
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}

func adminDeleteVisitorViewsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin?msg=Error+parsing+form.", http.StatusSeeOther)
		return
	}

	visitorHash := r.FormValue("visitor_hash")
	var msg string

	if visitorHash == "" {
		msg = "Error:+Missing+visitor+hash."
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("[E] [Admin] Failed to begin transaction for deleting visitor: %v", err)
		http.Redirect(w, r, "/admin?msg=Database+error+occurred.", http.StatusSeeOther)
		return
	}

	_, err = tx.Exec("DELETE FROM page_views WHERE visitor_hash = ?", visitorHash)
	if err != nil {
		tx.Rollback()
		log.Printf("[E] [Admin] Failed to delete from page_views for hash %s: %v", visitorHash, err)
		http.Redirect(w, r, "/admin?msg=Database+error+on+page_views.", http.StatusSeeOther)
		return
	}

	result, err := tx.Exec("DELETE FROM visitors WHERE visitor_hash = ?", visitorHash)
	if err != nil {
		tx.Rollback()
		log.Printf("[ECode] [Admin] Failed to delete from visitors for hash %s: %v", visitorHash, err)
		http.Redirect(w, r, "/admin?msg=Database+error+on+visitors.", http.StatusSeeOther)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[E] [Admin] Failed to commit transaction for deleting visitor: %v", err)
		http.Redirect(w, r, "/admin?msg=Database+commit+error.", http.StatusSeeOther)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		msg = "Visitor+records+removed+successfully."
		log.Printf("[I] [Admin] Admin removed all data for visitor with hash starting with %s...", visitorHash[:12])
	} else {
		msg = "Visitor+not+found+or+already+removed."
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}

func adminDeleteTradingPostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin?msg=Error+parsing+form.", http.StatusSeeOther)
		return
	}

	postID := r.FormValue("post_id")
	var msg string

	if postID == "" {
		msg = "Error:+Missing+post+ID."
	} else {

		result, err := db.Exec("DELETE FROM trading_posts WHERE id = ?", postID)
		if err != nil {
			msg = "Database+error+occurred+while+deleting+post."
			log.Printf("[E] [Admin] Failed to delete trading post with ID %s: %v", postID, err)
		} else {
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				msg = "Trading+post+deleted+successfully."
				log.Printf("[I] [Admin] Admin deleted trading post with ID %s.", postID)
			} else {
				msg = "Trading+post+not+found."
			}
		}
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
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
		http.Redirect(w, r, "/admin?msg=Error+parsing+form.", http.StatusSeeOther)
		return
	}

	postIDStr := r.FormValue("post_id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		http.Redirect(w, r, "/admin?msg=Error:+Invalid+post+ID.", http.StatusSeeOther)
		return
	}

	var msg string
	var originalMessage, originalPostType, characterName sql.NullString

	// 1. Fetch the post to re-parse
	err = db.QueryRow("SELECT notes, post_type, character_name FROM trading_posts WHERE id = ?", postID).Scan(&originalMessage, &originalPostType, &characterName)
	if err != nil {
		if err == sql.ErrNoRows {
			msg = "Error:+Post+not+found."
		} else {
			msg = "Error:+Database+query+failed."
			log.Printf("[E] [Admin/Reparse] Failed to fetch post %d for re-parse: %v", postID, err)
		}
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	// 2. Validate the post data
	if !originalMessage.Valid || originalMessage.String == "" {
		msg = "Error:+Post+has+no+original+message+(notes)+to+re-parse."
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}
	if !originalPostType.Valid || (originalPostType.String != "buying" && originalPostType.String != "selling") {
		msg = "Error:+Post+has+an+invalid+type."
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	// 3. Parse with Gemini
	geminiResult, geminiErr := parseTradeMessageWithGemini(originalMessage.String)
	if geminiErr != nil {
		msg = fmt.Sprintf("Error:+Gemini+parse+failed:+%s", url.QueryEscape(geminiErr.Error()))
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
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
		msg = fmt.Sprintf("Error:+Database+update+failed:+%s", url.QueryEscape(err.Error()))
		log.Printf("[E] [Admin/Reparse] Failed to re-parse post %d: %v", postID, err)
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	// 6. Success
	log.Printf("[I] [Admin] Admin successfully re-parsed trading post %d (%s) with %d items.", postID, characterName.String, itemsUpdated)
	msg = fmt.Sprintf("Successfully+re-parsed+post+%d.+Found+%d+items.", postID, itemsUpdated)
	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}

// --- Refactored AdminEditTradingPostHandler ---

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
		SELECT item_name, item_id, quantity, price_zeny, price_rmt, payment_methods, refinement, slots, card1, card2, card3, card4 
		FROM trading_post_items WHERE post_id = ?
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
			&item.Refinement, &item.Slots, &item.Card1, &item.Card2, &item.Card3, &item.Card4,
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
	http.Redirect(w, r, "/admin?msg=Trading+post+updated+successfully.", http.StatusSeeOther)
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
		msg = "Database+error+while+dropping+trading+post+items+table."
	} else {
		msg = "Successfully+dropped+the+trading_post_items+table."
		log.Printf("[I] [Admin] Admin dropped the trading_post_items table.")
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
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
		msg = "Database+error+while+dropping+dependent+items+table."
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	_, err = db.Exec("DROP TABLE IF EXISTS trading_posts")
	if err != nil {
		log.Printf("[E... ] [Admin] Failed to drop trading_posts table: %v", err)
		msg = "Database+error+while+dropping+trading_posts+table."
	} else {
		msg = "Successfully+dropped+the+trading_posts+and+trading_post_items+tables."
		log.Printf("[I] [Admin] Admin dropped the trading_posts and trading_post_items tables.")
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
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
