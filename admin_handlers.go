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
)

const adminUser = "admin"

var adminPass string

func basicAuth(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()

		if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(adminUser)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(adminPass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintln(w, "Unauthorized.")
			return
		}

		handler(w, r)
	}
}

func getAdminDashboardData(r *http.Request) (AdminDashboardData, error) {
	stats := AdminDashboardData{
		Message: r.URL.Query().Get("msg"),
	}

	guildRows, err := db.Query("SELECT name, COALESCE(emblem_url, '') FROM guilds ORDER BY name ASC")
	if err != nil {
		return stats, fmt.Errorf("could not query for guild list for admin page: %w", err)
	}
	defer guildRows.Close()
	for guildRows.Next() {
		var info GuildInfo
		if err := guildRows.Scan(&info.Name, &info.EmblemURL); err != nil {
			log.Printf("⚠️ Failed to scan guild info for admin page: %v", err)
			continue
		}
		stats.AllGuilds = append(stats.AllGuilds, info)
	}

	statsQuery := `
		SELECT
			(SELECT COUNT(*) FROM items),
			(SELECT COUNT(*) FROM items WHERE is_available = 1),
			(SELECT COUNT(DISTINCT name_of_the_item) FROM items),
			(SELECT COUNT(*) FROM rms_item_cache),
			(SELECT COUNT(*) FROM characters),
			(SELECT COUNT(*) FROM guilds),
			(SELECT COUNT(*) FROM player_history),
			(SELECT COUNT(*) FROM market_events),
			(SELECT COUNT(*) FROM character_changelog),
			(SELECT COUNT(*) FROM visitors),
			(SELECT COUNT(*) FROM visitors WHERE date(last_visit) = date('now', 'localtime'))
	`

	var (
		totalItems, availableItems, uniqueItems, cachedItems,
		totalCharacters, totalGuilds, playerHistoryEntries,
		marketEvents, changelogEntries, totalVisitors, visitorsToday sql.NullInt64
	)

	err = db.QueryRow(statsQuery).Scan(
		&totalItems, &availableItems, &uniqueItems, &cachedItems,
		&totalCharacters, &totalGuilds, &playerHistoryEntries,
		&marketEvents, &changelogEntries, &totalVisitors, &visitorsToday,
	)
	if err != nil {
		return stats, fmt.Errorf("could not query for dashboard stats: %w", err)
	}

	stats.TotalItems = int(totalItems.Int64)
	stats.AvailableItems = int(availableItems.Int64)
	stats.UniqueItems = int(uniqueItems.Int64)
	stats.CachedItems = int(cachedItems.Int64)
	stats.TotalCharacters = int(totalCharacters.Int64)
	stats.TotalGuilds = int(totalGuilds.Int64)
	stats.PlayerHistoryEntries = int(playerHistoryEntries.Int64)
	stats.MarketEvents = int(marketEvents.Int64)
	stats.ChangelogEntries = int(changelogEntries.Int64)
	stats.TotalVisitors = int(totalVisitors.Int64)
	stats.VisitorsToday = int(visitorsToday.Int64)

	err = db.QueryRow(`
		SELECT page_path, COUNT(page_path) as Cnt
		FROM page_views
		GROUP BY page_path
		ORDER BY Cnt DESC
		LIMIT 1
	`).Scan(&stats.MostVisitedPage, &stats.MostVisitedPageCount)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ Could not query for most visited page: %v", err)
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
		log.Printf("⚠️ Could not query for recent page views: %v", err)
	} else {
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
		log.Printf("⚠️ Admin Trading Post query error: %v", err)
	} else {
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

	rmsQuery := r.URL.Query().Get("rms_query")
	stats.RMSCacheSearchQuery = rmsQuery
	if rmsQuery != "" {

		ftsQuery := rmsQuery + "*"
		searchRows, err := db.Query(`
			SELECT rowid, name, name_pt 
			FROM rms_item_cache_fts 
			WHERE rms_item_cache_fts MATCH ? 
			ORDER BY rank 
			LIMIT 50`, ftsQuery)
		if err != nil {
			log.Printf("⚠️ Admin RMS Cache FTS query error: %v", err)
		} else {
			defer searchRows.Close()
			for searchRows.Next() {
				var result RMSCacheSearchResult
				if err := searchRows.Scan(&result.ItemID, &result.Name, &result.NamePT); err == nil {
					stats.RMSCacheSearchResults = append(stats.RMSCacheSearchResults, result)
				}
			}
		}
	}

	rmsLiveSearchQuery := r.URL.Query().Get("rms_live_search")
	stats.RMSLiveSearchQuery = rmsLiveSearchQuery
	if rmsLiveSearchQuery != "" {
		log.Printf("👤 Admin performing live search for: '%s'", rmsLiveSearchQuery)
		var wg sync.WaitGroup
		rmsChan := make(chan []ItemSearchResult, 1)
		rodbChan := make(chan []ItemSearchResult, 1)

		wg.Add(2)
		go func() {
			defer wg.Done()
			results, err := scrapeRMSItemSearch(rmsLiveSearchQuery)
			if err != nil {
				log.Printf("⚠️ Admin RMS Live Search (RMS) query error: %v", err)
				rmsChan <- nil
				return
			}
			rmsChan <- results
		}()
		go func() {
			defer wg.Done()

			results, err := scrapeRODatabaseSearch(rmsLiveSearchQuery, 0)
			if err != nil {
				log.Printf("⚠️ Admin RMS Live Search (RODB) query error: %v", err)
				rodbChan <- nil
				return
			}
			rodbChan <- results
		}()
		wg.Wait()

		rmsResults := <-rmsChan
		rodbResults := <-rodbChan

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
		log.Printf("👤 Admin live search for '%s' found %d combined results.", rmsLiveSearchQuery, len(combinedResults))
	}

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
	log.Printf("👤 Admin triggered manual cache for item ID %d (%s).", itemID, itemName)

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

	result, err := db.Exec(query, args...)
	if err != nil {
		log.Printf("❌ Failed to delete RMS cache entries: %v", err)
		http.Redirect(w, r, "/admin?msg=Database+error+while+deleting+entries.", http.StatusSeeOther)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	msg := fmt.Sprintf("Successfully+deleted+%d+cache+entries.", rowsAffected)
	log.Printf("👤 Admin deleted %d RMS cache entries: %v", rowsAffected, itemIDs)

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}

func adminHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := getAdminDashboardData(r)
	if err != nil {
		http.Error(w, "Could not load dashboard data", http.StatusInternalServerError)
		log.Printf("❌ Failed to get admin dashboard data: %v", err)
		return
	}

	tmpl, err := template.New("admin.html").Funcs(templateFuncs).ParseFiles("admin.html")
	if err != nil {
		http.Error(w, "Could not load admin template", http.StatusInternalServerError)
		log.Printf("❌ Could not load admin.html template: %v", err)
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
		log.Printf("❌ Failed to get admin dashboard data for trade parse: %v", err)
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
		log.Printf("❌ Could not load admin.html template: %v", err)
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
		log.Printf("👤 Admin triggered '%s' scrape manually.", name)
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
	case "drop":
		log.Println("👤 Admin triggered cache table drop.")
		_, err := db.Exec("DROP TABLE IF EXISTS rms_item_cache")
		if err != nil {
			msg = "Error+dropping+cache+table."
			log.Printf("❌ Failed to drop RMS cache table: %v", err)
		} else {
			msg = "Item+cache+table+dropped+successfully.+Restart+app+to+recreate."
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
		log.Printf("❌ Failed to update emblem for guild '%s': %v", guildName, err)
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
		log.Printf("❌ Failed to clear last_active times: %v", err)
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
		log.Printf("❌ Failed to clear MVP kills table: %v", err)
		msg = "Database+error+while+clearing+MVP+kills."
	} else {
		rowsAffected, _ := result.RowsAffected()
		msg = fmt.Sprintf("Successfully+deleted+%d+MVP+kill+records.", rowsAffected)
		log.Printf("👤 Admin cleared all MVP kill data (%d records).", rowsAffected)
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
		log.Printf("❌ Failed to begin transaction for deleting visitor: %v", err)
		http.Redirect(w, r, "/admin?msg=Database+error+occurred.", http.StatusSeeOther)
		return
	}

	_, err = tx.Exec("DELETE FROM page_views WHERE visitor_hash = ?", visitorHash)
	if err != nil {
		tx.Rollback()
		log.Printf("❌ Failed to delete from page_views for hash %s: %v", visitorHash, err)
		http.Redirect(w, r, "/admin?msg=Database+error+on+page_views.", http.StatusSeeOther)
		return
	}

	result, err := tx.Exec("DELETE FROM visitors WHERE visitor_hash = ?", visitorHash)
	if err != nil {
		tx.Rollback()
		log.Printf("❌ Failed to delete from visitors for hash %s: %v", visitorHash, err)
		http.Redirect(w, r, "/admin?msg=Database+error+on+visitors.", http.StatusSeeOther)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ Failed to commit transaction for deleting visitor: %v", err)
		http.Redirect(w, r, "/admin?msg=Database+commit+error.", http.StatusSeeOther)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		msg = "Visitor+records+removed+successfully."
		log.Printf("👤 Admin removed all data for visitor with hash starting with %s...", visitorHash[:12])
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
			log.Printf("❌ Failed to delete trading post with ID %s: %v", postID, err)
		} else {
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				msg = "Trading+post+deleted+successfully."
				log.Printf("👤 Admin deleted trading post with ID %s.", postID)
			} else {
				msg = "Trading+post+not+found."
			}
		}
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}

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

	err = db.QueryRow("SELECT notes, post_type, character_name FROM trading_posts WHERE id = ?", postID).Scan(&originalMessage, &originalPostType, &characterName)
	if err != nil {
		if err == sql.ErrNoRows {
			msg = "Error:+Post+not+found."
		} else {
			msg = "Error:+Database+query+failed."
			log.Printf("❌ Failed to fetch post %d for re-parse: %v", postID, err)
		}
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

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

	geminiResult, geminiErr := parseTradeMessageWithGemini(originalMessage.String)
	if geminiErr != nil {
		msg = fmt.Sprintf("Error:+Gemini+parse+failed:+%s", url.QueryEscape(geminiErr.Error()))
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	var itemsToUpdate []GeminiTradeItem
	for _, item := range geminiResult.Items {
		if item.Action == originalPostType.String {
			itemsToUpdate = append(itemsToUpdate, item)
		}
	}

	if len(itemsToUpdate) == 0 {

		log.Printf("👤 Admin re-parsed post %d. No items matching type '%s' were found by Gemini. Clearing items.", postID, originalPostType.String)
	}

	tx, err := db.Begin()
	if err != nil {
		msg = "Error:+Failed+to+start+database+transaction."
		log.Printf("❌ Failed to begin transaction for re-parsing post %d: %v", postID, err)
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	_, err = tx.Exec("DELETE FROM trading_post_items WHERE post_id = ?", postID)
	if err != nil {
		tx.Rollback()
		msg = "Error:+Failed+to+clear+old+items."
		log.Printf("❌ Failed to delete old items for post %d during re-parse: %v", postID, err)
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	if len(itemsToUpdate) > 0 {
		stmt, err := tx.Prepare(`
			INSERT INTO trading_post_items 
			(post_id, item_name, item_id, quantity, price_zeny, price_rmt, payment_methods, refinement, slots, card1, card2, card3, card4) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			tx.Rollback()
			msg = "Error:+Database+preparation+failed."
			log.Printf("❌ Failed to prepare insert statement for re-parsing post %d: %v", postID, err)
			http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
			return
		}
		defer stmt.Close()

		for _, item := range itemsToUpdate {
			itemName := sanitizeString(item.Name, itemSanitizer)
			if strings.TrimSpace(itemName) == "" {
				continue
			}

			itemID, findErr := findItemIDByName(itemName, true, item.Slots)
			if findErr != nil {
				log.Printf("⚠️ Error finding item ID for '%s' during re-parse: %v. Proceeding without ID.", itemName, findErr)
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
				tx.Rollback()
				msg = "Error:+Failed+to+save+one+of+the+new+items."
				log.Printf("❌ Failed to insert re-parsed item '%s' for post %d: %v", itemName, postID, err)
				http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
				return
			}
		}
	}

	if err := tx.Commit(); err != nil {
		msg = "Error:+Failed+to+finalize+transaction."
		log.Printf("❌ Failed to commit transaction for re-parsing post %d: %v", postID, err)
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	log.Printf("👤 Admin successfully re-parsed trading post %d (%s) with %d items.", postID, characterName.String, len(itemsToUpdate))
	msg = fmt.Sprintf("Successfully+re-parsed+post+%d.+Found+%d+items.", postID, len(itemsToUpdate))
	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}

func adminEditTradingPostHandler(w http.ResponseWriter, r *http.Request) {
	postIDStr := r.URL.Query().Get("id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		http.Error(w, "Invalid Post ID", http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodPost {

		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "Failed to start database transaction.", http.StatusInternalServerError)
			return
		}

		_, err = tx.Exec(`
			UPDATE trading_posts SET post_type=?, character_name=?, contact_info=?, notes=? WHERE id=?
		`, r.FormValue("post_type"), r.FormValue("character_name"), r.FormValue("contact_info"), r.FormValue("notes"), postID)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to update post.", http.StatusInternalServerError)
			log.Printf("❌ Failed to update trading post %d: %v", postID, err)
			return
		}

		_, err = tx.Exec("DELETE FROM trading_post_items WHERE post_id = ?", postID)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to clear old items.", http.StatusInternalServerError)
			log.Printf("❌ Failed to delete old items for post %d: %v", postID, err)
			return
		}

		itemNames := r.Form["item_name[]"]
		itemIDs := r.Form["item_id[]"]
		quantities := r.Form["quantity[]"]
		pricesZeny := r.Form["price_zeny[]"]
		pricesRMT := r.Form["price_rmt[]"]
		paymentMethodsList := r.Form["payment_methods[]"]
		refinements := r.Form["refinement[]"]
		slotsList := r.Form["slots[]"]
		cards1 := r.Form["card1[]"]
		cards2 := r.Form["card2[]"]
		cards3 := r.Form["card3[]"]
		cards4 := r.Form["card4[]"]

		if len(itemNames) > 0 {
			stmt, err := tx.Prepare(`
				INSERT INTO trading_post_items 
				(post_id, item_name, item_id, quantity, price_zeny, price_rmt, payment_methods, refinement, slots, card1, card2, card3, card4) 
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`)
			if err != nil {
				tx.Rollback()
				http.Error(w, "Database preparation failed.", http.StatusInternalServerError)
				return
			}
			defer stmt.Close()

			for i, itemName := range itemNames {
				if strings.TrimSpace(itemName) == "" {
					continue
				}

				quantity, _ := strconv.Atoi(quantities[i])
				priceZeny, _ := strconv.ParseInt(strings.ReplaceAll(pricesZeny[i], ",", ""), 10, 64)
				priceRMT, _ := strconv.ParseInt(strings.ReplaceAll(pricesRMT[i], ",", ""), 10, 64)

				var itemID sql.NullInt64
				if id, err := strconv.ParseInt(itemIDs[i], 10, 64); err == nil && id > 0 {
					itemID = sql.NullInt64{Int64: id, Valid: true}
				}

				paymentMethods := "zeny"
				if i < len(paymentMethodsList) && (paymentMethodsList[i] == "rmt" || paymentMethodsList[i] == "both") {
					paymentMethods = paymentMethodsList[i]
				}

				refinement, _ := strconv.Atoi(refinements[i])
				slots, _ := strconv.Atoi(slotsList[i])

				card1 := sql.NullString{String: cards1[i], Valid: strings.TrimSpace(cards1[i]) != ""}
				card2 := sql.NullString{String: cards2[i], Valid: strings.TrimSpace(cards2[i]) != ""}
				card3 := sql.NullString{String: cards3[i], Valid: strings.TrimSpace(cards3[i]) != ""}
				card4 := sql.NullString{String: cards4[i], Valid: strings.TrimSpace(cards4[i]) != ""}

				if quantity <= 0 || priceZeny < 0 || priceRMT < 0 {
					continue
				}

				_, err := stmt.Exec(postID, itemName, itemID, quantity, priceZeny, priceRMT, paymentMethods, refinement, slots, card1, card2, card3, card4)
				if err != nil {
					tx.Rollback()
					http.Error(w, "Failed to save one of the items.", http.StatusInternalServerError)
					log.Printf("❌ Failed to insert trading post item for post %d: %v", postID, err)
					return
				}
			}
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, "Failed to finalize transaction.", http.StatusInternalServerError)
			return
		}

		log.Printf("👤 Admin edited trading post with ID %d.", postID)
		http.Redirect(w, r, "/admin?msg=Trading+post+updated+successfully.", http.StatusSeeOther)

	} else {

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
				log.Printf("⚠️ Failed to scan trading post item row for edit: %v", err)
				continue
			}
			post.Items = append(post.Items, item)
		}

		tmpl, err := template.ParseFiles("admin_edit_post.html")
		if err != nil {
			http.Error(w, "Could not load edit template", http.StatusInternalServerError)
			log.Printf("❌ Could not load admin_edit_post.html: %v", err)
			return
		}

		data := AdminEditPostPageData{
			Post:           post,
			LastScrapeTime: GetLastScrapeTime(),
		}
		tmpl.Execute(w, data)
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
		log.Printf("❌ Failed to drop trading_post_items table: %v", err)
		msg = "Database+error+while+dropping+trading+post+items+table."
	} else {
		msg = "Successfully+dropped+the+trading_post_items+table."
		log.Printf("👤 Admin dropped the trading_post_items table.")
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
		log.Printf("❌ Failed to drop trading_post_items table (dependency): %v", err)
		msg = "Database+error+while+dropping+dependent+items+table."
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	_, err = db.Exec("DROP TABLE IF EXISTS trading_posts")
	if err != nil {
		log.Printf("❌ Failed to drop trading_posts table: %v", err)
		msg = "Database+error+while+dropping+trading_posts+table."
	} else {
		msg = "Successfully+dropped+the+trading_posts+and+trading_post_items+tables."
		log.Printf("👤 Admin dropped the trading_posts and trading_post_items tables.")
	}

	http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
}
