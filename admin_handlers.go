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
	"time"
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

	// Fetch all guilds for the emblem update dropdown
	guildRows, err := db.Query("SELECT name, COALESCE(emblem_url, '') FROM guilds ORDER BY name ASC")
	if err != nil {
		log.Printf("⚠️ Could not query for guild list for admin page: %v", err)
	} else {
		defer guildRows.Close()
		for guildRows.Next() {
			var info GuildInfo
			if err := guildRows.Scan(&info.Name, &info.EmblemURL); err != nil {
				log.Printf("⚠️ Failed to scan guild info for admin page: %v", err)
				continue
			}
			stats.AllGuilds = append(stats.AllGuilds, info)
		}
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
	_ = db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&stats.TotalVisitors)
	_ = db.QueryRow("SELECT COUNT(*) FROM visitors WHERE date(last_visit) = date('now', 'localtime')").Scan(&stats.VisitorsToday)

	// Get most visited page
	err = db.QueryRow(`
		SELECT page_path, COUNT(page_path) as Cnt
		FROM page_views
		GROUP BY page_path
		ORDER BY Cnt DESC
		LIMIT 1
	`).Scan(&stats.MostVisitedPage, &stats.MostVisitedPageCount)
	if err != nil {
		if err == sql.ErrNoRows {
			stats.MostVisitedPage = "N/A"
			stats.MostVisitedPageCount = 0
		} else {
			log.Printf("⚠️ Could not query for most visited page: %v", err)
			stats.MostVisitedPage = "Error"
			stats.MostVisitedPageCount = 0
		}
	}

	// --- Pagination for Recent Page Views ---
	const viewsPerPage = 20
	pageStr := r.URL.Query().Get("page")
	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	var totalViews int
	err = db.QueryRow("SELECT COUNT(*) FROM page_views").Scan(&totalViews)
	if err != nil {
		log.Printf("⚠️ Could not query for total page views count: %v", err)
	}

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

	// Get recent page views (now with pagination)
	var recentViews []PageViewEntry
	viewRows, err := db.Query(`
		SELECT page_path, view_timestamp, visitor_hash
		FROM page_views
		ORDER BY view_timestamp DESC
		LIMIT ? OFFSET ?
	`, viewsPerPage, offset)
	if err != nil {
		log.Printf("⚠️ Could not query for recent page views: %v", err)
	} else {
		defer viewRows.Close()
		for viewRows.Next() {
			var entry PageViewEntry
			var timestampStr string
			if err := viewRows.Scan(&entry.Path, &timestampStr, &entry.VisitorHash); err != nil {
				log.Printf("⚠️ Failed to scan page view row: %v", err)
				continue
			}
			// Format the timestamp
			parsedTime, parseErr := time.Parse(time.RFC3339, timestampStr)
			if parseErr == nil {
				entry.Timestamp = parsedTime.Format("15:04:05") // Just show HH:MM:SS
			} else {
				entry.Timestamp = "Invalid Time"
			}
			recentViews = append(recentViews, entry)
		}
	}
	stats.RecentPageViews = recentViews

	// --- Pagination for Trading Posts ---
	const postsPerPage = 10
	tpPageStr := r.URL.Query().Get("tp_page")
	tpPage, err := strconv.Atoi(tpPageStr)
	if err != nil || tpPage < 1 {
		tpPage = 1
	}

	var totalPosts int
	err = db.QueryRow("SELECT COUNT(*) FROM trading_posts").Scan(&totalPosts)
	if err != nil {
		log.Printf("⚠️ Could not query for total trading posts count: %v", err)
	}

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

	// Fetch paginated posts
	postQuery := `
        SELECT id, post_type, character_name, contact_info, created_at, notes 
        FROM trading_posts
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?`

	postRows, err := db.Query(postQuery, postsPerPage, tpOffset)
	if err != nil {
		log.Printf("⚠️ Admin Trading Post query error: %v", err)
	} else {
		defer postRows.Close()

		var posts []TradingPost
		postMap := make(map[int]int)
		var postIDs []interface{}

		for postRows.Next() {
			var post TradingPost
			var createdAtStr string
			if err := postRows.Scan(&post.ID, &post.PostType, &post.CharacterName, &post.ContactInfo, &createdAtStr, &post.Notes); err != nil {
				log.Printf("⚠️ Failed to scan admin trading post row: %v", err)
				continue
			}
			post.CreatedAt = createdAtStr
			post.Items = []TradingPostItem{}

			posts = append(posts, post)
			postMap[post.ID] = len(posts) - 1
			postIDs = append(postIDs, post.ID)
		}

		// Fetch items for the retrieved posts
		if len(postIDs) > 0 {
			placeholders := strings.Repeat("?,", len(postIDs)-1) + "?"
			itemQuery := fmt.Sprintf("SELECT post_id, item_name, quantity, price FROM trading_post_items WHERE post_id IN (%s)", placeholders)

			itemRows, err := db.Query(itemQuery, postIDs...)
			if err != nil {
				log.Printf("⚠️ Admin Trading Post item query error: %v", err)
			} else {
				defer itemRows.Close()
				for itemRows.Next() {
					var item TradingPostItem
					var postID int
					if err := itemRows.Scan(&postID, &item.ItemName, &item.Quantity, &item.Price); err != nil {
						log.Printf("⚠️ Failed to scan admin trading post item row: %v", err)
						continue
					}

					if index, ok := postMap[postID]; ok {
						posts[index].Items = append(posts[index].Items, item)
					}
				}
			}
		}
		stats.RecentTradingPosts = posts
	}

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

// adminUpdateGuildEmblemHandler handles the request to update a guild's emblem URL.
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

	// Execute the update query
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

// adminClearLastActiveHandler resets the last_active timestamp for all characters.
func adminClearLastActiveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	var msg string
	// The 'characters' table schema specifies last_active is NOT NULL.
	// We'll use the zero value for time, formatted according to RFC3339.
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

// adminClearMvpKillsHandler truncates the character_mvp_kills table.
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

// adminDeleteVisitorViewsHandler deletes all records for a specific visitor.
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

	// Use a transaction to ensure both tables are updated correctly.
	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ Failed to begin transaction for deleting visitor: %v", err)
		http.Redirect(w, r, "/admin?msg=Database+error+occurred.", http.StatusSeeOther)
		return
	}

	// Delete from page_views first
	_, err = tx.Exec("DELETE FROM page_views WHERE visitor_hash = ?", visitorHash)
	if err != nil {
		tx.Rollback()
		log.Printf("❌ Failed to delete from page_views for hash %s: %v", visitorHash, err)
		http.Redirect(w, r, "/admin?msg=Database+error+on+page_views.", http.StatusSeeOther)
		return
	}

	// Then delete from the main visitors table
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

// ADDED: adminDeleteTradingPostHandler removes a trading post.
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
		// The schema has ON DELETE CASCADE, so deleting the post will delete its items.
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

// ADDED: adminEditTradingPostHandler allows editing or displaying an edit form for a trading post.
func adminEditTradingPostHandler(w http.ResponseWriter, r *http.Request) {
	postIDStr := r.URL.Query().Get("id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		http.Error(w, "Invalid Post ID", http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodPost {
		// --- HANDLE FORM SUBMISSION ---
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}

		// Begin a transaction
		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "Failed to start database transaction.", http.StatusInternalServerError)
			return
		}

		// 1. Update the main post record
		_, err = tx.Exec(`
			UPDATE trading_posts SET post_type=?, character_name=?, contact_info=?, notes=? WHERE id=?
		`, r.FormValue("post_type"), r.FormValue("character_name"), r.FormValue("contact_info"), r.FormValue("notes"), postID)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to update post.", http.StatusInternalServerError)
			log.Printf("❌ Failed to update trading post %d: %v", postID, err)
			return
		}

		// 2. Delete all existing items for this post
		_, err = tx.Exec("DELETE FROM trading_post_items WHERE post_id = ?", postID)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to clear old items.", http.StatusInternalServerError)
			log.Printf("❌ Failed to delete old items for post %d: %v", postID, err)
			return
		}

		// 3. Loop through submitted items and re-insert them
		itemNames := r.Form["item_name[]"]
		quantities := r.Form["quantity[]"]
		prices := r.Form["price[]"]

		if len(itemNames) > 0 {
			stmt, err := tx.Prepare("INSERT INTO trading_post_items (post_id, item_name, quantity, price) VALUES (?, ?, ?, ?)")
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
				price, _ := strconv.ParseInt(strings.ReplaceAll(prices[i], ",", ""), 10, 64)

				if quantity <= 0 || price < 0 {
					continue
				}

				_, err := stmt.Exec(postID, itemName, quantity, price)
				if err != nil {
					tx.Rollback()
					http.Error(w, "Failed to save one of the items.", http.StatusInternalServerError)
					log.Printf("❌ Failed to insert trading post item for post %d: %v", postID, err)
					return
				}
			}
		}

		// 4. Commit transaction
		if err := tx.Commit(); err != nil {
			http.Error(w, "Failed to finalize transaction.", http.StatusInternalServerError)
			return
		}

		log.Printf("👤 Admin edited trading post with ID %d.", postID)
		http.Redirect(w, r, "/admin?msg=Trading+post+updated+successfully.", http.StatusSeeOther)

	} else {
		// --- SHOW THE EDIT FORM ---
		// 1. Fetch the post
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

		// 2. Fetch its items
		itemRows, err := db.Query("SELECT item_name, quantity, price FROM trading_post_items WHERE post_id = ?", postID)
		if err != nil {
			http.Error(w, "Database item query failed", http.StatusInternalServerError)
			return
		}
		defer itemRows.Close()

		for itemRows.Next() {
			var item TradingPostItem
			if err := itemRows.Scan(&item.ItemName, &item.Quantity, &item.Price); err != nil {
				log.Printf("⚠️ Failed to scan trading post item row for edit: %v", err)
				continue
			}
			post.Items = append(post.Items, item)
		}

		// 3. Render template
		tmpl, err := template.ParseFiles("admin_edit_post.html")
		if err != nil {
			http.Error(w, "Could not load edit template", http.StatusInternalServerError)
			log.Printf("❌ Could not load admin_edit_post.html: %v", err)
			return
		}

		data := AdminEditPostPageData{
			Post:           post,
			LastScrapeTime: getLastScrapeTime(),
		}
		tmpl.Execute(w, data)
	}
}

