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

// getAdminDashboardData centralizes the logic for fetching all dashboard stats.
func getAdminDashboardData(r *http.Request) (AdminDashboardData, error) {
	stats := AdminDashboardData{
		Message: r.URL.Query().Get("msg"),
	}

	// Fetch all guilds for the emblem update dropdown
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
	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ Could not query for most visited page: %v", err)
		stats.MostVisitedPage = "Error"
	}

	// Pagination for Recent Page Views
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

	// Pagination for Trading Posts
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
			// MODIFIED: Select more item fields for display on the admin dashboard
			itemQuery := fmt.Sprintf(`
				SELECT post_id, item_name, quantity, price, currency, refinement, card1 
				FROM trading_post_items WHERE post_id IN (%s)
			`, placeholders)
			itemRows, _ := db.Query(itemQuery, postIDs...)
			if itemRows != nil {
				defer itemRows.Close()
				for itemRows.Next() {
					var item TradingPostItem
					var postID int
					// MODIFIED: Scan the new fields
					if err := itemRows.Scan(&postID, &item.ItemName, &item.Quantity, &item.Price, &item.Currency, &item.Refinement, &item.Card1); err == nil {
						if index, ok := postMap[postID]; ok {
							posts[index].Items = append(posts[index].Items, item)
						}
					}
				}
			}
		}
		stats.RecentTradingPosts = posts
	}

	stats.LastMarketScrape = GetLastScrapeTime()
	stats.LastPlayerCountScrape = GetLastPlayerCountTime()
	stats.LastCharacterScrape = GetLastCharacterScrapeTime()
	stats.LastGuildScrape = GetLastGuildScrapeTime()

	return stats, nil
}

func adminHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := getAdminDashboardData(r)
	if err != nil {
		http.Error(w, "Could not load dashboard data", http.StatusInternalServerError)
		log.Printf("❌ Failed to get admin dashboard data: %v", err)
		return
	}

	// Use template.New().Funcs() to make formatZeny available
	tmpl, err := template.New("admin.html").Funcs(templateFuncs).ParseFiles("admin.html")
	if err != nil {
		http.Error(w, "Could not load admin template", http.StatusInternalServerError)
		log.Printf("❌ Could not load admin.html template: %v", err)
		return
	}

	tmpl.Execute(w, stats)
}

// adminParseTradeHandler processes the trade message from the admin form.
func adminParseTradeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	// Get base dashboard data to render the full page
	stats, err := getAdminDashboardData(r)
	if err != nil {
		http.Error(w, "Could not load dashboard data", http.StatusInternalServerError)
		log.Printf("❌ Failed to get admin dashboard data for trade parse: %v", err)
		return
	}

	// Parse the form to get the message
	if err := r.ParseForm(); err != nil {
		stats.TradeParseError = "Could not parse form input."
	} else {
		message := r.FormValue("message")
		stats.OriginalTradeMessage = message

		if strings.TrimSpace(message) != "" {
			// Call Gemini to process the message
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

	// Render the admin page again, now with the parsing results
	tmpl, err := template.New("admin.html").Funcs(templateFuncs).ParseFiles("admin.html")
	if err != nil {
		http.Error(w, "Could not load admin template", http.StatusInternalServerError)
		log.Printf("❌ Could not load admin.html template: %v", err)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
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
		// MODIFIED: Get all new form fields
		itemNames := r.Form["item_name[]"]
		itemIDs := r.Form["item_id[]"]
		quantities := r.Form["quantity[]"]
		prices := r.Form["price[]"]
		currencies := r.Form["currency[]"]
		paymentMethodsList := r.Form["payment_methods[]"]
		refinements := r.Form["refinement[]"]
		slotsList := r.Form["slots[]"]
		cards1 := r.Form["card1[]"]
		cards2 := r.Form["card2[]"]
		cards3 := r.Form["card3[]"]
		cards4 := r.Form["card4[]"]

		if len(itemNames) > 0 {
			// MODIFIED: Update INSERT statement
			stmt, err := tx.Prepare(`
				INSERT INTO trading_post_items 
				(post_id, item_name, item_id, quantity, price, currency, payment_methods, refinement, slots, card1, card2, card3, card4) 
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

				// MODIFIED: Parse all new fields
				quantity, _ := strconv.Atoi(quantities[i])
				price, _ := strconv.ParseInt(strings.ReplaceAll(prices[i], ",", ""), 10, 64)

				var itemID sql.NullInt64
				if id, err := strconv.ParseInt(itemIDs[i], 10, 64); err == nil && id > 0 {
					itemID = sql.NullInt64{Int64: id, Valid: true}
				}

				currency := "zeny"
				if i < len(currencies) && currencies[i] == "rmt" {
					currency = "rmt"
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

				if quantity <= 0 || price < 0 {
					continue
				}

				// MODIFIED: Execute with all new fields
				_, err := stmt.Exec(postID, itemName, itemID, quantity, price, currency, paymentMethods, refinement, slots, card1, card2, card3, card4)
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
		// MODIFIED: Select all item fields
		itemRows, err := db.Query(`
			SELECT item_name, item_id, quantity, price, currency, payment_methods, refinement, slots, card1, card2, card3, card4 
			FROM trading_post_items WHERE post_id = ?
		`, postID)
		if err != nil {
			http.Error(w, "Database item query failed", http.StatusInternalServerError)
			return
		}
		defer itemRows.Close()

		for itemRows.Next() {
			var item TradingPostItem
			// MODIFIED: Scan all item fields
			if err := itemRows.Scan(
				&item.ItemName, &item.ItemID, &item.Quantity, &item.Price, &item.Currency, &item.PaymentMethods,
				&item.Refinement, &item.Slots, &item.Card1, &item.Card2, &item.Card3, &item.Card4,
			); err != nil {
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
			LastScrapeTime: GetLastScrapeTime(),
		}
		tmpl.Execute(w, data)
	}
}

// adminClearTradingPostItemsHandler drops the trading_post_items table.
func adminClearTradingPostItemsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	var msg string
	// This now drops the entire table. It will be recreated on next app start.
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

// adminClearTradingPostsHandler drops the trading_posts and trading_post_items tables.
func adminClearTradingPostsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	var msg string
	var err error

	// Drop items table first due to foreign key constraint
	_, err = db.Exec("DROP TABLE IF EXISTS trading_post_items")
	if err != nil {
		log.Printf("❌ Failed to drop trading_post_items table (dependency): %v", err)
		msg = "Database+error+while+dropping+dependent+items+table."
		http.Redirect(w, r, "/admin?msg="+msg, http.StatusSeeOther)
		return
	}

	// Then, drop the posts table
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
