package server

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

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

	tmpl, ok := templateCache["admin_edit_post.html"]
	if !ok {
		http.Error(w, "Could not load edit template", http.StatusInternalServerError)
		log.Println("[E] [HTTP] admin_edit_post.html template missing from cache")
		return
	}

	data := AdminEditPostPageData{
		Post:           post,
		LastScrapeTime: GetLastScrapeTime(),
	}
	if err := tmpl.Execute(w, data); err != nil {
		log.Printf("[E] [HTTP] Could not execute admin_edit_post.html: %v", err)
	}
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

