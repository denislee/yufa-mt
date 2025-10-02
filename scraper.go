package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
)

// scrapeAndStorePlayerCount fetches the online player count, queries for the unique seller count, and saves them.
func scrapeAndStorePlayerCount() {
	log.Println("📊 Checking player and seller count...")

	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second) // 30-second timeout
	defer cancel()

	var htmlContent string
	err := chromedp.Run(ctx,
		chromedp.Navigate("https://projetoyufa.com/download"),
		chromedp.OuterHTML("html", &htmlContent),
	)

	if err != nil {
		log.Printf("❌ Failed to get player info page: %v", err)
		return
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("❌ Failed to parse player info HTML: %v", err)
		return
	}

	var onlineCount int
	var found bool

	selection := doc.Find("p:contains('Online')")
	if selection.Length() > 0 {
		fullText := selection.First().Text()
		re := regexp.MustCompile(`\d+`)
		numStr := re.FindString(fullText)
		if num, err := strconv.Atoi(numStr); err == nil {
			onlineCount = num
			found = true
		}
	}

	if !found {
		log.Println("⚠️ Could not find player count on the info page after successful load.")
		return
	}

	// Get the number of unique sellers with available items directly from the database.
	var sellerCount int
	err = db.QueryRow("SELECT COUNT(DISTINCT seller_name) FROM items WHERE is_available = 1").Scan(&sellerCount)
	if err != nil {
		log.Printf("⚠️ Could not query for unique seller count: %v", err)
		// Don't return, as we can still store the player count. Default sellerCount to 0.
		sellerCount = 0
	}

	// Check if the latest counts are different from the new ones to avoid duplicate entries.
	var lastPlayerCount int
	var lastSellerCount sql.NullInt64
	err = db.QueryRow("SELECT count, seller_count FROM player_history ORDER BY timestamp DESC LIMIT 1").Scan(&lastPlayerCount, &lastSellerCount)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ Could not query for last player/seller count: %v", err)
		return
	}

	// If both counts are the same as the last record, do nothing.
	if err != sql.ErrNoRows && onlineCount == lastPlayerCount && lastSellerCount.Valid && sellerCount == int(lastSellerCount.Int64) {
		return
	}

	retrievalTime := time.Now().Format(time.RFC3339)
	_, err = db.Exec("INSERT INTO player_history (timestamp, count, seller_count) VALUES (?, ?, ?)", retrievalTime, onlineCount, sellerCount)
	if err != nil {
		log.Printf("❌ Failed to insert new player/seller count: %v", err)
		return
	}

	log.Printf("✅ Player/seller count updated. New values: %d players, %d sellers", onlineCount, sellerCount)
}

// scrapePlayerCharacters scrapes player character data from all pages with enhanced logging and reliability.
func scrapePlayerCharacters() {
	log.Println("🏆 [Characters] Starting player character scrape...")

	var allPlayers []PlayerCharacter
	const maxRetries = 3 // Maximum number of retries for a single page

	allocOpts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
	)
	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), allocOpts...)
	defer cancel()

	// Loop through pages until there's no more data
	for page := 1; ; page++ {
		var htmlContent string
		var pageScrapedSuccessfully bool

		// --- Retry Loop for the current page ---
		for attempt := 1; attempt <= maxRetries; attempt++ {
			taskCtx, cancelCtx := chromedp.NewContext(allocCtx)
			defer cancelCtx()
			taskCtx, cancelTimeout := context.WithTimeout(taskCtx, 60*time.Second)
			defer cancelTimeout()

			url := fmt.Sprintf("https://projetoyufa.com/rankings?page=%d", page)
			log.Printf("🏆 [Characters] Scraping page %d (Attempt %d/%d)...", page, attempt, maxRetries)

			err := chromedp.Run(taskCtx,
				chromedp.Navigate(url),
				chromedp.WaitVisible(`tbody[data-slot="table-body"] tr[data-slot="table-row"]`),
				chromedp.OuterHTML("html", &htmlContent),
			)

			if err == nil {
				// Success: Page was fetched without a connection/retrieval error.
				pageScrapedSuccessfully = true
				break // Exit the retry loop and proceed to parsing.
			}

			// Error: Log the failure and decide whether to retry.
			log.Printf("❌ [Characters] Error on page %d, attempt %d/%d: %v", page, attempt, maxRetries, err)
			if attempt < maxRetries {
				log.Printf("🕒 [Characters] Waiting 30 seconds before retrying...")
				time.Sleep(30 * time.Second)
			}
		}

		if !pageScrapedSuccessfully {
			log.Printf("❌ [Characters] All %d attempts failed for page %d. Aborting characters scrape.", maxRetries, page)
			break // Exit the main page loop
		}

		doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
		if err != nil {
			log.Printf("❌ [Characters] Failed to parse HTML for page %d: %v", page, err)
			break
		}

		rows := doc.Find(`tbody[data-slot="table-body"] tr[data-slot="table-row"]`)
		if rows.Length() == 0 {
			log.Println("✅ [Characters] No more player rows found on the page. Concluding scrape.")
			break // End of pages
		}
		log.Printf("🔎 [Characters] Found %d player rows on page %d. Processing...", rows.Length(), page)

		rows.Each(func(i int, s *goquery.Selection) {
			var player PlayerCharacter
			var parseErr error

			cells := s.Find(`td[data-slot="table-cell"]`)
			if cells.Length() < 4 {
				log.Printf("    -> [Parser] WARN: Row %d has less than 4 cells, skipping.", i)
				return
			}

			rankStr := strings.TrimSpace(cells.Eq(0).Text())
			nameStr := strings.TrimSpace(cells.Eq(1).Text())
			levelStrRaw := cells.Eq(2).Find("span").First().Text()
			expStrRaw := cells.Eq(2).Find("span").Last().Text()
			classStr := cells.Eq(3).Find("span").Last().Text()

			log.Printf("    -> [Parser] Processing Row %d: Rank='%s', Name='%s', LevelInfo='%s', ExpInfo='%s', Class='%s'", i, rankStr, nameStr, levelStrRaw, expStrRaw, classStr)

			player.Name = nameStr
			player.Class = classStr

			player.Rank, parseErr = strconv.Atoi(rankStr)
			if parseErr != nil {
				log.Printf("        -> [Parser] ERROR: Could not parse RANK for '%s' from value '%s'. Skipping row. Error: %v", nameStr, rankStr, parseErr)
				return
			}

			levelStrClean := strings.TrimSpace(strings.TrimPrefix(levelStrRaw, "Nv."))
			levelParts := strings.Split(levelStrClean, "/")
			if len(levelParts) == 2 {
				baseLevelStr := strings.TrimSpace(levelParts[0])
				jobLevelStr := strings.TrimSpace(levelParts[1])
				player.BaseLevel, parseErr = strconv.Atoi(baseLevelStr)
				if parseErr != nil {
					log.Printf("        -> [Parser] ERROR: Could not parse BASE LEVEL for '%s' from value '%s'. Skipping row. Error: %v", nameStr, baseLevelStr, parseErr)
					return
				}
				player.JobLevel, parseErr = strconv.Atoi(jobLevelStr)
				if parseErr != nil {
					log.Printf("        -> [Parser] ERROR: Could not parse JOB LEVEL for '%s' from value '%s'. Skipping row. Error: %v", nameStr, jobLevelStr, parseErr)
					return
				}
			} else {
				log.Printf("        -> [Parser] ERROR: Level string for '%s' has unexpected format: '%s'. Skipping row.", nameStr, levelStrRaw)
				return
			}

			expStr := strings.TrimSuffix(strings.TrimSpace(expStrRaw), "%")
			player.Experience, parseErr = strconv.ParseFloat(expStr, 64)
			if parseErr != nil {
				log.Printf("        -> [Parser] WARN: Could not parse EXPERIENCE for '%s' from value '%s'. Defaulting to 0. Error: %v", nameStr, expStr, parseErr)
				player.Experience = 0.0
			}

			log.Printf("    -> [Parser] SUCCESS: Parsed player %s (Rank: %d, Class: %s, Level: %d/%d)", player.Name, player.Rank, player.Class, player.BaseLevel, player.JobLevel)
			allPlayers = append(allPlayers, player)
		})

		// Wait for a short time after a SUCCESSFUL page scrape to be polite to the server.
		time.Sleep(2 * time.Second)
	}

	if len(allPlayers) == 0 {
		log.Println("⚠️ [Characters] Scrape finished with 0 total players found. Database will not be updated.")
		return
	}

	log.Printf("💾 [DB] Preparing to save %d player character records to the database...", len(allPlayers))

	// Fetch existing player data for comparison.
	log.Println("    -> [DB] Fetching existing player data for activity comparison...")
	existingPlayers := make(map[string]PlayerCharacter)
	rows, err := db.Query("SELECT name, experience, last_active FROM characters")
	if err != nil {
		log.Printf("❌ [DB] Failed to query existing characters for comparison: %v", err)
		// Proceed with the assumption that all are new if the query fails.
	} else {
		defer rows.Close()
		for rows.Next() {
			var p PlayerCharacter
			if err := rows.Scan(&p.Name, &p.Experience, &p.LastActive); err != nil {
				log.Printf("    -> [DB] WARN: Failed to scan existing player row: %v", err)
				continue
			}
			existingPlayers[p.Name] = p
		}
		log.Printf("    -> [DB] Found %d existing player records for comparison.", len(existingPlayers))
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [DB] Failed to begin transaction for characters: %v", err)
		return
	}
	defer tx.Rollback()

	log.Println("    -> [DB] Clearing existing characters table...")
	if _, err := tx.Exec("DELETE FROM characters"); err != nil {
		log.Printf("❌ [DB] Failed to clear characters table: %v", err)
		return
	}

	stmt, err := tx.Prepare(`INSERT INTO characters (rank, name, base_level, job_level, experience, class, last_updated, last_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Printf("❌ [DB] Failed to prepare characters insert statement: %v", err)
		return
	}
	defer stmt.Close()

	updateTime := time.Now().Format(time.RFC3339)
	for _, p := range allPlayers {
		lastActiveTime := updateTime // Default to now for new or changed players.
		if oldPlayer, exists := existingPlayers[p.Name]; exists {
			// If the player existed and their experience is unchanged, keep the old 'last_active' time.
			if oldPlayer.Experience == p.Experience {
				lastActiveTime = oldPlayer.LastActive
			}
		}

		if _, err := stmt.Exec(p.Rank, p.Name, p.BaseLevel, p.JobLevel, p.Experience, p.Class, updateTime, lastActiveTime); err != nil {
			log.Printf("    -> [DB] WARN: Failed to insert character for player %s: %v", p.Name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ [DB] Failed to commit characters transaction: %v", err)
		return
	}

	log.Printf("✅ [Characters] Scrape and update complete. Saved %d player records.", len(allPlayers))
}

// startBackgroundJobs starts all recurring background tasks.
func startBackgroundJobs() {
	// --- Market Scraper ---
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		scrapeData() // Run once immediately on start
		for {
			log.Printf("🕒 Waiting for the next 5-minute market scrape schedule...")
			<-ticker.C
			scrapeData()
		}
	}()

	// --- Player Count Scraper ---
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		scrapeAndStorePlayerCount() // Run once immediately on start
		for {
			log.Printf("🕒 Waiting for the next 1-minute player count schedule...")
			<-ticker.C
			scrapeAndStorePlayerCount()
		}
	}()

	// --- Player Characters Scraper ---
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		scrapePlayerCharacters() // Run once immediately on start
		for {
			log.Printf("🕒 Waiting for the next 30-minute player character schedule...")
			<-ticker.C
			scrapePlayerCharacters()
		}
	}()
}

// scrapeData performs a single scrape of the market data.
func scrapeData() {
	log.Println("🚀 Starting scrape...")
	// Compile regexes once for efficiency.
	reRefineMid := regexp.MustCompile(`\s(\+\d+)`)
	reRefineStart := regexp.MustCompile(`^(\+\d+)\s`)

	allocOpts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
	)
	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), allocOpts...)
	defer cancel()
	taskCtx, cancel := chromedp.NewContext(allocCtx)
	defer cancel()
	taskCtx, cancel = context.WithTimeout(taskCtx, 30*time.Second)
	defer cancel()

	var htmlContent string
	err := chromedp.Run(taskCtx,
		chromedp.Navigate("https://projetoyufa.com/market"),
		chromedp.WaitVisible(`div[data-slot="card-header"]`),
		chromedp.OuterHTML("html", &htmlContent),
	)
	if err != nil {
		log.Printf("❌ Failed to run chromedp tasks: %v", err)
		return
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("❌ Failed to parse HTML: %v", err)
		return
	}

	retrievalTime := time.Now().Format(time.RFC3339)
	scrapedItemsByName := make(map[string][]Item)

	doc.Find(`div[data-slot="card"]`).Each(func(i int, s *goquery.Selection) {
		shopName := strings.TrimSpace(s.Find(`div[data-slot="card-title"]`).Text())
		sellerName := strings.TrimSpace(s.Find("svg.lucide-user").Next().Text())
		mapName := strings.TrimSpace(s.Find("svg.lucide-map-pin").Next().Text())
		mapCoordinates := strings.TrimSpace(s.Find("svg.lucide-copy").Next().Text())

		s.Find(`div[data-slot="card-content"] .flex.items-center.space-x-2`).Each(func(j int, itemSelection *goquery.Selection) {
			itemName := strings.TrimSpace(itemSelection.Find("p.truncate").Text())

			// Standardize item names by moving refinement level (e.g., +7) to the end.
			if match := reRefineMid.FindStringSubmatch(itemName); len(match) > 1 && !strings.HasSuffix(itemName, match[0]) {
				cleanedName := strings.Replace(itemName, match[0], "", 1)
				cleanedName = strings.Join(strings.Fields(cleanedName), " ")
				itemName = cleanedName + match[0]
			} else {
				if match := reRefineStart.FindStringSubmatch(itemName); len(match) > 1 {
					cleanedName := strings.Replace(itemName, match[0], "", 1)
					cleanedName = strings.Join(strings.Fields(cleanedName), " ")
					itemName = cleanedName + " " + match[1] // Re-add space before the refinement
				}
			}

			// Find and append card names.
			var cardNames []string
			itemSelection.Find("div.mt-1.flex.flex-wrap.gap-1 span[data-slot='badge']").Each(func(k int, cardSelection *goquery.Selection) {
				cardName := strings.TrimSpace(strings.TrimSuffix(cardSelection.Text(), " Card"))
				if cardName != "" {
					cardNames = append(cardNames, cardName)
				}
			})

			if len(cardNames) > 0 {
				wrapped := make([]string, len(cardNames))
				for i, c := range cardNames {
					wrapped[i] = fmt.Sprintf(" [%s]", c)
				}
				itemName = fmt.Sprintf("%s%s", itemName, strings.Join(wrapped, ""))
			}

			quantityStr := strings.TrimSuffix(strings.TrimSpace(itemSelection.Find("span.text-xs.text-muted-foreground").Text()), "x")
			priceStr := strings.TrimSpace(itemSelection.Find("span.text-xs.font-medium.text-green-600").Text())
			// Use a more specific selector for the ID to avoid picking up card badges.
			idStr := strings.TrimPrefix(strings.TrimSpace(itemSelection.Find("div.flex.items-center.gap-1 span[data-slot='badge']").First().Text()), "ID: ")

			if itemName == "" || priceStr == "" || shopName == "" {
				return
			}
			quantity, _ := strconv.Atoi(quantityStr)
			if quantity == 0 {
				quantity = 1
			}
			itemID, _ := strconv.Atoi(idStr)

			item := Item{
				Name:           itemName,
				ItemID:         itemID,
				Quantity:       quantity,
				Price:          priceStr,
				StoreName:      shopName,
				SellerName:     sellerName,
				MapName:        mapName,
				MapCoordinates: mapCoordinates,
			}
			scrapedItemsByName[itemName] = append(scrapedItemsByName[itemName], item)
		})
	})

	log.Printf("🔎 Scrape parsed. Found %d unique item names.", len(scrapedItemsByName))
	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("INSERT OR IGNORE INTO scrape_history (timestamp) VALUES (?)", retrievalTime)
	if err != nil {
		log.Printf("❌ Failed to log scrape history: %v", err)
		return
	}

	// --- FIX IS HERE ---
	// Changed '=' to ':=' to correctly declare the 'rows' variable.
	rows, err := tx.Query("SELECT DISTINCT name_of_the_item FROM items WHERE is_available = 1")
	if err != nil {
		log.Printf("❌ Could not get list of available items: %v", err)
		return
	}
	dbAvailableNames := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		dbAvailableNames[name] = true
	}
	rows.Close()

	itemsUpdated := 0
	itemsUnchanged := 0
	itemsAdded := 0

	for itemName, currentScrapedItems := range scrapedItemsByName {
		var lastAvailableItems []Item
		rows, err := tx.Query("SELECT name_of_the_item, item_id, quantity, price, store_name, seller_name, map_name, map_coordinates FROM items WHERE name_of_the_item = ? AND is_available = 1", itemName)
		if err != nil {
			log.Printf("⚠️ Failed to query for existing item %s: %v", itemName, err)
			continue
		}
		for rows.Next() {
			var item Item
			err := rows.Scan(&item.Name, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &item.MapName, &item.MapCoordinates)
			if err != nil {
				log.Printf("⚠️ Failed to scan existing item: %v", err)
				continue
			}
			lastAvailableItems = append(lastAvailableItems, item)
		}
		rows.Close()

		if areItemSetsIdentical(currentScrapedItems, lastAvailableItems) {
			itemsUnchanged++
			continue
		}

		if _, err := tx.Exec("UPDATE items SET is_available = 0 WHERE name_of_the_item = ?", itemName); err != nil {
			log.Printf("❌ Failed to mark old %s as unavailable: %v", itemName, err)
			continue
		}

		insertSQL := `INSERT INTO items(name_of_the_item, item_id, quantity, price, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates, is_available) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			log.Printf("⚠️ Could not prepare insert for %s: %v", itemName, err)
			continue
		}
		for _, item := range currentScrapedItems {
			if _, err := stmt.Exec(item.Name, item.ItemID, item.Quantity, item.Price, item.StoreName, item.SellerName, retrievalTime, item.MapName, item.MapCoordinates); err != nil {
				log.Printf("⚠️ Could not execute insert for %s: %v", item.Name, err)
			}
		}
		stmt.Close()

		if len(lastAvailableItems) == 0 {
			itemsAdded++
			if len(currentScrapedItems) > 0 {
				firstItem := currentScrapedItems[0]
				details, _ := json.Marshal(map[string]interface{}{"price": firstItem.Price, "quantity": firstItem.Quantity, "seller": firstItem.SellerName})
				_, err := tx.Exec(`INSERT INTO market_events (event_timestamp, event_type, item_name, item_id, details) VALUES (?, 'ADDED', ?, ?, ?)`, retrievalTime, itemName, firstItem.ItemID, string(details))
				if err != nil {
					log.Printf("❌ Failed to log ADDED event for %s: %v", itemName, err)
				}

				go scrapeAndCacheItemIfNotExists(firstItem.ItemID, itemName)
			}

			var historicalLowestPrice sql.NullInt64
			err := tx.QueryRow(`SELECT MIN(CAST(REPLACE(price, ',', '') AS INTEGER)) FROM items WHERE name_of_the_item = ?`, itemName).Scan(&historicalLowestPrice)
			if err != nil && err != sql.ErrNoRows {
				log.Printf("⚠️ Could not get historical lowest price for %s: %v", itemName, err)
			}

			var lowestPriceListingInBatch Item
			lowestPriceInBatch := -1
			for _, item := range currentScrapedItems {
				priceStr := strings.ReplaceAll(item.Price, ",", "")
				currentPrice, convErr := strconv.Atoi(priceStr)
				if convErr != nil {
					continue
				}
				if lowestPriceInBatch == -1 || currentPrice < lowestPriceInBatch {
					lowestPriceInBatch = currentPrice
					lowestPriceListingInBatch = item
				}
			}

			if lowestPriceInBatch != -1 && (!historicalLowestPrice.Valid || int64(lowestPriceInBatch) < historicalLowestPrice.Int64) {
				details, _ := json.Marshal(map[string]interface{}{"price": lowestPriceListingInBatch.Price, "quantity": lowestPriceListingInBatch.Quantity, "seller": lowestPriceListingInBatch.SellerName})
				_, err := tx.Exec(`INSERT INTO market_events (event_timestamp, event_type, item_name, item_id, details) VALUES (?, 'NEW_LOW', ?, ?, ?)`, retrievalTime, itemName, lowestPriceListingInBatch.ItemID, string(details))
				if err != nil {
					log.Printf("❌ Failed to log NEW_LOW event for %s: %v", itemName, err)
				}
			}
		} else {
			itemsUpdated++
		}
	}

	itemsRemoved := 0
	for name := range dbAvailableNames {
		if _, foundInScrape := scrapedItemsByName[name]; !foundInScrape {
			// --- FIX START ---
			var itemID int // Declare itemID
			err := tx.QueryRow("SELECT item_id FROM items WHERE name_of_the_item = ? AND item_id > 0 LIMIT 1", name).Scan(&itemID)
			if err != nil {
				// If no ID is found, log it but default to 0 so the event can still be created.
				log.Printf("⚠️ Could not find item_id for removed item '%s', logging event with item_id 0: %v", name, err)
				itemID = 0
			}
			// --- FIX END ---
			_, err = tx.Exec(`INSERT INTO market_events (event_timestamp, event_type, item_name, item_id, details) VALUES (?, 'REMOVED', ?, ?, '{}')`, retrievalTime, name, itemID)
			if err != nil {
				log.Printf("❌ Failed to log REMOVED event for %s: %v", name, err)
			}
			if _, err := tx.Exec("UPDATE items SET is_available = 0 WHERE name_of_the_item = ?", name); err != nil {
				log.Printf("❌ Failed to mark disappeared item %s as unavailable: %v", name, err)
			} else {
				itemsRemoved++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ Failed to commit transaction: %v", err)
		return
	}
	log.Printf("✅ Scrape complete. Unchanged: %d groups. Updated: %d groups. Newly Added: %d groups. Removed: %d groups.", itemsUnchanged, itemsUpdated, itemsAdded, itemsRemoved)
}

// areItemSetsIdentical compares two slices of Items to see if they are identical.
func areItemSetsIdentical(setA, setB []Item) bool {
	if len(setA) != len(setB) {
		return false
	}
	makeComparable := func(items []Item) []comparableItem {
		comp := make([]comparableItem, len(items))
		for i, item := range items {
			comp[i] = comparableItem{
				Name:           item.Name,
				ItemID:         item.ID,
				Quantity:       item.Quantity,
				Price:          item.Price,
				StoreName:      item.StoreName,
				SellerName:     item.SellerName,
				MapName:        item.MapName,
				MapCoordinates: item.MapCoordinates,
			}
		}
		return comp
	}
	compA := makeComparable(setA)
	compB := makeComparable(setB)
	counts := make(map[comparableItem]int)
	for _, item := range compA {
		counts[item]++
	}
	for _, item := range compB {
		if counts[item] == 0 {
			return false
		}
		counts[item]--
	}
	return true
}

