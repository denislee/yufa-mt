package main

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// scrapeRMSItemDetails scrapes detailed item information from ratemyserver.net and ragnarokdatabase.com.
func scrapeRMSItemDetails(itemID int) (*RMSItem, error) {
	log.Printf("ℹ️ [RMS Scraper] Starting detailed scrape for Item ID: %d", itemID)

	// --- Part 1: Scrape from RateMyServer.net (Primary Source) ---
	rmsURL := fmt.Sprintf("https://ratemyserver.net/item_db.php?item_id=%d", itemID)
	log.Printf("   -> [RMS] Fetching primary data from: %s", rmsURL)
	client := http.Client{Timeout: 10 * time.Second}
	res, err := client.Get(rmsURL)
	if err != nil {
		log.Printf("   -> ❌ [RMS] FAILED to get URL. Error: %v", err)
		return nil, fmt.Errorf("failed to get URL from RMS: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Printf("   -> ❌ [RMS] FAILED with non-200 status code: %d %s", res.StatusCode, res.Status)
		// A 404 from RMS is a critical failure, as it's the primary data source.
		return nil, fmt.Errorf("RMS status code error: %d %s", res.StatusCode, res.Status)
	}
	log.Printf("   -> ✅ [RMS] Successfully received response.")

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Printf("   -> ❌ [RMS] FAILED to parse HTML. Error: %v", err)
		return nil, fmt.Errorf("failed to parse RMS HTML: %w", err)
	}
	log.Printf("   -> ✅ [RMS] HTML parsed successfully.")

	item := &RMSItem{
		ID:       itemID,
		ImageURL: fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", itemID),
	}

	// Get Item Name (from RMS)
	item.Name = strings.TrimSpace(doc.Find("div.main_block b").First().Text())
	if item.Name == "" {
		log.Printf("   -> ⚠️ [RMS] Could not find item name.")
	} else {
		log.Printf("   -> [RMS] Found primary name: '%s'", item.Name)
	}

	// Get Item Properties from the info grid (from RMS)
	doc.Find(".info_grid_item").Each(func(i int, s *goquery.Selection) {
		label := strings.TrimSpace(s.Text())
		var value string
		if next := s.Next(); next.Length() > 0 {
			value = strings.TrimSpace(next.Text())
		}

		switch label {
		case "Type":
			item.Type = value
		case "Class":
			item.Class = value
		case "Buy":
			item.Buy = value
		case "Sell":
			item.Sell = value
		case "Weight":
			item.Weight = value
		case "Pre/Suffix":
			item.Prefix = value
		}
	})

	// Get Description (from RMS)
	item.Description = strings.TrimSpace(doc.Find("th:contains('Description')").Next().Find("div.longtext").Text())
	if item.Description == "" { // Fallback for items without a longtext div
		item.Description = strings.TrimSpace(doc.Find("th:contains('Description')").Next().Text())
	}

	// Get Item Script (from RMS)
	item.Script = strings.TrimSpace(doc.Find("th:contains('Item Script')").Next().Find("div.db_script_txt").Text())

	// Get Dropped By (from RMS)
	reDrop := regexp.MustCompile(`(.+)\s+\(([\d.]+%)\)`)
	doc.Find("th:contains('Dropped By')").Next().Find("a.nbu_m").Each(func(i int, s *goquery.Selection) {
		text := s.Text()
		matches := reDrop.FindStringSubmatch(text)
		if len(matches) == 3 {
			item.DroppedBy = append(item.DroppedBy, RMSDrop{
				Monster: strings.TrimSpace(matches[1]),
				Rate:    matches[2],
			})
		}
	})

	// Get Obtainable From (from RMS)
	doc.Find("th:contains('Obtainable From')").Next().Find("a").Each(func(i int, s *goquery.Selection) {
		item.ObtainableFrom = append(item.ObtainableFrom, strings.TrimSpace(s.Text()))
	})

	// --- Part 2: Scrape from RagnarokDatabase.com (Secondary Source for name_pt) ---
	// This part is best-effort. If it fails, we still have the primary RMS data.
	rdbURL := fmt.Sprintf("https://ragnarokdatabase.com/item/%d", itemID)
	log.Printf("   -> [RDB] Fetching Portuguese name from: %s", rdbURL)
	rdbRes, err := client.Get(rdbURL)
	if err != nil {
		log.Printf("   -> ⚠️ [RDB] Could not fetch from RagnarokDatabase for item %d: %v", itemID, err)
		return item, nil // Return the successfully scraped RMS data without the PT name
	}
	defer rdbRes.Body.Close()

	if rdbRes.StatusCode == 200 {
		log.Printf("   -> ✅ [RDB] Successfully received response.")
		body, readErr := io.ReadAll(rdbRes.Body)
		if readErr != nil {
			log.Printf("   -> ⚠️ [RDB] Could not read body from RagnarokDatabase for item %d: %v", itemID, readErr)
		} else {
			reNamePT := regexp.MustCompile(`<h1 class="item-title-db">([^<]+)</h1>`)
			matches := reNamePT.FindStringSubmatch(string(body))
			if len(matches) > 1 {
				item.NamePT = strings.TrimSpace(matches[1])
				log.Printf("   -> ✅ [RDB] Found Portuguese name: '%s'", item.NamePT)
			} else {
				log.Printf("   -> ⚠️ [RDB] Could not find Portuguese name with regex on page.")
			}
		}
	} else {
		log.Printf("   -> ⚠️ [RDB] Received non-200 status (%d) from RagnarokDatabase for item %d", rdbRes.StatusCode, itemID)
	}

	log.Printf("✅ [RMS Scraper] Finished detailed scrape for Item ID: %d", itemID)
	return item, nil
}

// getItemDetailsFromCache tries to fetch item details from the local DB cache.
func getItemDetailsFromCache(itemID int) (*RMSItem, error) {
	row := db.QueryRow(`
		SELECT name, name_pt, image_url, item_type, item_class, buy, sell, weight, prefix, description, script, dropped_by_json, obtainable_from_json
		FROM rms_item_cache WHERE item_id = ?`, itemID)

	var item RMSItem
	item.ID = itemID
	var droppedByJSON, obtainableFromJSON string

	err := row.Scan(
		&item.Name, &item.NamePT, &item.ImageURL, &item.Type, &item.Class, &item.Buy, &item.Sell,
		&item.Weight, &item.Prefix, &item.Description, &item.Script,
		&droppedByJSON, &obtainableFromJSON,
	)
	if err != nil {
		// This is a normal cache miss, not necessarily an application error.
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("item %d not found in cache", itemID)
		}
		// Any other error is a real problem.
		return nil, fmt.Errorf("error querying cache for item %d: %w", itemID, err)
	}

	// Deserialize JSON fields back into slices
	if err := json.Unmarshal([]byte(droppedByJSON), &item.DroppedBy); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DroppedBy from cache for item %d: %w", itemID, err)
	}
	if err := json.Unmarshal([]byte(obtainableFromJSON), &item.ObtainableFrom); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ObtainableFrom from cache for item %d: %w", itemID, err)
	}

	return &item, nil
}

// saveItemDetailsToCache saves a successfully scraped item to the local DB cache.
func saveItemDetailsToCache(item *RMSItem) error {
	droppedByJSON, err := json.Marshal(item.DroppedBy)
	if err != nil {
		return fmt.Errorf("failed to marshal DroppedBy for caching item %d: %w", item.ID, err)
	}
	obtainableFromJSON, err := json.Marshal(item.ObtainableFrom)
	if err != nil {
		return fmt.Errorf("failed to marshal ObtainableFrom for caching item %d: %w", item.ID, err)
	}

	// Use INSERT OR REPLACE to either create a new entry or update an existing one.
	_, err = db.Exec(`
		INSERT OR REPLACE INTO rms_item_cache
		(item_id, name, name_pt, image_url, item_type, item_class, buy, sell, weight, prefix, description, script, dropped_by_json, obtainable_from_json, last_checked)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.ID, item.Name, item.NamePT, item.ImageURL, item.Type, item.Class, item.Buy, item.Sell,
		item.Weight, item.Prefix, item.Description, item.Script,
		string(droppedByJSON), string(obtainableFromJSON), time.Now().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("failed to execute insert/replace for item %d in cache: %w", item.ID, err)
	}
	return nil
}

// scrapeAndCacheItemIfNotExists checks the cache first, and only scrapes if the item is missing.
func scrapeAndCacheItemIfNotExists(itemID int, itemName string) {
	if itemID <= 0 {
		return // Don't process invalid item IDs
	}

	// First, check if the item already exists in the cache.
	_, err := getItemDetailsFromCache(itemID)
	if err == nil {
		// Cache hit, nothing to do.
		return
	}

	// Cache miss, proceed to scrape.
	log.Printf("ℹ️ Caching details for new/missing item: %s (ID: %d)", itemName, itemID)
	scrapedItem, scrapeErr := scrapeRMSItemDetails(itemID)
	if scrapeErr != nil {
		log.Printf("⚠️ Failed to scrape RateMyServer for item ID %d (%s): %v", itemID, itemName, scrapeErr)
		return // Stop if scraping fails
	}

	// Save the newly scraped data to the cache.
	if saveErr := saveItemDetailsToCache(scrapedItem); saveErr != nil {
		log.Printf("⚠️ Failed to save item ID %d (%s) to cache: %v", itemID, itemName, saveErr)
	} else {
		log.Printf("✅ Successfully cached details for item ID %d (%s).", itemID, itemName)
	}
}

// populateMissingCachesOnStartup runs on startup to find and cache any items in the DB
// that do not yet have an entry in the rms_item_cache table.
func populateMissingCachesOnStartup() {
	log.Println("🛠️ Starting background task: Verifying RMS item cache...")

	// 1. Get all unique item IDs from the main items table.
	rows, err := db.Query("SELECT DISTINCT item_id, name_of_the_item FROM items WHERE item_id > 0")
	if err != nil {
		log.Printf("❌ [Cache Verification] Failed to query for all items: %v", err)
		return
	}
	defer rows.Close()

	type dbItem struct {
		ID   int
		Name string
	}
	var allDBItems []dbItem
	for rows.Next() {
		var item dbItem
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			log.Printf("⚠️ [Cache Verification] Failed to scan item: %v", err)
			continue
		}
		allDBItems = append(allDBItems, item)
	}

	// 2. Get all item IDs that are already in the cache.
	cacheRows, err := db.Query("SELECT item_id FROM rms_item_cache")
	if err != nil {
		log.Printf("❌ [Cache Verification] Failed to query for cached items: %v", err)
		return
	}
	defer cacheRows.Close()

	cachedIDs := make(map[int]bool)
	for cacheRows.Next() {
		var id int
		if err := cacheRows.Scan(&id); err != nil {
			continue
		}
		cachedIDs[id] = true
	}

	// 3. Determine which items are missing from the cache.
	var itemsToCache []dbItem
	for _, item := range allDBItems {
		if !cachedIDs[item.ID] {
			itemsToCache = append(itemsToCache, item)
		}
	}

	if len(itemsToCache) == 0 {
		log.Println("✅ [Cache Verification] All items are already cached. No work to do.")
		return
	}

	log.Printf("ℹ️ [Cache Verification] Found %d item(s) missing from the RMS cache. Populating now...", len(itemsToCache))

	// 4. Scrape and cache the missing items, with a delay to be polite.
	for i, item := range itemsToCache {
		log.Printf("    -> Caching %d/%d: %s (ID: %d)", i+1, len(itemsToCache), item.Name, item.ID)
		scrapeAndCacheItemIfNotExists(item.ID, item.Name)
		// Be a good citizen and don't spam the server.
		time.Sleep(1 * time.Second)
	}

	log.Println("✅ [Cache Verification] Finished populating missing cache entries.")
}

// scrapeRMSItemSearch performs a live search on ratemyserver.net for an item by name by parsing the HTML results page.
func scrapeRMSItemSearch(query string) ([]ItemSearchResult, error) {
	// 1. Construct the URL for the standard HTML search results page

	rmsURL := fmt.Sprintf("https://ratemyserver.net/index.php?iname=%s&page=item_db&quick=1&isearch=Search", url.QueryEscape(query))
	log.Printf("➡️ [RMS HTML Search] Performing live search for: '%s'", query)
	log.Printf("   URL: %s", rmsURL)

	client := http.Client{Timeout: 10 * time.Second}
	res, err := client.Get(rmsURL)
	if err != nil {
		log.Printf("❌ [RMS HTML Search] Failed to get URL: %v", err)
		return nil, fmt.Errorf("failed to get search URL: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("⚠️ [RMS HTML Search] Received non-200 status code: %d %s", res.StatusCode, res.Status)
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// 2. Parse the HTML document using goquery
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Printf("❌ [RMS HTML Search] Failed to parse search HTML: %v", err)
		return nil, fmt.Errorf("failed to parse search HTML: %w", err)
	}

	var results []ItemSearchResult
	idRegex := regexp.MustCompile(`Item ID# (\d+)`)

	// 3. Find each result container and extract the data
	log.Printf("   [RMS HTML Search] Searching for result containers in HTML...")
	doc.Find("div[style*='display: flex']").Each(func(i int, s *goquery.Selection) {
		var result ItemSearchResult
		var found bool

		// 4. Extract the full text content of the div
		fullText := s.Text()
		log.Printf("   [RMS HTML Search] Processing block %d: Raw text -> \"%s\"", i+1, strings.TrimSpace(fullText))

		matches := idRegex.FindStringSubmatch(fullText)

		// 5. Use the regex to find the Item ID
		if len(matches) > 1 {
			id, _ := strconv.Atoi(matches[1])
			result.ID = id
			found = true

			// 6. Extract the item name from the first <b> tag
			result.Name = strings.TrimSpace(s.Find("b").First().Text())

			// 7. Construct the image URL
			result.ImageURL = fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", id)
			log.Printf("   [RMS HTML Search]   -> Extracted: ID=%d, Name='%s'", result.ID, result.Name)
		} else {
			log.Printf("   [RMS HTML Search]   -> No Item ID found in block %d.", i+1)
		}

		// 8. Add the successfully parsed item to our results
		if found && result.ID > 0 && result.Name != "" {
			results = append(results, result)
		}
	})

	if len(results) == 0 {
		log.Printf("   [RMS HTML Search] No valid items were parsed from the page.")
	}

	log.Printf("✅ [RMS HTML Search] Found %d item(s) for query: '%s'", len(results), query)
	return results, nil
}

// scrapeRagnarokDatabaseSearch performs a search on ragnarokdatabase.com to find potential item IDs when a local search fails.
// It uses regex to parse the HTML response, and as a side effect, it updates the 'name_pt' column in the cache for any matching items found.
func scrapeRagnarokDatabaseSearch(query string) ([]int, error) {
	// 1. Construct the URL
	searchURL := fmt.Sprintf("https://ragnarokdatabase.com/search/all/%s", query)
	log.Printf("➡️ [Fallback Search] Performing search on RagnarokDatabase for: '%s'", query)
	log.Printf("   URL: %s", searchURL)

	// 2. Make the HTTP request
	client := http.Client{Timeout: 10 * time.Second}
	res, err := client.Get(searchURL)
	if err != nil {
		log.Printf("❌ [Fallback Search] Failed to get URL: %v", err)
		return nil, fmt.Errorf("failed to get search URL: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("⚠️ [Fallback Search] Received non-200 status code: %d %s", res.StatusCode, res.Status)
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// 3. Read the body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("❌ [Fallback Search] Failed to read response body: %v", err)
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 4. Use Regex to find all item IDs and names from links
	// Example HTML: <a class="font-bold" href=".../item/501/red-potion">Red Potion</a>
	idNameRegex := regexp.MustCompile(`href="https://ragnarokdatabase.com/item/(\d+)/[^"]+">([^<]+)</a>`)
	matches := idNameRegex.FindAllStringSubmatch(string(body), -1)

	if len(matches) == 0 {
		log.Printf("   [Fallback Search] No item IDs found on the page for query '%s'.", query)
		return nil, nil // No error, just no results
	}

	var itemIDs []int
	seenIDs := make(map[int]bool) // To handle duplicates

	// Prepare a statement for updating the cache. This is more efficient than preparing it inside the loop.
	updateStmt, err := db.Prepare("UPDATE rms_item_cache SET name_pt = ? WHERE item_id = ?")
	if err != nil {
		log.Printf("⚠️ [Fallback Search] Could not prepare update statement for name_pt, will skip updating: %v", err)
		// This is not a fatal error; we can still return the found IDs.
	} else {
		defer updateStmt.Close()
	}

	// 5. Extract IDs and names, update the cache, and collect IDs to return
	for _, match := range matches {
		if len(match) > 2 {
			id, err := strconv.Atoi(match[1])
			if err != nil {
				continue // Skip if ID is not a valid number
			}

			// Add ID to the list to be returned, ensuring no duplicates
			if !seenIDs[id] {
				itemIDs = append(itemIDs, id)
				seenIDs[id] = true
			}

			// If the statement was prepared successfully, try to update the cache with the Portuguese name.
			if updateStmt != nil {
				namePT := strings.TrimSpace(match[2])
				if namePT != "" {
					res, updateErr := updateStmt.Exec(namePT, id)
					if updateErr == nil {
						rowsAffected, _ := res.RowsAffected()
						if rowsAffected > 0 {
							log.Printf("   [Fallback Search] Updated Portuguese name for item ID %d to '%s'", id, namePT)
						}
					} else {
						log.Printf("⚠️ [Fallback Search] Failed to execute update for item ID %d: %v", id, updateErr)
					}
				}
			}
		}
	}

	log.Printf("✅ [Fallback Search] Found %d unique item(s) for query: '%s'", len(itemIDs), query)
	return itemIDs, nil
}

// scrapeRODatabaseSearch performs a search on rodatabase.com to find potential item IDs.
// It uses regex to parse the HTML response from the search results page.
func scrapeRODatabaseSearch(query string, slots int) ([]int, error) {
	// --- ADDED: Remove slot information [N] from the query string ---
	// This prevents searches like "Jur [3]" from failing, as the site's
	// search field expects just the name ("Jur") and uses a separate
	// field for the slot count.
	reSlots := regexp.MustCompile(`\s*\[\d+\]\s*`)
	searchQuery := reSlots.ReplaceAllString(query, " ") // Replace with space to avoid merging words
	searchQuery = strings.TrimSpace(searchQuery)        // Clean up leading/trailing spaces
	// --- END ADDITION ---

	// 1. Construct the URL by escaping the search query.
	v := url.Values{}
	v.Set("name", searchQuery) // Use the cleaned searchQuery
	if slots > 0 {
		v.Set("slots_operator", "eq")
		v.Set("slots", strconv.Itoa(slots))
	}
	searchURL := fmt.Sprintf("https://rodatabase.com/pt-BR/item/search?%s", v.Encode())

	// Modified log to show both original and cleaned query
	log.Printf("➡️ [RODatabase Search] Performing search for: '%s' (Original: '%s', Slots: %d)", searchQuery, query, slots)
	log.Printf("	URL: %s", searchURL)

	// 2. Make the HTTP request with a timeout and custom transport.
	// Create a custom transport that ignores invalid SSL certificates.
	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := http.Client{
		Timeout:   10 * time.Second,
		Transport: customTransport, // Use the custom transport here
	}
	res, err := client.Get(searchURL)
	if err != nil {
		log.Printf("❌ [RODatabase Search] Failed to get URL: %v", err)
		return nil, fmt.Errorf("failed to get search URL: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("⚠️ [RODatabase Search] Received non-200 status code: %d %s", res.StatusCode, res.Status)
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// 3. Read the response body.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("❌ [RODatabase Search] Failed to read response body: %v", err)
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 4. Use Regex to find all item IDs from the links in the results table.
	idRegex := regexp.MustCompile(`href="[^"]*/pt-BR/item/id/(\d+)"`)
	matches := idRegex.FindAllStringSubmatch(string(body), -1)

	if len(matches) == 0 {
		log.Printf("	[RODatabase Search] No item IDs found on the page for query '%s'.", query)
		return nil, nil // No error, just no results found.
	}

	var itemIDs []int
	seenIDs := make(map[int]bool) // Use a map to handle and prevent duplicate IDs.

	// 5. Extract IDs from the regex matches.
	for _, match := range matches {
		if len(match) > 1 {
			id, convErr := strconv.Atoi(match[1])
			if convErr != nil {
				continue // Skip if the captured ID is not a valid number.
			}
			if !seenIDs[id] {
				itemIDs = append(itemIDs, id)
				seenIDs[id] = true
			}
		}
	}

	log.Printf("✅ [RODatabase Search] Found %d unique item(s) for query: '%s'", len(itemIDs), query)
	return itemIDs, nil
}

// runFullRMSCacheJob is a slow, rate-limited job to refresh the entire RMS cache.
// It runs in two parts:
//  1. Refreshes all "stale" items (older than 7 days) that are in the market 'items' table.
//  2. Proactively discovers new items by iterating from a low ID (501) up to a limit,
//     skipping any items that are already present in the cache.
func runFullRMSCacheJob() {
	log.Println("🛠️ [RMS Refresh] Starting full RMS cache refresh job...")

	// --- Constants for the job ---
	const staleDuration = 7 * 24 * time.Hour // 7 days
	const scrapeDelay = 5 * time.Second      // 5 seconds per item to be polite
	const startDiscoveryID = 501             // Start from Red Potion (ID 501)
	const maxPreRenewalItemID = 20000        // Stop searching after this ID
	const maxConsecutiveFailures = 1000      // Stop if 100 empty IDs are found in a row

	// =========================================================================
	// Part 1: Refresh Stale Items from the Market
	// =========================================================================
	log.Println("    -> [RMS Refresh] Part 1: Refreshing stale items from market...")

	// 1. Get all unique item IDs from the main items table.
	rows, err := db.Query("SELECT DISTINCT item_id, name_of_the_item FROM items WHERE item_id > 0")
	if err != nil {
		log.Printf("❌ [RMS Refresh] Part 1: Failed to query for all items: %v", err)
		return
	}
	defer rows.Close()

	type dbItem struct {
		ID   int
		Name string
	}
	var allDBItems []dbItem
	for rows.Next() {
		var item dbItem
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			log.Printf("⚠️ [RMS Refresh] Part 1: Failed to scan item: %v", err)
			continue
		}
		allDBItems = append(allDBItems, item)
	}

	// 2. Get all item IDs and last_checked times that are already in the cache.
	cacheRows, err := db.Query("SELECT item_id, last_checked FROM rms_item_cache")
	if err != nil {
		log.Printf("❌ [RMS Refresh] Part 1: Failed to query for cached items: %v", err)
		return
	}
	defer cacheRows.Close()

	cacheMap := make(map[int]time.Time)
	for cacheRows.Next() {
		var id int
		var lastCheckedStr sql.NullString
		if err := cacheRows.Scan(&id, &lastCheckedStr); err != nil {
			continue
		}
		if lastCheckedStr.Valid {
			if t, err := time.Parse(time.RFC3339, lastCheckedStr.String); err == nil {
				cacheMap[id] = t
			}
		}
	}

	// 3. Determine which items are missing or stale.
	var itemsToRefresh []dbItem
	for _, item := range allDBItems {
		lastChecked, exists := cacheMap[item.ID]
		if !exists {
			// Item is missing
			itemsToRefresh = append(itemsToRefresh, item)
		} else if time.Since(lastChecked) > staleDuration {
			// Item is stale
			itemsToRefresh = append(itemsToRefresh, item)
		}
	}

	if len(itemsToRefresh) > 0 {
		log.Printf("    -> [RMS Refresh] Part 1: Found %d market item(s) to refresh. Starting slow refresh (1 item / %v)...", len(itemsToRefresh), scrapeDelay)

		// 4. Scrape and cache the missing/stale items, with a delay.
		for i, item := range itemsToRefresh {
			log.Printf("    -> [RMS Refresh] Refreshing %d/%d: %s (ID: %d)", i+1, len(itemsToRefresh), item.Name, item.ID)

			scrapedItem, scrapeErr := scrapeRMSItemDetails(item.ID)
			if scrapeErr != nil {
				log.Printf("    -> ❌ [RMS Refresh] FAILED to scrape (Part 1) for item ID %d (%s): %v", item.ID, item.Name, scrapeErr)
				time.Sleep(scrapeDelay)
				continue
			}
			if saveErr := saveItemDetailsToCache(scrapedItem); saveErr != nil {
				log.Printf("    -> ⚠️ [RMS Refresh] FAILED to save (Part 1) for item ID %d (%s): %v", item.ID, item.Name, saveErr)
			}
			time.Sleep(scrapeDelay)
		}
	} else {
		log.Println("    -> [RMS Refresh] Part 1: All market item caches are up-to-date.")
	}
	log.Println("    -> [RMS Refresh] Part 1 complete.")

	// =========================================================================
	// Part 2: Proactively Discover New Items
	// =========================================================================
	log.Println("    -> [RMS Refresh] Part 2: Discovering new items by iterating IDs...")

	// 1. Start discovery from our defined constant (e.g., 501).
	//    We no longer query for MAX(item_id) here, as we want to fill gaps from the beginning.
	log.Printf("    -> [RMS Refresh] Part 2: Starting discovery from ID %d.", startDiscoveryID)
	consecutiveFailures := 0

	for currentItemID := startDiscoveryID; currentItemID <= maxPreRenewalItemID; currentItemID++ {
		// 2. Check if we *already* have this item in the cache.
		var exists int
		err := db.QueryRow("SELECT 1 FROM rms_item_cache WHERE item_id = ?", currentItemID).Scan(&exists)
		if err == nil {
			// Item is already cached. Skip it.
			consecutiveFailures = 0 // Reset failure count
			continue
		}

		// If the error was *not* "no rows", it was a real DB error. Log and skip.
		if err != sql.ErrNoRows {
			log.Printf("    -> ⚠️ [RMS Refresh] Part 2: DB error checking for item %d: %v. Skipping.", currentItemID, err)
			continue
		}

		// 3. This is a new ID (err was sql.ErrNoRows). Scrape it.
		log.Printf("    -> [RMS Refresh] Discovering ID %d...", currentItemID)
		scrapedItem, scrapeErr := scrapeRMSItemDetails(currentItemID)

		if scrapeErr != nil {
			log.Printf("    -> ⚠️ [RMS Refresh] Part 2: Failed to find item ID %d. %v", currentItemID, scrapeErr)
			consecutiveFailures++
		} else {
			// 4. Success! Save it to the cache.
			if saveErr := saveItemDetailsToCache(scrapedItem); saveErr != nil {
				log.Printf("    -> ⚠️ [RMS Refresh] FAILED to save (Part 2) for item ID %d (%s): %v", currentItemID, scrapedItem.Name, saveErr)
			} else {
				log.Printf("    -> ✅ [RMS Refresh] Part 2: Successfully discovered and cached ID %d (%s)", currentItemID, scrapedItem.Name)
			}
			consecutiveFailures = 0 // Reset failure count on success
		}

		// 5. Check if we should stop
		if consecutiveFailures >= maxConsecutiveFailures {
			log.Printf("    -> [RMS Refresh] Part 2: Stopping discovery after %d consecutive failures. Assuming end of item list.", maxConsecutiveFailures)
			break
		}
		if currentItemID == maxPreRenewalItemID {
			log.Printf("    -> [RMS Refresh] Part 2: Stopping discovery after hitting max ID %d.", maxPreRenewalItemID)
			break
		}

		// 6. Wait before the next attempt, regardless of success or failure.
		time.Sleep(scrapeDelay)
	}

	log.Println("✅ [RMS Refresh] Full refresh and discovery job complete.")
}
