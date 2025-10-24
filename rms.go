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

func scrapeRMSItemDetails(itemID int) (*RMSItem, error) {
	log.Printf("ℹ️ [RMS Scraper] Starting detailed scrape for Item ID: %d", itemID)

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

	item.Name = strings.TrimSpace(doc.Find("div.main_block b").First().Text())
	if item.Name == "" {
		log.Printf("   -> ⚠️ [RMS] Could not find item name.")
	} else {
		log.Printf("   -> [RMS] Found primary name: '%s'", item.Name)
	}

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

	item.Description = strings.TrimSpace(doc.Find("th:contains('Description')").Next().Find("div.longtext").Text())
	if item.Description == "" {
		item.Description = strings.TrimSpace(doc.Find("th:contains('Description')").Next().Text())
	}

	item.Script = strings.TrimSpace(doc.Find("th:contains('Item Script')").Next().Find("div.db_script_txt").Text())

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

	doc.Find("th:contains('Obtainable From')").Next().Find("a").Each(func(i int, s *goquery.Selection) {
		item.ObtainableFrom = append(item.ObtainableFrom, strings.TrimSpace(s.Text()))
	})

	rdbURL := fmt.Sprintf("https://ragnarokdatabase.com/item/%d", itemID)
	log.Printf("   -> [RDB] Fetching Portuguese name from: %s", rdbURL)
	rdbRes, err := client.Get(rdbURL)
	if err != nil {
		log.Printf("   -> ⚠️ [RDB] Could not fetch from RagnarokDatabase for item %d: %v", itemID, err)
		return item, nil
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

		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("item %d not found in cache", itemID)
		}

		return nil, fmt.Errorf("error querying cache for item %d: %w", itemID, err)
	}

	if err := json.Unmarshal([]byte(droppedByJSON), &item.DroppedBy); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DroppedBy from cache for item %d: %w", itemID, err)
	}
	if err := json.Unmarshal([]byte(obtainableFromJSON), &item.ObtainableFrom); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ObtainableFrom from cache for item %d: %w", itemID, err)
	}

	return &item, nil
}

func saveItemDetailsToCache(item *RMSItem) error {
	droppedByJSON, err := json.Marshal(item.DroppedBy)
	if err != nil {
		return fmt.Errorf("failed to marshal DroppedBy for caching item %d: %w", item.ID, err)
	}
	obtainableFromJSON, err := json.Marshal(item.ObtainableFrom)
	if err != nil {
		return fmt.Errorf("failed to marshal ObtainableFrom for caching item %d: %w", item.ID, err)
	}

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

func scrapeAndCacheItemIfNotExists(itemID int, itemName string) {
	if itemID <= 0 {
		return
	}

	_, err := getItemDetailsFromCache(itemID)
	if err == nil {

		return
	}

	log.Printf("ℹ️ Caching details for new/missing item: %s (ID: %d)", itemName, itemID)
	scrapedItem, scrapeErr := scrapeRMSItemDetails(itemID)
	if scrapeErr != nil {
		log.Printf("⚠️ Failed to scrape RateMyServer for item ID %d (%s): %v", itemID, itemName, scrapeErr)
		return
	}

	if saveErr := saveItemDetailsToCache(scrapedItem); saveErr != nil {
		log.Printf("⚠️ Failed to save item ID %d (%s) to cache: %v", itemID, itemName, saveErr)
	} else {
		log.Printf("✅ Successfully cached details for item ID %d (%s).", itemID, itemName)
	}
}

func populateMissingCachesOnStartup() {
	log.Println("🛠️ Starting background task: Verifying RMS item cache...")

	type dbItem struct {
		ID   int
		Name string
	}
	var itemsToCache []dbItem

	query := `
		SELECT
			i.item_id,
			SUBSTR(MAX(i.date_and_time_retrieved || i.name_of_the_item), 20) as name
		FROM items i
		WHERE i.item_id > 0
		AND NOT EXISTS (
			SELECT 1
			FROM rms_item_cache r
			WHERE r.item_id = i.item_id
		)
		GROUP BY i.item_id;
	`
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("❌ [Cache Verification] Failed to query for missing items: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item dbItem
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			log.Printf("⚠️ [Cache Verification] Failed to scan item: %v", err)
			continue
		}
		itemsToCache = append(itemsToCache, item)
	}

	if len(itemsToCache) == 0 {
		log.Println("✅ [Cache Verification] All items are already cached. No work to do.")
		return
	}

	log.Printf("ℹ️ [Cache Verification] Found %d item(s) missing from the RMS cache. Populating now...", len(itemsToCache))

	for i, item := range itemsToCache {
		log.Printf("    -> Caching %d/%d: %s (ID: %d)", i+1, len(itemsToCache), item.Name, item.ID)
		scrapeAndCacheItemIfNotExists(item.ID, item.Name)

		time.Sleep(1 * time.Second)
	}

	log.Println("✅ [Cache Verification] Finished populating missing cache entries.")
}

func scrapeRMSItemSearch(query string) ([]ItemSearchResult, error) {

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

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Printf("❌ [RMS HTML Search] Failed to parse search HTML: %v", err)
		return nil, fmt.Errorf("failed to parse search HTML: %w", err)
	}

	var results []ItemSearchResult
	idRegex := regexp.MustCompile(`Item ID# (\d+)`)

	log.Printf("   [RMS HTML Search] Searching for result containers in HTML...")
	doc.Find("div[style*='display: flex']").Each(func(i int, s *goquery.Selection) {
		var result ItemSearchResult
		var found bool

		fullText := s.Text()
		log.Printf("   [RMS HTML Search] Processing block %d: Raw text -> \"%s\"", i+1, strings.TrimSpace(fullText))

		matches := idRegex.FindStringSubmatch(fullText)

		if len(matches) > 1 {
			id, _ := strconv.Atoi(matches[1])
			result.ID = id
			found = true

			result.Name = strings.TrimSpace(s.Find("b").First().Text())

			result.ImageURL = fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", id)
			log.Printf("   [RMS HTML Search]   -> Extracted: ID=%d, Name='%s'", result.ID, result.Name)
		} else {
			log.Printf("   [RMS HTML Search]   -> No Item ID found in block %d.", i+1)
		}

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

func scrapeRagnarokDatabaseSearch(query string) ([]int, error) {

	searchURL := fmt.Sprintf("https://ragnarokdatabase.com/search/all/%s", query)
	log.Printf("➡️ [Fallback Search] Performing search on RagnarokDatabase for: '%s'", query)
	log.Printf("   URL: %s", searchURL)

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

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("❌ [Fallback Search] Failed to read response body: %v", err)
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	idNameRegex := regexp.MustCompile(`href="https://ragnarokdatabase.com/item/(\d+)/[^"]+">([^<]+)</a>`)
	matches := idNameRegex.FindAllStringSubmatch(string(body), -1)

	if len(matches) == 0 {
		log.Printf("   [Fallback Search] No item IDs found on the page for query '%s'.", query)
		return nil, nil
	}

	var itemIDs []int
	seenIDs := make(map[int]bool)

	updateStmt, err := db.Prepare("UPDATE rms_item_cache SET name_pt = ? WHERE item_id = ?")
	if err != nil {
		log.Printf("⚠️ [Fallback Search] Could not prepare update statement for name_pt, will skip updating: %v", err)

	} else {
		defer updateStmt.Close()
	}

	for _, match := range matches {
		if len(match) > 2 {
			id, err := strconv.Atoi(match[1])
			if err != nil {
				continue
			}

			if !seenIDs[id] {
				itemIDs = append(itemIDs, id)
				seenIDs[id] = true
			}

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

func scrapeRODatabaseSearch(query string, slots int) ([]ItemSearchResult, error) {

	reSlots := regexp.MustCompile(`\s*\[\d+\]\s*`)
	searchQuery := reSlots.ReplaceAllString(query, " ")
	searchQuery = strings.TrimSpace(searchQuery)

	v := url.Values{}
	v.Set("name", searchQuery)
	if slots > 0 {
		v.Set("slots_operator", "eq")
		v.Set("slots", strconv.Itoa(slots))
	}
	searchURL := fmt.Sprintf("https://rodatabase.com/pt-BR/item/search?%s", v.Encode())

	log.Printf("➡️ [RODatabase Search] Performing search for: '%s' (Original: '%s', Slots: %d)", searchQuery, query, slots)
	log.Printf("	URL: %s", searchURL)

	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := http.Client{
		Timeout:   10 * time.Second,
		Transport: customTransport,
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

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("❌ [RODatabase Search] Failed to read response body: %v", err)
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	idNameRegex := regexp.MustCompile(`href="[^"]*/pt-BR/item/id/(\d+)"[^>]*>\s*([^<]+)\s*</a>`)
	matches := idNameRegex.FindAllStringSubmatch(string(body), -1)

	if len(matches) == 0 {
		log.Printf("	[RODatabase Search] No item IDs found on the page for query '%s'.", query)
		return nil, nil
	}

	var results []ItemSearchResult
	seenIDs := make(map[int]bool)

	for _, match := range matches {
		if len(match) > 2 {
			id, convErr := strconv.Atoi(match[1])
			if convErr != nil {
				continue
			}
			if !seenIDs[id] {
				name := strings.TrimSpace(match[2])
				results = append(results, ItemSearchResult{
					ID:       id,
					Name:     name,
					ImageURL: fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", id),
				})
				seenIDs[id] = true
			}
		}
	}

	log.Printf("✅ [RODatabase Search] Found %d unique item(s) for query: '%s'", len(results), query)
	return results, nil
}

func runFullRMSCacheJob() {
	log.Println("🛠️ [RMS Refresh] Starting full RMS cache refresh job...")

	const staleDuration = 7 * 24 * time.Hour
	const scrapeDelay = 5 * time.Second
	const startDiscoveryID = 501
	const maxPreRenewalItemID = 20000
	const maxConsecutiveFailures = 1000

	log.Println("    -> [RMS Refresh] Part 1: Refreshing stale items from market...")

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

	var itemsToRefresh []dbItem
	for _, item := range allDBItems {
		lastChecked, exists := cacheMap[item.ID]
		if !exists {

			itemsToRefresh = append(itemsToRefresh, item)
		} else if time.Since(lastChecked) > staleDuration {

			itemsToRefresh = append(itemsToRefresh, item)
		}
	}

	if len(itemsToRefresh) > 0 {
		log.Printf("    -> [RMS Refresh] Part 1: Found %d market item(s) to refresh. Starting slow refresh (1 item / %v)...", len(itemsToRefresh), scrapeDelay)

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

	log.Println("    -> [RMS Refresh] Part 2: Discovering new items by iterating IDs...")

	log.Println("    -> [RMS Refresh] Part 2: Fetching all existing cached item IDs...")
	cachedIDs := make(map[int]bool)

	for id := range cacheMap {
		cachedIDs[id] = true
	}

	log.Printf("    -> [RMS Refresh] Part 2: Found %d existing IDs from Part 1's map. Starting discovery from ID %d.", len(cachedIDs), startDiscoveryID)

	consecutiveFailures := 0

	for currentItemID := startDiscoveryID; currentItemID <= maxPreRenewalItemID; currentItemID++ {

		if cachedIDs[currentItemID] {

			consecutiveFailures = 0
			continue
		}

		log.Printf("    -> [RMS Refresh] Discovering ID %d...", currentItemID)
		scrapedItem, scrapeErr := scrapeRMSItemDetails(currentItemID)

		if scrapeErr != nil {
			log.Printf("    -> ⚠️ [RMS Refresh] Part 2: Failed to find item ID %d. %v", currentItemID, scrapeErr)
			consecutiveFailures++
		} else {

			if saveErr := saveItemDetailsToCache(scrapedItem); saveErr != nil {
				log.Printf("    -> ⚠️ [RMS Refresh] FAILED to save (Part 2) for item ID %d (%s): %v", currentItemID, scrapedItem.Name, saveErr)
			} else {
				log.Printf("    -> ✅ [RMS Refresh] Part 2: Successfully discovered and cached ID %d (%s)", currentItemID, scrapedItem.Name)

				cachedIDs[currentItemID] = true

			}
			consecutiveFailures = 0
		}

		if consecutiveFailures >= maxConsecutiveFailures {
			log.Printf("    -> [RMS Refresh] Part 2: Stopping discovery after %d consecutive failures. Assuming end of item list.", maxConsecutiveFailures)
			break
		}
		if currentItemID == maxPreRenewalItemID {
			log.Printf("    -> [RMS Refresh] Part 2: Stopping discovery after hitting max ID %d.", maxPreRenewalItemID)
			break
		}

		time.Sleep(scrapeDelay)
	}

	log.Println("✅ [RMS Refresh] Full refresh and discovery job complete.")
}
