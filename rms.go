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
	"sync"
	"time"
)

// rmsCacheMutex protects the rms_item_cache table from concurrent write operations.
var rmsCacheMutex sync.Mutex

// CachedItemName holds the minimal data needed for in-memory name searching.
type CachedItemName struct {
	ID         int
	Name       string         // Normalized (lowercase) English name
	NamePT     string         // Normalized (lowercase) Portuguese name
	OrigName   string         // Original display name
	OrigNamePT sql.NullString // Original display name (PT)
}

// LocalItemSearchResult defines the result of a local cache search.
type LocalItemSearchResult struct {
	ID             int
	Name           string
	IsPerfectMatch bool
}

// itemNameCache holds an in-memory copy of all item names for fast searching.
var itemNameCache struct {
	sync.RWMutex
	items []CachedItemName
}

func scrapeRMSItemDetails(itemID int) (*RMSItem, error) {
	log.Printf("[I] [RMS] Starting detailed scrape for Item ID: %d", itemID)

	rmsURL := fmt.Sprintf("https://ratemyserver.net/item_db.php?item_id=%d", itemID)
	log.Printf("[D] [RMS] Fetching primary data from: %s", rmsURL)
	client := http.Client{Timeout: 10 * time.Second}
	res, err := client.Get(rmsURL)
	if err != nil {
		log.Printf("[E] [RMS] FAILED to get URL. Error: %v", err)
		return nil, fmt.Errorf("failed to get URL from RMS: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Printf("[E] [RMS] FAILED with non-200 status code: %d %s", res.StatusCode, res.Status)

		return nil, fmt.Errorf("RMS status code error: %d %s", res.StatusCode, res.Status)
	}
	log.Printf("[D] [RMS] Successfully received response.")

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Printf("[E] [RMS] FAILED to parse HTML. Error: %v", err)
		return nil, fmt.Errorf("failed to parse RMS HTML: %w", err)
	}
	log.Printf("[D] [RMS] HTML parsed successfully.")

	item := &RMSItem{
		ID:       itemID,
		ImageURL: fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", itemID),
	}

	item.Name = strings.TrimSpace(doc.Find("div.main_block b").First().Text())
	if item.Name == "" {
		log.Printf("[W] [RMS] Could not find item name.")
	} else {
		log.Printf("[D] [RMS] Found primary name: '%s'", item.Name)
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
	log.Printf("[D] [RDB] Fetching Portuguese name from: %s", rdbURL)
	rdbRes, err := client.Get(rdbURL)
	if err != nil {
		log.Printf("[W] [RDB] Could not fetch from RagnarokDatabase for item %d: %v", itemID, err)
		return item, nil
	}
	defer rdbRes.Body.Close()

	if rdbRes.StatusCode == 200 {
		log.Printf("[D] [RDB] Successfully received response.")
		body, readErr := io.ReadAll(rdbRes.Body)
		if readErr != nil {
			log.Printf("[W] [RDB] Could not read body from RagnarokDatabase for item %d: %v", itemID, readErr)
		} else {
			reNamePT := regexp.MustCompile(`<h1 class="item-title-db">([^<]+)</h1>`)
			matches := reNamePT.FindStringSubmatch(string(body))
			if len(matches) > 1 {
				item.NamePT = strings.TrimSpace(matches[1])
				log.Printf("[D] [RDB] Found Portuguese name: '%s'", item.NamePT)
			} else {
				log.Printf("[W] [RDB] Could not find Portuguese name with regex on page.")
			}
		}
	} else {
		log.Printf("[W] [RDB] Received non-200 status (%d) from RagnarokDatabase for item %d", rdbRes.StatusCode, itemID)
	}

	log.Printf("[I] [RMS] Finished detailed scrape for Item ID: %d", itemID)
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

	// --- THE FIX IS HERE ---
	// Lock the mutex to ensure only one goroutine writes to this table at a time.
	rmsCacheMutex.Lock()
	defer rmsCacheMutex.Unlock()

	_, err = db.Exec(`
		INSERT OR REPLACE INTO rms_item_cache
		(item_id, name, name_pt, image_url, item_type, item_class, buy, sell, weight, prefix, description, script, dropped_by_json, obtainable_from_json, last_checked)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.ID, item.Name, item.NamePT, item.ImageURL, item.Type, item.Class, item.Buy, item.Sell,
		item.Weight, item.Prefix, item.Description, item.Script,
		string(droppedByJSON), string(obtainableFromJSON), time.Now().Format(time.RFC3339),
	)
	// --- END FIX ---

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

	log.Printf("[I] [RMS] Caching details for new/missing item: %s (ID: %d)", itemName, itemID)
	scrapedItem, scrapeErr := scrapeRMSItemDetails(itemID)
	if scrapeErr != nil {
		log.Printf("[W] [RMS] Failed to scrape RateMyServer for item ID %d (%s): %v", itemID, itemName, scrapeErr)
		return
	}

	if saveErr := saveItemDetailsToCache(scrapedItem); saveErr != nil {
		log.Printf("[W] [RMS] Failed to save item ID %d (%s) to cache: %v", itemID, itemName, saveErr)
	} else {
		log.Printf("[I] [RMS] Successfully cached details for item ID %d (%s).", itemID, itemName)
	}
}

func populateMissingCachesOnStartup() {
	log.Println("[I] [RMS/Cache] Starting background task: Verifying RMS item cache...")

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
		log.Printf("[E] [RMS/Cache] Failed to query for missing items: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item dbItem
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			log.Printf("[W] [RMS/Cache] Failed to scan item: %v", err)
			continue
		}
		itemsToCache = append(itemsToCache, item)
	}

	if len(itemsToCache) == 0 {
		log.Println("[I] [RMS/Cache] All items are already cached. No work to do.")
		return
	}

	log.Printf("[I] [RMS/Cache] Found %d item(s) missing from the RMS cache. Populating concurrently...", len(itemsToCache))

	var wg sync.WaitGroup

	sem := make(chan struct{}, 5)

	for i, item := range itemsToCache {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int, itm dbItem) {
			defer wg.Done()

			log.Printf("[D] [RMS/Cache] Caching %d/%d: %s (ID: %d)", idx+1, len(itemsToCache), itm.Name, itm.ID)
			scrapeAndCacheItemIfNotExists(itm.ID, itm.Name)

			time.Sleep(1 * time.Second)

			<-sem
		}(i, item)
	}

	wg.Wait()
	log.Println("[I] [RMS/Cache] Finished populating missing cache entries.")
}

func scrapeRMSItemSearch(query string) ([]ItemSearchResult, error) {

	rmsURL := fmt.Sprintf("https://ratemyserver.net/index.php?iname=%s&page=item_db&quick=1&isearch=Search", url.QueryEscape(query))
	log.Printf("[I] [RMS/Search] Performing live search for: '%s'", query)
	log.Printf("[D] [RMS/Search] URL: %s", rmsURL)

	client := http.Client{Timeout: 10 * time.Second}
	res, err := client.Get(rmsURL)
	if err != nil {
		log.Printf("[E] [RMS/Search] Failed to get URL: %v", err)
		return nil, fmt.Errorf("failed to get search URL: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("[W] [RMS/Search] Received non-200 status code: %d %s", res.StatusCode, res.Status)
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Printf("[E] [RMS/Search] Failed to parse search HTML: %v", err)
		return nil, fmt.Errorf("failed to parse search HTML: %w", err)
	}

	var results []ItemSearchResult
	idRegex := regexp.MustCompile(`Item ID# (\d+)`)

	log.Printf("[D] [RMS/Search] Searching for result containers in HTML...")
	doc.Find("div[style*='display: flex']").Each(func(i int, s *goquery.Selection) {
		var result ItemSearchResult
		var found bool

		fullText := s.Text()
		log.Printf("[D] [RMS/Search] Processing block %d: Raw text -> \"%s\"", i+1, strings.TrimSpace(fullText))

		matches := idRegex.FindStringSubmatch(fullText)

		if len(matches) > 1 {
			id, _ := strconv.Atoi(matches[1])
			result.ID = id
			found = true

			result.Name = strings.TrimSpace(s.Find("b").First().Text())

			result.ImageURL = fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", id)
			log.Printf("[D] [RMS/Search] Extracted: ID=%d, Name='%s'", result.ID, result.Name)
		} else {
			log.Printf("[D] [RMS/Search] No Item ID found in block %d.", i+1)
		}

		if found && result.ID > 0 && result.Name != "" {
			results = append(results, result)
		}
	})

	if len(results) == 0 {
		log.Printf("[D] [RMS/Search] No valid items were parsed from the page.")
	}

	log.Printf("[I] [RMS/Search] Found %d item(s) for query: '%s'", len(results), query)
	return results, nil
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

	log.Printf("[I] [RODB/Search] Performing search for: '%s' (Original: '%s', Slots: %d)", searchQuery, query, slots)
	log.Printf("[D] [RODB/Search] URL: %s", searchURL)

	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := http.Client{
		Timeout:   10 * time.Second,
		Transport: customTransport,
	}
	res, err := client.Get(searchURL)
	if err != nil {
		log.Printf("[E] [RODB/Search] Failed to get URL: %v", err)
		return nil, fmt.Errorf("failed to get search URL: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("[W] [RODB/Search] Received non-200 status code: %d %s", res.StatusCode, res.Status)
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("[E] [RODB/Search] Failed to read response body: %v", err)
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	idNameRegex := regexp.MustCompile(`href="[^"]*/pt-BR/item/id/(\d+)"[^>]*>\s*([^<]+)\s*</a>`)
	matches := idNameRegex.FindAllStringSubmatch(string(body), -1)

	if len(matches) == 0 {
		log.Printf("[D] [RODB/Search] No item IDs found on the page for query '%s'.", query)
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

	log.Printf("[I] [RODB/Search] Found %d unique item(s) for query: '%s'", len(results), query)
	return results, nil
}

func runFullRMSCacheJob() {
	log.Println("[I] [Job/RMS] Starting full RMS cache refresh job...")

	const staleDuration = 7 * 24 * time.Hour
	const scrapeDelay = 5 * time.Second
	const startDiscoveryID = 501
	const maxPreRenewalItemID = 20000
	const maxConsecutiveFailures = 1000

	log.Println("[I] [Job/RMS] Part 1: Refreshing stale items from market...")

	rows, err := db.Query("SELECT DISTINCT item_id, name_of_the_item FROM items WHERE item_id > 0")
	if err != nil {
		log.Printf("[E] [Job/RMS] Part 1: Failed to query for all items: %v", err)
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
			log.Printf("[W] [Job/RMS] Part 1: Failed to scan item: %v", err)
			continue
		}
		allDBItems = append(allDBItems, item)
	}

	cacheRows, err := db.Query("SELECT item_id, last_checked FROM rms_item_cache")
	if err != nil {
		log.Printf("[E] [Job/RMS] Part 1: Failed to query for cached items: %v", err)
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
		log.Printf("[I] [Job/RMS] Part 1: Found %d market item(s) to refresh. Starting slow refresh (1 item / %v)...", len(itemsToRefresh), scrapeDelay)

		for i, item := range itemsToRefresh {
			log.Printf("[D] [Job/RMS] Refreshing %d/%d: %s (ID: %d)", i+1, len(itemsToRefresh), item.Name, item.ID)

			scrapedItem, scrapeErr := scrapeRMSItemDetails(item.ID)
			if scrapeErr != nil {
				log.Printf("[E] [Job/RMS] FAILED to scrape (Part 1) for item ID %d (%s): %v", item.ID, item.Name, scrapeErr)
				time.Sleep(scrapeDelay)
				continue
			}
			if saveErr := saveItemDetailsToCache(scrapedItem); saveErr != nil {
				log.Printf("[W] [Job/RMS] FAILED to save (Part 1) for item ID %d (%s): %v", item.ID, item.Name, saveErr)
			}
			time.Sleep(scrapeDelay)
		}
	} else {
		log.Println("[I] [Job/RMS] Part 1: All market item caches are up-to-date.")
	}
	log.Println("[I] [Job/RMS] Part 1 complete.")

	log.Println("[I] [Job/RMS] Part 2: Discovering new items by iterating IDs...")

	log.Println("[D] [Job/RMS] Part 2: Fetching all existing cached item IDs...")
	cachedIDs := make(map[int]bool)

	for id := range cacheMap {
		cachedIDs[id] = true
	}

	log.Printf("[I] [Job/RMS] Part 2: Found %d existing IDs from Part 1's map. Starting discovery from ID %d.", len(cachedIDs), startDiscoveryID)

	consecutiveFailures := 0

	for currentItemID := startDiscoveryID; currentItemID <= maxPreRenewalItemID; currentItemID++ {

		if cachedIDs[currentItemID] {

			consecutiveFailures = 0
			continue
		}

		log.Printf("[D] [Job/RMS] Discovering ID %d...", currentItemID)
		scrapedItem, scrapeErr := scrapeRMSItemDetails(currentItemID)

		if scrapeErr != nil {
			log.Printf("[W] [Job/RMS] Part 2: Failed to find item ID %d. %v", currentItemID, scrapeErr)
			consecutiveFailures++
		} else {

			if saveErr := saveItemDetailsToCache(scrapedItem); saveErr != nil {
				log.Printf("[W] [Job/RMS] FAILED to save (Part 2) for item ID %d (%s): %v", currentItemID, scrapedItem.Name, saveErr)
			} else {
				log.Printf("[I] [Job/RMS] Part 2: Successfully discovered and cached ID %d (%s)", currentItemID, scrapedItem.Name)

				cachedIDs[currentItemID] = true

			}
			consecutiveFailures = 0
		}

		if consecutiveFailures >= maxConsecutiveFailures {
			log.Printf("[I] [Job/RMS] Part 2: Stopping discovery after %d consecutive failures. Assuming end of item list.", maxConsecutiveFailures)
			break
		}
		if currentItemID == maxPreRenewalItemID {
			log.Printf("[I] [Job/RMS] Part 2: Stopping discovery after hitting max ID %d.", maxPreRenewalItemID)
			break
		}

		time.Sleep(scrapeDelay)
	}

	log.Println("[I] [Job/RMS] Full refresh and discovery job complete.")
}

// LoadItemNameCache queries the DB and populates the in-memory item name cache.
// This should be called ONCE on application startup after the DB is initialized.
func LoadItemNameCache() error {
	log.Println("[I] [Cache] Loading all item names into memory for fuzzy search...")

	// Query for all distinct items from the cache table
	rows, err := db.Query(`
		SELECT item_id, name, name_pt 
		FROM rms_item_cache 
		WHERE item_id > 0 AND name IS NOT NULL AND name != ''
	`)
	if err != nil {
		return fmt.Errorf("failed to query rms_item_cache for populating name cache: %w", err)
	}
	defer rows.Close()

	var newCache []CachedItemName
	var count int
	for rows.Next() {
		var item CachedItemName
		var namePT sql.NullString

		if err := rows.Scan(&item.ID, &item.OrigName, &namePT); err != nil {
			log.Printf("[W] [Cache] Failed to scan item name: %v", err)
			continue
		}

		// Store original and normalized (lowercase) names
		item.Name = strings.ToLower(item.OrigName)
		item.OrigNamePT = namePT // Keep original for result
		if namePT.Valid {
			item.NamePT = strings.ToLower(namePT.String)
		}

		newCache = append(newCache, item)
		count++
	}

	// Atomically update the global cache
	itemNameCache.Lock()
	itemNameCache.items = newCache
	itemNameCache.Unlock()

	log.Printf("[I] [Cache] Successfully loaded %d item names into memory.", count)
	return nil
}
