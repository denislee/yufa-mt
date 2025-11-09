package main

import (
	"crypto/tls"
	"database/sql"
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
		if next := s.Next(); next.Length() > 0 && next.HasClass("info_grid_item") {
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
		case "Slot":
			if intVal, err := strconv.Atoi(value); err == nil {
				item.Slots = intVal
			} else {
				item.Slots = 0
			}
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
				// --- MODIFICATION IS HERE ---
				// Extract the raw name
				rawNamePT := strings.TrimSpace(matches[1])

				// Use the existing package-level regex from handlers.go to remove slots
				// (e.g., reSlotRemover = regexp.MustCompile(`\s*\[\d+\]\s*`))
				cleanNamePT := reSlotRemover.ReplaceAllString(rawNamePT, " ")

				// Assign the cleaned and trimmed name
				item.NamePT = strings.TrimSpace(cleanNamePT)
				// --- END MODIFICATION ---

				log.Printf("[D] [RDB] Found Portuguese name: '%s' (Cleaned from: '%s')", item.NamePT, rawNamePT)
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

// getItemDetailsFromCache queries the new local item database (internal_item_db).
func getItemDetailsFromCache(itemID int) (*RMSItem, error) {
	row := db.QueryRow(`
		SELECT name, name_pt, type, buy, sell, weight, slots, script
		FROM internal_item_db WHERE item_id = ?`, itemID)

	var item RMSItem
	item.ID = itemID
	item.ImageURL = fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", itemID)

	var namePT sql.NullString
	var buy, sell, weight, slots sql.NullInt64
	var script sql.NullString

	err := row.Scan(
		&item.Name, &namePT, &item.Type, &buy, &sell,
		&weight, &slots, &script,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("item %d not found in internal_item_db", itemID)
		}
		return nil, fmt.Errorf("error querying internal_item_db for item %d: %w", itemID, err)
	}

	// Populate the RMSItem struct from the data
	if namePT.Valid {
		item.NamePT = namePT.String
	}
	if buy.Valid {
		item.Buy = strconv.FormatInt(buy.Int64, 10)
	}
	if sell.Valid {
		item.Sell = strconv.FormatInt(sell.Int64, 10)
	}
	if weight.Valid {
		// Weight in YAML is 10x (e.g., 100 = 10.0)
		item.Weight = fmt.Sprintf("%.1f", float64(weight.Int64)/10.0)
	}
	if slots.Valid {
		item.Slots = int(slots.Int64)
	}
	if script.Valid {
		item.Script = script.String
	}

	// These fields are not in internal_item_db and will be empty
	item.Class = ""
	item.Prefix = ""
	item.Description = ""
	item.DroppedBy = nil
	item.ObtainableFrom = nil

	return &item, nil
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
