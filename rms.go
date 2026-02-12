package main

import (
	"database/sql"
	"fmt"
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

// Pre-compiled regexes for RO Database scraping
var (
	rmsSlotRegex  = regexp.MustCompile(`\s*\[\d+\]\s*`)
	rmsIDNameRegex = regexp.MustCompile(`href="[^"]*/pt-BR/item/id/(\d+)"[^>]*>\s*([^<]+)\s*</a>`)
)

func scrapeRODatabaseSearch(query string, slots int) ([]ItemSearchResult, error) {
	searchQuery := rmsSlotRegex.ReplaceAllString(query, " ")
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

	client := http.Client{
		Timeout: 10 * time.Second,
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

	matches := rmsIDNameRegex.FindAllStringSubmatch(string(body), -1)

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
