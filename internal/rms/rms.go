// Package rms loads item details from the local internal_item_db table
// and scrapes rodatabase.com for item lookups by name + slot count.
package rms

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
	"time"
)

// Item mirrors models.RMSItem for the public surface of this package. The
// caller copies into its own type.
type Item struct {
	ID             int
	Name           string
	NamePT         string
	Type           string
	Buy            string
	Sell           string
	Weight         string
	Slots          int
	Script         string
	Class          string
	Prefix         string
	Description    string
	ImageURL       string
	DroppedBy      []string
	ObtainableFrom []string
}

// SearchResult is one row in the rodatabase.com search result.
type SearchResult struct {
	ID       int
	Name     string
	ImageURL string
}

// Lookup fetches item details from the local internal_item_db table.
func Lookup(db *sql.DB, itemID int) (*Item, error) {
	row := db.QueryRow(`
		SELECT name, name_pt, type, buy, sell, weight, slots, script
		FROM internal_item_db WHERE item_id = ?`, itemID)

	var item Item
	item.ID = itemID
	item.ImageURL = fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", itemID)

	var namePT sql.NullString
	var buy, sell, weight, slots sql.NullInt64
	var script sql.NullString

	err := row.Scan(&item.Name, &namePT, &item.Type, &buy, &sell, &weight, &slots, &script)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("item %d not found in internal_item_db", itemID)
		}
		return nil, fmt.Errorf("error querying internal_item_db for item %d: %w", itemID, err)
	}

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
		item.Weight = fmt.Sprintf("%.1f", float64(weight.Int64)/10.0)
	}
	if slots.Valid {
		item.Slots = int(slots.Int64)
	}
	if script.Valid {
		item.Script = script.String
	}
	return &item, nil
}

var (
	slotRegex   = regexp.MustCompile(`\s*\[\d+\]\s*`)
	idNameRegex = regexp.MustCompile(`href="[^"]*/pt-BR/item/id/(\d+)"[^>]*>\s*([^<]+)\s*</a>`)
)

// Search scrapes rodatabase.com for items matching `query` (with the slot
// suffix stripped) and the given slot count.
func Search(query string, slots int) ([]SearchResult, error) {
	searchQuery := slotRegex.ReplaceAllString(query, " ")
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

	client := http.Client{Timeout: 10 * time.Second}
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

	matches := idNameRegex.FindAllStringSubmatch(string(body), -1)
	if len(matches) == 0 {
		log.Printf("[D] [RODB/Search] No item IDs found on the page for query '%s'.", query)
		return nil, nil
	}

	var results []SearchResult
	seen := make(map[int]bool)
	for _, m := range matches {
		if len(m) > 2 {
			id, convErr := strconv.Atoi(m[1])
			if convErr != nil {
				continue
			}
			if !seen[id] {
				results = append(results, SearchResult{
					ID:       id,
					Name:     strings.TrimSpace(m[2]),
					ImageURL: fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", id),
				})
				seen[id] = true
			}
		}
	}

	log.Printf("[I] [RODB/Search] Found %d unique item(s) for query: '%s'", len(results), query)
	return results, nil
}
