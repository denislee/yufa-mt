package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

// (Structs like Item, comparableItem, Column, etc., remain the same)
// ...

type Item struct {
	ID             int
	Name           string
	ItemID         int
	Quantity       int
	Price          string
	StoreName      string
	SellerName     string
	Timestamp      string
	MapName        string
	MapCoordinates string
	IsAvailable    bool
}

// A comparable version of Item for easy checking of differences.
type comparableItem struct {
	Name           string
	ItemID         int
	Quantity       int
	Price          string
	StoreName      string
	SellerName     string
	MapName        string
	MapCoordinates string
}

// Column struct to define toggleable columns
type Column struct {
	ID          string
	DisplayName string
}

// MarketEvent struct for logging market changes
type MarketEvent struct {
	Timestamp string
	EventType string
	ItemName  string
	ItemID    int // Added ItemID
	Details   map[string]interface{}
}

// PageData for the detailed full list view
type PageData struct {
	Items          []Item
	SearchQuery    string
	SortBy         string
	Order          string
	ShowAll        bool
	LastScrapeTime string
	VisibleColumns map[string]bool
	AllColumns     []Column
	ColumnParams   template.URL
}

// ActivityPageData for the new market activity page
type ActivityPageData struct {
	MarketEvents   []MarketEvent
	LastScrapeTime string
}

// ItemSummary for the main summary view
type ItemSummary struct {
	Name         string
	ItemID       int
	LowestPrice  sql.NullInt64 // Use sql.NullInt64 to handle cases with no available listings
	HighestPrice sql.NullInt64 // Use sql.NullInt64
	ListingCount int
}

// SummaryPageData for the summary view template (now the main page data)
type SummaryPageData struct {
	Items          []ItemSummary
	SearchQuery    string
	SortBy         string
	Order          string
	ShowAll        bool // To track the state of the "show all" checkbox
	LastScrapeTime string
}

type PricePointDetails struct {
	Timestamp         string `json:"Timestamp"`
	LowestPrice       int    `json:"LowestPrice"`
	LowestQuantity    int    `json:"LowestQuantity"`
	LowestStoreName   string `json:"LowestStoreName"`
	LowestSellerName  string `json:"LowestSellerName"`
	LowestMapName     string `json:"LowestMapName"`
	LowestMapCoords   string `json:"LowestMapCoords"`
	HighestPrice      int    `json:"HighestPrice"`
	HighestQuantity   int    `json:"HighestQuantity"`
	HighestStoreName  string `json:"HighestStoreName"`
	HighestSellerName string `json:"HighestSellerName"`
	HighestMapName    string `json:"HighestMapName"`
	HighestMapCoords  string `json:"HighestMapCoords"`
}

// ItemListing holds details for a single current listing for the info cards.
type ItemListing struct {
	Price          int    `json:"Price"`
	Quantity       int    `json:"Quantity"`
	StoreName      string `json:"StoreName"`
	SellerName     string `json:"SellerName"`
	MapName        string `json:"MapName"`
	MapCoordinates string `json:"MapCoordinates"`
	Timestamp      string `json:"Timestamp"`
}

// RMSItem holds the detailed information scraped from RateMyServer.
type RMSItem struct {
	ID             int
	Name           string
	ImageURL       string
	Type           string
	Class          string
	Buy            string
	Sell           string
	Weight         string
	Prefix         string
	Description    string
	Script         string
	DroppedBy      []RMSDrop
	ObtainableFrom []string
}

// RMSDrop holds monster drop information from RateMyServer.
type RMSDrop struct {
	Monster string
	Rate    string
}

type HistoryPageData struct {
	ItemName           string
	PriceDataJSON      template.JS
	OverallLowest      int
	OverallHighest     int
	CurrentLowestJSON  template.JS
	CurrentHighestJSON template.JS
	ItemDetails        *RMSItem
	AllListings        []Item
	LastScrapeTime     string
}

func main() {
	var err error
	db, err = initDB("./market_data.db")
	if err != nil {
		log.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer db.Close()

	go populateMissingCachesOnStartup()

	go startBackgroundScraper()

	http.HandleFunc("/", viewHandler)              // Now serves the summary page
	http.HandleFunc("/full-list", fullListHandler) // New route for the detailed list
	http.HandleFunc("/item", historyHandler)
	http.HandleFunc("/activity", activityHandler)

	port := "8080"
	log.Printf("🚀 Web server started. Open http://localhost:%s in your browser.", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("❌ Failed to start web server: %v", err)
	}
}

// (The rest of your functions remain the same: getLastScrapeTime, etc.)
// ...
// getLastScrapeTime is a helper function to get the most recent scrape time.
func getLastScrapeTime() string {
	var lastScrapeTimestamp sql.NullString
	err := db.QueryRow("SELECT MAX(timestamp) FROM scrape_history").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last scrape time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

// viewHandler serves the main summary page
func viewHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	// 1. Get parameters from the request
	searchQuery := r.FormValue("query")
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")

	// Default to "only available" unless a form was submitted with the box unchecked.
	formSubmitted := len(r.Form) > 0
	showAll := false
	if formSubmitted && r.FormValue("only_available") != "true" {
		showAll = true
	}

	// 2. Build the query dynamically
	params := []interface{}{"%" + searchQuery + "%"}

	// Base query with conditional aggregation for lowest/highest price and a sum for available count
	baseQuery := `
        SELECT
            name_of_the_item,
            MIN(item_id) as item_id,
            MIN(CASE WHEN is_available = 1 THEN CAST(REPLACE(price, ',', '') AS INTEGER) ELSE NULL END) as lowest_price,
            MAX(CASE WHEN is_available = 1 THEN CAST(REPLACE(price, ',', '') AS INTEGER) ELSE NULL END) as highest_price,
            SUM(CASE WHEN is_available = 1 THEN 1 ELSE 0 END) as listing_count
        FROM items
        WHERE name_of_the_item LIKE ?
        GROUP BY name_of_the_item
    `
	// Add HAVING clause if we only want to show items with available listings
	if !showAll {
		baseQuery += " HAVING listing_count > 0"
	}

	// 3. Handle sorting securely
	allowedSorts := map[string]string{
		"name":          "name_of_the_item",
		"item_id":       "item_id",
		"listings":      "listing_count",
		"lowest_price":  "lowest_price",
		"highest_price": "highest_price",
	}
	orderByClause, ok := allowedSorts[sortBy]
	if !ok {
		orderByClause, sortBy = "name_of_the_item", "name" // Default sort
	}
	if strings.ToUpper(order) != "DESC" {
		order = "ASC"
	}

	// Append ORDER BY to the query, with a secondary sort for stability
	query := fmt.Sprintf("%s ORDER BY %s %s, name_of_the_item ASC;", baseQuery, orderByClause, order)

	rows, err := db.Query(query, params...)
	if err != nil {
		http.Error(w, "Database query for summary failed", http.StatusInternalServerError)
		log.Printf("❌ Summary query error: %v", err)
		return
	}
	defer rows.Close()

	var items []ItemSummary
	for rows.Next() {
		var item ItemSummary
		// Scan into the new struct with sql.NullInt64 for prices
		if err := rows.Scan(&item.Name, &item.ItemID, &item.LowestPrice, &item.HighestPrice, &item.ListingCount); err != nil {
			log.Printf("⚠️ Failed to scan summary row: %v", err)
			continue
		}
		items = append(items, item)
	}

	// Create a FuncMap to register the "lower" function.
	funcMap := template.FuncMap{
		"lower": strings.ToLower,
	}

	// Parse the template file with the custom function map.
	tmpl, err := template.New("index.html").Funcs(funcMap).ParseFiles("index.html")
	if err != nil {
		http.Error(w, "Could not load index template", http.StatusInternalServerError)
		log.Printf("❌ Could not load index.html template: %v", err)
		return
	}

	// 4. Populate data for the template
	data := SummaryPageData{
		Items:          items,
		SearchQuery:    searchQuery,
		SortBy:         sortBy,
		Order:          order,
		ShowAll:        showAll,
		LastScrapeTime: getLastScrapeTime(),
	}
	tmpl.Execute(w, data)
}

// activityHandler serves the new page for recent market activity
func activityHandler(w http.ResponseWriter, r *http.Request) {
	eventRows, err := db.Query(`
        SELECT event_timestamp, event_type, item_name, item_id, details
        FROM market_events
        ORDER BY event_timestamp DESC
        LIMIT 200`) // Show more events on the dedicated page
	if err != nil {
		http.Error(w, "Could not query for market events", http.StatusInternalServerError)
		log.Printf("❌ Could not query for market events: %v", err)
		return
	}
	defer eventRows.Close()

	var marketEvents []MarketEvent
	for eventRows.Next() {
		var event MarketEvent
		var detailsStr, timestampStr string
		if err := eventRows.Scan(&timestampStr, &event.EventType, &event.ItemName, &event.ItemID, &detailsStr); err != nil {
			log.Printf("⚠️ Failed to scan market event row: %v", err)
			continue
		}

		parsedTime, err := time.Parse(time.RFC3339, timestampStr)
		if err == nil {
			event.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			event.Timestamp = timestampStr
		}

		if err := json.Unmarshal([]byte(detailsStr), &event.Details); err != nil {
			event.Details = make(map[string]interface{})
		}
		marketEvents = append(marketEvents, event)
	}

	tmpl, err := template.ParseFiles("activity.html")
	if err != nil {
		http.Error(w, "Could not load activity template", http.StatusInternalServerError)
		log.Printf("❌ Could not load activity.html template: %v", err)
		return
	}

	data := ActivityPageData{
		MarketEvents:   marketEvents,
		LastScrapeTime: getLastScrapeTime(),
	}
	tmpl.Execute(w, data)
}

func scrapeRMSItemDetails(itemID int) (*RMSItem, error) {
	url := fmt.Sprintf("https://ratemyserver.net/item_db.php?item_id=%d", itemID)
	// Use a client with a timeout
	client := http.Client{Timeout: 10 * time.Second}
	res, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get URL: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	item := &RMSItem{
		ID:       itemID,
		ImageURL: fmt.Sprintf("https://divine-pride.net/img/items/collection/iRO/%d", itemID),
	}

	// Get Item Name
	item.Name = strings.TrimSpace(doc.Find("div.main_block b").First().Text())

	// Get Item Properties from the info grid
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

	// Get Description
	item.Description = strings.TrimSpace(doc.Find("th:contains('Description')").Next().Find("div.longtext").Text())
	if item.Description == "" { // Fallback for items without a longtext div
		item.Description = strings.TrimSpace(doc.Find("th:contains('Description')").Next().Text())
	}

	// Get Item Script
	item.Script = strings.TrimSpace(doc.Find("th:contains('Item Script')").Next().Find("div.db_script_txt").Text())

	// Get Dropped By
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

	// Get Obtainable From
	doc.Find("th:contains('Obtainable From')").Next().Find("a").Each(func(i int, s *goquery.Selection) {
		item.ObtainableFrom = append(item.ObtainableFrom, strings.TrimSpace(s.Text()))
	})

	return item, nil
}

// ---- START: NEW CACHING FUNCTIONS ----

// getItemDetailsFromCache tries to fetch item details from the local DB cache.
func getItemDetailsFromCache(itemID int) (*RMSItem, error) {
	row := db.QueryRow(`
		SELECT name, image_url, item_type, item_class, buy, sell, weight, prefix, description, script, dropped_by_json, obtainable_from_json
		FROM rms_item_cache WHERE item_id = ?`, itemID)

	var item RMSItem
	item.ID = itemID
	var droppedByJSON, obtainableFromJSON string

	err := row.Scan(
		&item.Name, &item.ImageURL, &item.Type, &item.Class, &item.Buy, &item.Sell,
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
		(item_id, name, image_url, item_type, item_class, buy, sell, weight, prefix, description, script, dropped_by_json, obtainable_from_json, last_checked)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.ID, item.Name, item.ImageURL, item.Type, item.Class, item.Buy, item.Sell,
		item.Weight, item.Prefix, item.Description, item.Script,
		string(droppedByJSON), string(obtainableFromJSON), time.Now().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("failed to execute insert/replace for item %d in cache: %w", item.ID, err)
	}
	return nil
}

// ---- END: NEW CACHING FUNCTIONS ----

// ---- START: MODIFIED historyHandler ----
func historyHandler(w http.ResponseWriter, r *http.Request) {
	itemName := r.FormValue("name")
	if itemName == "" {
		http.Error(w, "Item name is required", http.StatusBadRequest)
		return
	}

	var itemID int
	err := db.QueryRow("SELECT item_id FROM items WHERE name_of_the_item = ? AND item_id > 0 LIMIT 1", itemName).Scan(&itemID)
	if err != nil {
		log.Printf("⚠️ Could not find a valid ItemID for '%s' in the database: %v", itemName, err)
	}

	var rmsItemDetails *RMSItem
	if itemID > 0 {
		// 1. Try to get details from the cache first.
		cachedItem, err := getItemDetailsFromCache(itemID)
		if err == nil {
			log.Printf("✅ Cache HIT for item ID %d (%s)", itemID, itemName)
			rmsItemDetails = cachedItem
		} else {
			// 2. If cache miss, scrape from the source.
			log.Printf("ℹ️ Cache MISS for item ID %d (%s). Scraping RMS... Error: %v", itemID, itemName, err)
			scrapedItem, scrapeErr := scrapeRMSItemDetails(itemID)
			if scrapeErr != nil {
				log.Printf("⚠️ Failed to scrape RateMyServer for item ID %d: %v", itemID, scrapeErr)
			} else {
				rmsItemDetails = scrapedItem
				// 3. Save the newly scraped data to the cache for future requests.
				if saveErr := saveItemDetailsToCache(rmsItemDetails); saveErr != nil {
					log.Printf("⚠️ Failed to save item ID %d to cache: %v", itemID, saveErr)
				} else {
					log.Printf("✅ Saved item ID %d (%s) to cache.", itemID, itemName)
				}
			}
		}
	}
	// The rest of the function proceeds normally with `rmsItemDetails`
	// whether it came from the cache or a fresh scrape.

	currentListingsQuery := `
		SELECT
			CAST(REPLACE(price, ',', '') AS INTEGER) as price_int,
			quantity,
			store_name,
			seller_name,
			map_name,
			map_coordinates,
			date_and_time_retrieved
		FROM items
		WHERE name_of_the_item = ? AND is_available = 1
		ORDER BY price_int ASC;
	`
	rowsCurrent, err := db.Query(currentListingsQuery, itemName)
	if err != nil {
		http.Error(w, "Database query for current listings failed", http.StatusInternalServerError)
		log.Printf("❌ Current listings query error: %v", err)
		return
	}
	defer rowsCurrent.Close()

	var currentListings []ItemListing
	for rowsCurrent.Next() {
		var listing ItemListing
		var timestampStr string
		if err := rowsCurrent.Scan(&listing.Price, &listing.Quantity, &listing.StoreName, &listing.SellerName, &listing.MapName, &listing.MapCoordinates, &timestampStr); err != nil {
			log.Printf("⚠️ Failed to scan current listing row: %v", err)
			continue
		}
		parsedTime, err := time.Parse(time.RFC3339, timestampStr)
		if err == nil {
			listing.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			listing.Timestamp = timestampStr
		}
		currentListings = append(currentListings, listing)
	}

	var currentLowest, currentHighest *ItemListing
	if len(currentListings) > 0 {
		lowestListing := currentListings[0]
		currentLowest = &lowestListing
		highestListing := currentListings[len(currentListings)-1]
		currentHighest = &highestListing
	}

	currentLowestJSON, _ := json.Marshal(currentLowest)
	currentHighestJSON, _ := json.Marshal(currentHighest)

	var overallLowest, overallHighest sql.NullInt64
	overallStatsQuery := `
        SELECT
            MIN(CAST(REPLACE(price, ',', '') AS INTEGER)),
            MAX(CAST(REPLACE(price, ',', '') AS INTEGER))
        FROM items
        WHERE name_of_the_item = ?;
    `
	err = db.QueryRow(overallStatsQuery, itemName).Scan(&overallLowest, &overallHighest)
	if err != nil {
		log.Printf("❌ Overall stats query error for '%s': %v", itemName, err)
	}

	priceChangeQuery := `
		WITH RankedItems AS (
			SELECT
				quantity,
				CAST(REPLACE(price, ',', '') AS INTEGER) as price_int,
				store_name,
				seller_name,
				date_and_time_retrieved,
				map_name,
				map_coordinates,
				ROW_NUMBER() OVER(PARTITION BY date_and_time_retrieved ORDER BY CAST(REPLACE(price, ',', '') AS INTEGER) ASC) as rn_asc,
				ROW_NUMBER() OVER(PARTITION BY date_and_time_retrieved ORDER BY CAST(REPLACE(price, ',', '') AS INTEGER) DESC) as rn_desc
			FROM items
			WHERE name_of_the_item = ?
		)
		SELECT
			t_lowest.date_and_time_retrieved,
			t_lowest.price_int, t_lowest.quantity, t_lowest.store_name, t_lowest.seller_name, t_lowest.map_name, t_lowest.map_coordinates,
			t_highest.price_int, t_highest.quantity, t_highest.store_name, t_highest.seller_name, t_highest.map_name, t_highest.map_coordinates
		FROM
			(SELECT * FROM RankedItems WHERE rn_asc = 1) AS t_lowest
		JOIN
			(SELECT * FROM RankedItems WHERE rn_desc = 1) AS t_highest
		ON
			t_lowest.date_and_time_retrieved = t_highest.date_and_time_retrieved
		ORDER BY
			t_lowest.date_and_time_retrieved ASC;
    `
	rows, err := db.Query(priceChangeQuery, itemName)
	if err != nil {
		http.Error(w, "Database query for changes failed", http.StatusInternalServerError)
		log.Printf("❌ History change query error: %v", err)
		return
	}
	defer rows.Close()

	priceEvents := make(map[string]PricePointDetails)
	for rows.Next() {
		var p PricePointDetails
		var timestampStr string
		err := rows.Scan(
			&timestampStr,
			&p.LowestPrice, &p.LowestQuantity, &p.LowestStoreName, &p.LowestSellerName, &p.LowestMapName, &p.LowestMapCoords,
			&p.HighestPrice, &p.HighestQuantity, &p.HighestStoreName, &p.HighestSellerName, &p.HighestMapName, &p.HighestMapCoords,
		)
		if err != nil {
			log.Printf("⚠️ Failed to scan history row: %v", err)
			continue
		}
		priceEvents[timestampStr] = p
	}

	scrapeHistoryRows, err := db.Query("SELECT timestamp FROM scrape_history ORDER BY timestamp ASC;")
	if err != nil {
		http.Error(w, "Database query for scrape history failed", http.StatusInternalServerError)
		return
	}
	defer scrapeHistoryRows.Close()

	var allScrapeTimes []string
	for scrapeHistoryRows.Next() {
		var ts string
		if err := scrapeHistoryRows.Scan(&ts); err != nil {
			continue
		}
		allScrapeTimes = append(allScrapeTimes, ts)
	}

	var fullPriceHistory []PricePointDetails
	var lastKnownDetails PricePointDetails
	var detailsInitialized bool

	for _, scrapeTimeStr := range allScrapeTimes {
		if event, ok := priceEvents[scrapeTimeStr]; ok {
			lastKnownDetails = event
			detailsInitialized = true
		}

		if detailsInitialized {
			t, _ := time.Parse(time.RFC3339, scrapeTimeStr)
			currentPoint := lastKnownDetails
			currentPoint.Timestamp = t.Format("2006-01-02 15:04")
			fullPriceHistory = append(fullPriceHistory, currentPoint)
		}
	}

	var finalPriceHistory []PricePointDetails
	if len(fullPriceHistory) > 0 {
		finalPriceHistory = append(finalPriceHistory, fullPriceHistory[0])
		for i := 1; i < len(fullPriceHistory); i++ {
			prev := finalPriceHistory[len(finalPriceHistory)-1]
			curr := fullPriceHistory[i]
			if prev.LowestPrice != curr.LowestPrice || prev.HighestPrice != curr.HighestPrice {
				finalPriceHistory = append(finalPriceHistory, curr)
			}
		}
	}

	priceHistoryJSON, err := json.Marshal(finalPriceHistory)
	if err != nil {
		http.Error(w, "Failed to create chart data", http.StatusInternalServerError)
		return
	}

	allListingsQuery := `
		SELECT
			price,
			quantity,
			store_name,
			seller_name,
			map_name,
			map_coordinates,
			date_and_time_retrieved,
            is_available
		FROM items
		WHERE name_of_the_item = ?
		ORDER BY is_available DESC, date_and_time_retrieved DESC;
	`
	rowsAll, err := db.Query(allListingsQuery, itemName)
	if err != nil {
		http.Error(w, "Database query for all listings failed", http.StatusInternalServerError)
		log.Printf("❌ All listings query error: %v", err)
		return
	}
	defer rowsAll.Close()

	var allListings []Item
	for rowsAll.Next() {
		var listing Item
		var timestampStr string
		if err := rowsAll.Scan(&listing.Price, &listing.Quantity, &listing.StoreName, &listing.SellerName, &listing.MapName, &listing.MapCoordinates, &timestampStr, &listing.IsAvailable); err != nil {
			log.Printf("⚠️ Failed to scan all listing row: %v", err)
			continue
		}
		parsedTime, err := time.Parse(time.RFC3339, timestampStr)
		if err == nil {
			listing.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			listing.Timestamp = timestampStr
		}
		allListings = append(allListings, listing)
	}

	tmpl, err := template.ParseFiles("history.html")
	if err != nil {
		http.Error(w, "Could not load history template", http.StatusInternalServerError)
		return
	}

	data := HistoryPageData{
		ItemName:           itemName,
		PriceDataJSON:      template.JS(priceHistoryJSON),
		OverallLowest:      int(overallLowest.Int64),
		OverallHighest:     int(overallHighest.Int64),
		CurrentLowestJSON:  template.JS(currentLowestJSON),
		CurrentHighestJSON: template.JS(currentHighestJSON),
		ItemDetails:        rmsItemDetails,
		AllListings:        allListings,
		LastScrapeTime:     getLastScrapeTime(),
	}
	tmpl.Execute(w, data)
}

// ---- END: MODIFIED historyHandler ----

// fullListHandler shows the complete, detailed market list
func fullListHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	searchQuery := r.FormValue("query")
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")
	selectedCols := r.Form["cols"]

	// Default to "only available" unless a form was submitted with the box unchecked.
	formSubmitted := len(r.Form) > 0
	showAll := false
	if formSubmitted && r.FormValue("only_available") != "true" {
		showAll = true
	}

	allCols := []Column{
		{ID: "item_id", DisplayName: "Item ID"},
		{ID: "quantity", DisplayName: "Quantity"},
		{ID: "store_name", DisplayName: "Store Name"},
		{ID: "seller_name", DisplayName: "Seller Name"},
		{ID: "map_name", DisplayName: "Map Name"},
		{ID: "map_coordinates", DisplayName: "Map Coords"},
		{ID: "retrieved", DisplayName: "Date Retrieved"},
		{ID: "availability", DisplayName: "Availability"},
	}
	visibleColumns := make(map[string]bool)
	columnParams := url.Values{}

	if len(selectedCols) > 0 {
		for _, col := range selectedCols {
			visibleColumns[col] = true
			columnParams.Add("cols", col)
		}
	} else {
		visibleColumns["quantity"] = true
		visibleColumns["store_name"] = true
		visibleColumns["map_coordinates"] = true
	}

	allowedSorts := map[string]string{
		"name":         "name_of_the_item",
		"item_id":      "item_id",
		"quantity":     "quantity",
		"price":        "CAST(REPLACE(price, ',', '') AS INTEGER)",
		"store":        "store_name",
		"seller":       "seller_name",
		"retrieved":    "date_and_time_retrieved",
		"store_name":   "store_name",
		"map_name":     "map_name",
		"availability": "is_available",
	}

	orderByClause, ok := allowedSorts[sortBy]
	if !ok {
		orderByClause, sortBy = "name_of_the_item", "name"
	}
	if strings.ToUpper(order) != "DESC" {
		order = "ASC"
	}

	whereClause := "WHERE name_of_the_item LIKE ?"
	if !showAll {
		whereClause += " AND is_available = 1"
	}

	query := fmt.Sprintf(`
		SELECT id, name_of_the_item, item_id, quantity, price, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates, is_available
		FROM items 
		%s 
		ORDER BY %s %s;`, whereClause, orderByClause, order)

	rows, err := db.Query(query, "%"+searchQuery+"%")
	if err != nil {
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		log.Printf("❌ Database query error: %v", err)
		return
	}
	defer rows.Close()

	var items []Item
	for rows.Next() {
		var item Item
		var retrievedTime string
		err := rows.Scan(&item.ID, &item.Name, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &retrievedTime, &item.MapName, &item.MapCoordinates, &item.IsAvailable)
		if err != nil {
			log.Printf("⚠️ Failed to scan row: %v", err)
			continue
		}
		parsedTime, err := time.Parse(time.RFC3339, retrievedTime)
		if err == nil {
			item.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			item.Timestamp = retrievedTime
		}
		items = append(items, item)
	}
	funcMap := template.FuncMap{
		"lower": strings.ToLower,
	}

	tmpl, err := template.New("full_list.html").Funcs(funcMap).ParseFiles("full_list.html")
	if err != nil {
		http.Error(w, "Could not load full_list template", http.StatusInternalServerError)
		log.Printf("❌ Could not load full_list.html template: %v", err)
		return
	}

	data := PageData{
		Items:          items,
		SearchQuery:    searchQuery,
		SortBy:         sortBy,
		Order:          order,
		ShowAll:        showAll,
		LastScrapeTime: getLastScrapeTime(),
		VisibleColumns: visibleColumns,
		AllColumns:     allCols,
		ColumnParams:   template.URL(columnParams.Encode()),
	}
	tmpl.Execute(w, data)
}

func startBackgroundScraper() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	go scrapeData()
	for {
		log.Printf("🕒 Waiting for the next 5-minute schedule...")
		<-ticker.C
		scrapeData()
	}
}

func areItemSetsIdentical(setA, setB []Item) bool {
	if len(setA) != len(setB) {
		return false
	}
	makeComparable := func(items []Item) []comparableItem {
		comp := make([]comparableItem, len(items))
		for i, item := range items {
			comp[i] = comparableItem{
				Name:           item.Name,
				ItemID:         item.ItemID,
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
			var itemID int
			err := tx.QueryRow("SELECT item_id FROM items WHERE name_of_the_item = ? AND item_id > 0 LIMIT 1", name).Scan(&itemID)
			if err != nil {
				log.Printf("⚠️ Could not find item_id for removed item '%s', logging event with item_id 0: %v", name, err)
				itemID = 0
			}

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

// ---- START: MODIFIED initDB ----
func initDB(filepath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		return nil, err
	}
	createItemsTableSQL := `
	CREATE TABLE IF NOT EXISTS items (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"name_of_the_item" TEXT,
		"item_id" INTEGER,
		"quantity" INTEGER,
		"price" TEXT,
		"store_name" TEXT,
		"seller_name" TEXT,
		"date_and_time_retrieved" TEXT,
		"map_name" TEXT,
		"map_coordinates" TEXT,
		"is_available" INTEGER DEFAULT 1
	);`
	if _, err = db.Exec(createItemsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create items table: %w", err)
	}

	createEventsTableSQL := `
	CREATE TABLE IF NOT EXISTS market_events (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"event_timestamp" TEXT NOT NULL,
		"event_type" TEXT NOT NULL,
		"item_name" TEXT NOT NULL,
		"item_id" INTEGER,
		"details" TEXT
	);`
	if _, err = db.Exec(createEventsTableSQL); err != nil {
		return nil, fmt.Errorf("could not create market_events table: %w", err)
	}

	createHistoryTableSQL := `
	CREATE TABLE IF NOT EXISTS scrape_history (
		"timestamp" TEXT NOT NULL PRIMARY KEY
	);`
	if _, err = db.Exec(createHistoryTableSQL); err != nil {
		return nil, fmt.Errorf("could not create scrape_history table: %w", err)
	}

	// NEW: Create the cache table for RateMyServer data.
	createRMSCacheTableSQL := `
	CREATE TABLE IF NOT EXISTS rms_item_cache (
		"item_id" INTEGER NOT NULL PRIMARY KEY,
		"name" TEXT,
		"image_url" TEXT,
		"item_type" TEXT,
		"item_class" TEXT,
		"buy" TEXT,
		"sell" TEXT,
		"weight" TEXT,
		"prefix" TEXT,
		"description" TEXT,
		"script" TEXT,
		"dropped_by_json" TEXT,
		"obtainable_from_json" TEXT,
		"last_checked" TEXT
	);`
	if _, err = db.Exec(createRMSCacheTableSQL); err != nil {
		return nil, fmt.Errorf("could not create rms_item_cache table: %w", err)
	}

	return db, nil
}

// ---- END: MODIFIED initDB ----

// scrapeAndCacheItemIfNotExists checks if an item is cached, and if not, scrapes and saves its details.
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

// ---- START: NEW STARTUP FUNCTION ----

// populateMissingCachesOnStartup verifies that all unique items in the database have a cache entry.
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

// ---- END: NEW STARTUP FUNCTION ----
