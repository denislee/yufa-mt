package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

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

// PageData struct updated with column visibility info
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

type PricePointDetails struct {
	Timestamp     string `json:"Timestamp"`
	MinPrice      int    `json:"MinPrice"`
	MinQuantity   int    `json:"MinQuantity"`
	MinStoreName  string `json:"MinStoreName"`
	MinSellerName string `json:"MinSellerName"`
	MinMapName    string `json:"MinMapName"`
	MinMapCoords  string `json:"MinMapCoords"`
	MaxPrice      int    `json:"MaxPrice"`
	MaxQuantity   int    `json:"MaxQuantity"`
	MaxStoreName  string `json:"MaxStoreName"`
	MaxSellerName string `json:"MaxSellerName"`
	MaxMapName    string `json:"MaxMapName"`
	MaxMapCoords  string `json:"MaxMapCoords"`
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

// RagnaItem holds the detailed information fetched from RagnaAPI.
type RagnaItem struct {
	ID          int        `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	ImageURL    string     `json:"img"`
	DropRates   []DropRate `json:"drop_rate"`
}

// UnmarshalJSON is a custom unmarshaler for RagnaItem to handle inconsistent "drop_rate" types.
func (r *RagnaItem) UnmarshalJSON(data []byte) error {
	// Use an alias to avoid recursion
	type Alias RagnaItem

	// Create a temporary struct with a json.RawMessage for the drop_rate field
	aux := &struct {
		DropRates json.RawMessage `json:"drop_rate"`
		*Alias
	}{
		Alias: (*Alias)(r),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Try to unmarshal drop_rate as a slice.
	var dropRates []DropRate
	if err := json.Unmarshal(aux.DropRates, &dropRates); err == nil {
		// If successful (it's an array), assign it.
		r.DropRates = dropRates
	} else {
		// If it fails (e.g., it's a boolean `false`), set DropRates to nil.
		r.DropRates = nil
	}

	return nil
}

// DropRate holds monster drop information.
type DropRate struct {
	Monster      string `json:"monster"`
	Rate         string `json:"rate"`
	HighestSpawn string `json:"highest_spawn"`
	Element      string `json:"element"`
	Flee         string `json:"flee"`
	Hit          string `json:"hit"`
}

type HistoryPageData struct {
	ItemName       string
	PriceDataJSON  template.JS
	OverallMin     int
	OverallMax     int
	CurrentMinJSON template.JS
	CurrentMaxJSON template.JS
	ItemDetails    *RagnaItem
}

func main() {
	var err error
	db, err = initDB("./market_data.db")
	if err != nil {
		log.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer db.Close()

	go startBackgroundScraper()

	http.HandleFunc("/", viewHandler)
	http.HandleFunc("/item", historyHandler)

	port := "8080"
	log.Printf("🚀 Web server started. Open http://localhost:%s in your browser.", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("❌ Failed to start web server: %v", err)
	}
}

func historyHandler(w http.ResponseWriter, r *http.Request) {
	itemName := r.FormValue("name")
	if itemName == "" {
		http.Error(w, "Item name is required", http.StatusBadRequest)
		return
	}

	// Fetch item details from RagnaAPI
	var itemID int
	err := db.QueryRow("SELECT item_id FROM items WHERE name_of_the_item = ? AND item_id > 0 LIMIT 1", itemName).Scan(&itemID)
	if err != nil {
		log.Printf("⚠️ Could not find a valid ItemID for '%s' in the database: %v", itemName, err)
	}

	var ragnaItemDetails *RagnaItem
	if itemID > 0 {
		apiURL := fmt.Sprintf("https://ragnapi.com/api/v1/old-times/items/%d", itemID)
		client := http.Client{Timeout: 10 * time.Second}
		resp, err := client.Get(apiURL)
		if err != nil {
			log.Printf("❌ Failed to call RagnaAPI for item ID %d: %v", itemID, err)
		} else {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					log.Printf("❌ Failed to read RagnaAPI response body for item ID %d: %v", itemID, err)
				} else {
					var itemDetails RagnaItem
					// The custom UnmarshalJSON method on RagnaItem will be called here
					if err := json.Unmarshal(body, &itemDetails); err != nil {
						log.Printf("❌ Failed to unmarshal RagnaAPI JSON for item ID %d: %v", itemID, err)
					} else {
						ragnaItemDetails = &itemDetails
					}
				}
			} else {
				log.Printf("⚠️ RagnaAPI returned non-OK status for item ID %d: %s", itemID, resp.Status)
			}
		}
	}

	// Query for current available listings to find min and max for the cards
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

	var currentMin, currentMax *ItemListing
	if len(currentListings) > 0 {
		minListing := currentListings[0]
		currentMin = &minListing
		maxListing := currentListings[len(currentListings)-1]
		currentMax = &maxListing
	}

	currentMinJSON, _ := json.Marshal(currentMin)
	currentMaxJSON, _ := json.Marshal(currentMax)

	// ADDED: Query for overall min and max prices across all history
	var overallMin, overallMax sql.NullInt64
	overallStatsQuery := `
        SELECT
            MIN(CAST(REPLACE(price, ',', '') AS INTEGER)),
            MAX(CAST(REPLACE(price, ',', '') AS INTEGER))
        FROM items
        WHERE name_of_the_item = ?;
    `
	err = db.QueryRow(overallStatsQuery, itemName).Scan(&overallMin, &overallMax)
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
			t_min.date_and_time_retrieved,
			t_min.price_int, t_min.quantity, t_min.store_name, t_min.seller_name, t_min.map_name, t_min.map_coordinates,
			t_max.price_int, t_max.quantity, t_max.store_name, t_max.seller_name, t_max.map_name, t_max.map_coordinates
		FROM
			(SELECT * FROM RankedItems WHERE rn_asc = 1) AS t_min
		JOIN
			(SELECT * FROM RankedItems WHERE rn_desc = 1) AS t_max
		ON
			t_min.date_and_time_retrieved = t_max.date_and_time_retrieved
		ORDER BY
			t_min.date_and_time_retrieved ASC;
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
			&p.MinPrice, &p.MinQuantity, &p.MinStoreName, &p.MinSellerName, &p.MinMapName, &p.MinMapCoords,
			&p.MaxPrice, &p.MaxQuantity, &p.MaxStoreName, &p.MaxSellerName, &p.MaxMapName, &p.MaxMapCoords,
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
			if prev.MinPrice != curr.MinPrice || prev.MaxPrice != curr.MaxPrice {
				finalPriceHistory = append(finalPriceHistory, curr)
			}
		}
	}

	priceHistoryJSON, err := json.Marshal(finalPriceHistory)
	if err != nil {
		http.Error(w, "Failed to create chart data", http.StatusInternalServerError)
		return
	}

	tmpl, err := template.ParseFiles("history.html")
	if err != nil {
		http.Error(w, "Could not load history template", http.StatusInternalServerError)
		return
	}

	data := HistoryPageData{
		ItemName:       itemName,
		PriceDataJSON:  template.JS(priceHistoryJSON),
		OverallMin:     int(overallMin.Int64),
		OverallMax:     int(overallMax.Int64),
		CurrentMinJSON: template.JS(currentMinJSON),
		CurrentMaxJSON: template.JS(currentMaxJSON),
		ItemDetails:    ragnaItemDetails,
	}
	tmpl.Execute(w, data)
}

// viewHandler now manages column visibility
func viewHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	searchQuery := r.FormValue("query")
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")
	// Default to showing all items unless 'only_available' is checked.
	showAll := r.FormValue("only_available") != "true"
	selectedCols := r.Form["cols"]

	// Define all toggleable columns
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

	// If user submitted a preference, use it. Otherwise, use defaults.
	if len(selectedCols) > 0 {
		for _, col := range selectedCols {
			visibleColumns[col] = true
			columnParams.Add("cols", col)
		}
	} else {
		// Default columns
		visibleColumns["quantity"] = true
		visibleColumns["store_name"] = true
		visibleColumns["map_coordinates"] = true
	}

	// Get the last scrape time
	var lastScrapeTimestamp sql.NullString
	err := db.QueryRow("SELECT MAX(timestamp) FROM scrape_history").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last scrape time: %v", err)
	}
	var formattedLastScrapeTime string
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {
			formattedLastScrapeTime = parsedTime.Format("2006-01-02 15:04:05")
		}
	} else {
		formattedLastScrapeTime = "Never"
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

	tmpl, err := template.ParseFiles("index.html")
	if err != nil {
		http.Error(w, "Could not load template", http.StatusInternalServerError)
		return
	}

	data := PageData{
		Items:          items,
		SearchQuery:    searchQuery,
		SortBy:         sortBy,
		Order:          order,
		ShowAll:        showAll,
		LastScrapeTime: formattedLastScrapeTime,
		VisibleColumns: visibleColumns,
		AllColumns:     allCols,
		ColumnParams:   template.URL(columnParams.Encode()),
	}
	tmpl.Execute(w, data)
}

func startBackgroundScraper() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	go scrapeData()
	for {
		log.Printf("🕒 Waiting for the next 30-minute schedule...")
		<-ticker.C
		scrapeData()
	}
}

// areItemSetsIdentical checks if two slices of items are semantically identical,
// ignoring order and fields not relevant for comparison.
func areItemSetsIdentical(setA, setB []Item) bool {
	if len(setA) != len(setB) {
		return false
	}

	// Create comparable representations of each item
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

	// Use a map to count occurrences in setA
	counts := make(map[comparableItem]int)
	for _, item := range compA {
		counts[item]++
	}

	// Decrement counts for items in setB
	for _, item := range compB {
		if counts[item] == 0 {
			return false // Item in B not in A, or excess count
		}
		counts[item]--
	}

	// If all counts are zero, the sets are identical
	return true
}

func scrapeData() {
	log.Println("🚀 Starting scrape...")
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

	// Step 1: Scrape all items from the website and group them by name in memory.
	doc.Find(`div[data-slot="card"]`).Each(func(i int, s *goquery.Selection) {
		shopName := strings.TrimSpace(s.Find(`div[data-slot="card-title"]`).Text())
		sellerName := strings.TrimSpace(s.Find("svg.lucide-user").Next().Text())
		mapName := strings.TrimSpace(s.Find("svg.lucide-map-pin").Next().Text())
		mapCoordinates := strings.TrimSpace(s.Find("svg.lucide-copy").Next().Text())

		s.Find(`div[data-slot="card-content"] .flex.items-center.space-x-2`).Each(func(j int, itemSelection *goquery.Selection) {
			itemName := strings.TrimSpace(itemSelection.Find("p.truncate").Text())
			quantityStr := strings.TrimSuffix(strings.TrimSpace(itemSelection.Find("span.text-xs.text-muted-foreground").Text()), "x")
			priceStr := strings.TrimSpace(itemSelection.Find("span.text-xs.font-medium.text-green-600").Text())
			idStr := strings.TrimPrefix(strings.TrimSpace(itemSelection.Find(`span[data-slot="badge"]`).First().Text()), "ID: ")

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

	// Get a set of all item names currently marked as available in the DB
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

	// Step 2: For each item name scraped, compare with DB records.
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
			// No changes for this item name, do nothing.
			itemsUnchanged++
			continue
		}

		// There's a change. Mark all old versions as unavailable.
		if _, err := tx.Exec("UPDATE items SET is_available = 0 WHERE name_of_the_item = ?", itemName); err != nil {
			log.Printf("❌ Failed to mark old %s as unavailable: %v", itemName, err)
			continue
		}

		// Insert the new versions.
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
		} else {
			itemsUpdated++
		}
	}

	// Step 3: Mark any items that were in the DB but not in the new scrape as unavailable.
	itemsRemoved := 0
	for name := range dbAvailableNames {
		if _, foundInScrape := scrapedItemsByName[name]; !foundInScrape {
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
	createHistoryTableSQL := `
	CREATE TABLE IF NOT EXISTS scrape_history (
		"timestamp" TEXT NOT NULL PRIMARY KEY
	);`
	if _, err = db.Exec(createHistoryTableSQL); err != nil {
		return nil, fmt.Errorf("could not create scrape_history table: %w", err)
	}
	return db, nil
}

