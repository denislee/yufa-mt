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

type HistoryPageData struct {
	ItemName      string
	PriceDataJSON template.JS
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
		ItemName:      itemName,
		PriceDataJSON: template.JS(priceHistoryJSON),
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
	showAll := r.FormValue("show_all") == "true"
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
		"name":            "name_of_the_item",
		"item_id":         "item_id",
		"quantity":        "quantity",
		"price":           "CAST(REPLACE(price, ',', '') AS INTEGER)",
		"store":           "store_name",
		"seller":          "seller_name",
		"retrieved":       "date_and_time_retrieved",
		"store_name":      "store_name",
		"map_name":        "map_name",
		"map_coordinates": "map_coordinates",
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
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	go scrapeData()
	for {
		log.Printf("🕒 Waiting for the next hourly schedule...")
		<-ticker.C
		scrapeData()
	}
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
	itemsSaved := 0
	itemsUpdated := 0
	itemsChecked := 0
	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	if _, err := tx.Exec("UPDATE items SET is_available = 0 WHERE is_available = 1"); err != nil {
		log.Printf("❌ Failed to mark old items as unavailable: %v", err)
		return
	}
	_, err = tx.Exec("INSERT OR IGNORE INTO scrape_history (timestamp) VALUES (?)", retrievalTime)
	if err != nil {
		log.Printf("❌ Failed to log scrape history: %v", err)
		return
	}
	insertSQL := `INSERT INTO items(name_of_the_item, item_id, quantity, price, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates, is_available) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Printf("⚠️ Could not prepare insert statement: %v", err)
		return
	}
	defer stmt.Close()

	doc.Find(`div[data-slot="card"]`).Each(func(i int, s *goquery.Selection) {
		shopName := strings.TrimSpace(s.Find(`div[data-slot="card-title"]`).Text())
		sellerName := strings.TrimSpace(s.Find("svg.lucide-user").Next().Text())
		mapName := strings.TrimSpace(s.Find("svg.lucide-map-pin").Next().Text())
		mapCoordinates := strings.TrimSpace(s.Find("svg.lucide-copy").Next().Text())

		s.Find(`div[data-slot="card-content"] .flex.items-center.space-x-2`).Each(func(j int, itemSelection *goquery.Selection) {
			itemsChecked++
			itemName := strings.TrimSpace(itemSelection.Find("p.truncate").Text())
			quantityStr := strings.TrimSuffix(strings.TrimSpace(itemSelection.Find("span.text-xs.text-muted-foreground").Text()), "x")
			priceStr := strings.TrimSpace(itemSelection.Find("span.text-xs.font-medium.text-green-600").Text())
			idStr := strings.TrimPrefix(strings.TrimSpace(itemSelection.Find(`span[data-slot="badge"]`).First().Text()), "ID: ")
			quantity, _ := strconv.Atoi(quantityStr)
			if quantity == 0 {
				quantity = 1
			}
			itemID, _ := strconv.Atoi(idStr)

			if itemName == "" || priceStr == "" || shopName == "" {
				return
			}

			var existingID int
			findQuery := `
				SELECT id FROM items WHERE
				name_of_the_item = ? AND item_id = ? AND quantity = ? AND price = ? AND
				store_name = ? AND seller_name = ? AND map_name = ? AND map_coordinates = ? AND is_available = 0
				ORDER BY date_and_time_retrieved DESC LIMIT 1`
			err := tx.QueryRow(findQuery, itemName, itemID, quantity, priceStr, shopName, sellerName, mapName, mapCoordinates).Scan(&existingID)

			if err == nil {
				if _, err := tx.Exec("UPDATE items SET is_available = 1 WHERE id = ?", existingID); err != nil {
					log.Printf("⚠️ Could not update item %s as available: %v", itemName, err)
				} else {
					itemsUpdated++
				}
			} else if err == sql.ErrNoRows {
				if _, err := stmt.Exec(itemName, itemID, quantity, priceStr, shopName, sellerName, retrievalTime, mapName, mapCoordinates); err != nil {
					log.Printf("⚠️ Could not execute insert for %s: %v", itemName, err)
				} else {
					itemsSaved++
				}
			} else {
				log.Printf("❌ Error checking for existing item %s: %v", itemName, err)
			}
		})
	})

	if err := tx.Commit(); err != nil {
		log.Printf("❌ Failed to commit transaction: %v", err)
		return
	}
	log.Printf("✅ Scrape complete. Checked %d items. Saved %d new/changed items. Marked %d unchanged items as available.", itemsChecked, itemsSaved, itemsUpdated)
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
