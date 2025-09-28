package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

// CHANGED: Added MapName and MapCoordinates fields to the Item struct.
type Item struct {
	ID             int
	Name           string
	ItemID         int
	Quantity       int
	Price          string
	StoreName      string
	Timestamp      string
	MapName        string
	MapCoordinates string
}

type PageData struct {
	Items       []Item
	SearchQuery string
	SortBy      string
	Order       string
}

// PricePoint is for the graph data
type PricePoint struct {
	Timestamp string `json:"Timestamp"`
	MinPrice  int    `json:"MinPrice"`
	MaxPrice  int    `json:"MaxPrice"`
}

// HistoryPageData is for the item detail page
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

	query := `
        SELECT
            date_and_time_retrieved,
            MIN(CAST(REPLACE(price, ',', '') AS INTEGER)),
            MAX(CAST(REPLACE(price, ',', '') AS INTEGER))
        FROM items
        WHERE name_of_the_item = ?
        GROUP BY date_and_time_retrieved
        ORDER BY date_and_time_retrieved ASC;
    `
	rows, err := db.Query(query, itemName)
	if err != nil {
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		log.Printf("❌ History query error: %v", err)
		return
	}
	defer rows.Close()

	var priceHistory []PricePoint
	// CHANGED: Initialize variables to track the last price.
	var lastMinPrice, lastMaxPrice int = -1, -1 // Use -1 to ensure the first point is always added.

	for rows.Next() {
		var p PricePoint
		var timestampStr string
		if err := rows.Scan(&timestampStr, &p.MinPrice, &p.MaxPrice); err != nil {
			log.Printf("⚠️ Failed to scan history row: %v", err)
			continue
		}

		// CHANGED: Only add the data point if it's the first one or if the price has changed.
		if lastMinPrice == -1 || p.MinPrice != lastMinPrice || p.MaxPrice != lastMaxPrice {
			t, _ := time.Parse(time.RFC3339, timestampStr)
			p.Timestamp = t.Format("2006-01-02 15:04")
			priceHistory = append(priceHistory, p)

			// Update the last known prices
			lastMinPrice = p.MinPrice
			lastMaxPrice = p.MaxPrice
		}
	}

	priceHistoryJSON, err := json.Marshal(priceHistory)
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

// NOTE: To display the new map and coordinates, you'll need to update your 'index.html' template.
// The query and scan functions below are updated to fetch the data.
func viewHandler(w http.ResponseWriter, r *http.Request) {
	searchQuery := r.FormValue("query")
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")

	allowedSorts := map[string]string{
		"name": "name_of_the_item", "item_id": "item_id", "quantity": "quantity",
		"price": "CAST(REPLACE(price, ',', '') AS INTEGER)", "store": "store_name", "retrieved": "date_and_time_retrieved",
	}

	orderByClause, ok := allowedSorts[sortBy]
	if !ok {
		orderByClause, sortBy = "name_of_the_item", "name"
	}
	if strings.ToUpper(order) != "DESC" {
		order = "ASC"
	}

	// CHANGED: Added map_name and map_coordinates to the SELECT statement.
	query := fmt.Sprintf(`
		SELECT id, name_of_the_item, item_id, quantity, price, store_name, date_and_time_retrieved, map_name, map_coordinates
		FROM items 
		WHERE date_and_time_retrieved = (SELECT MAX(date_and_time_retrieved) FROM items) 
		AND name_of_the_item LIKE ? ORDER BY %s %s;`, orderByClause, order)

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
		// CHANGED: Added item.MapName and item.MapCoordinates to the Scan function.
		if err := rows.Scan(&item.ID, &item.Name, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &retrievedTime, &item.MapName, &item.MapCoordinates); err != nil {
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

	data := PageData{Items: items, SearchQuery: searchQuery, SortBy: sortBy, Order: order}
	tmpl.Execute(w, data)
}

func startBackgroundScraper() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	scrapeData()
	for {
		log.Printf("🕒 Waiting for the next hourly schedule...")
		<-ticker.C
		scrapeData()
	}
}
func scrapeData() {
	log.Println("🚀 Starting hourly scrape...")
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
		log.Printf("❌ Failed to run chromedp tasks this hour: %v", err)
		return
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("❌ Failed to parse HTML: %v", err)
		return
	}

	retrievalTime := time.Now().Format(time.RFC3339)
	itemsSaved := 0
	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// CHANGED: Updated SQL query to include new fields.
	insertSQL := `INSERT INTO items(name_of_the_item, item_id, quantity, price, store_name, date_and_time_retrieved, map_name, map_coordinates) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Printf("⚠️ Could not prepare insert statement: %v", err)
		return
	}
	defer stmt.Close()

	doc.Find(`div[data-slot="card"]`).Each(func(i int, s *goquery.Selection) {
		shopName := strings.TrimSpace(s.Find(`div[data-slot="card-title"]`).Text())

		// CHANGED: Extract map name and coordinates for each shop.
		mapName := strings.TrimSpace(s.Find("svg.lucide-map-pin").Next().Text())
		mapCoordinates := strings.TrimSpace(s.Find("svg.lucide-copy").Next().Text())

		s.Find(`div[data-slot="card-content"] .flex.items-center.space-x-2`).Each(func(j int, itemSelection *goquery.Selection) {
			itemName := strings.TrimSpace(itemSelection.Find("p.truncate").Text())
			quantityStr := strings.TrimSuffix(strings.TrimSpace(itemSelection.Find("span.text-xs.text-muted-foreground").Text()), "x")
			priceStr := strings.TrimSpace(itemSelection.Find("span.text-xs.font-medium.text-green-600").Text())
			idStr := strings.TrimPrefix(strings.TrimSpace(itemSelection.Find(`span[data-slot="badge"]`).First().Text()), "ID: ")
			quantity, _ := strconv.Atoi(quantityStr)
			if quantity == 0 {
				quantity = 1
			}
			itemID, _ := strconv.Atoi(idStr)

			if itemName != "" && priceStr != "" && shopName != "" {
				// CHANGED: Pass the new map data to the database insert.
				if _, err := stmt.Exec(itemName, itemID, quantity, priceStr, shopName, retrievalTime, mapName, mapCoordinates); err != nil {
					log.Printf("⚠️ Could not execute insert: %v", err)
				} else {
					itemsSaved++
				}
			}
		})
	})

	if err := tx.Commit(); err != nil {
		log.Printf("❌ Failed to commit transaction: %v", err)
		return
	}
	log.Printf("✅ Scrape complete. Saved %d items.", itemsSaved)
}
func initDB(filepath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		return nil, err
	}

	// CHANGED: Added map_name and map_coordinates columns to the table definition.
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS items (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		"name_of_the_item" TEXT,
		"item_id" INTEGER,
		"quantity" INTEGER,
		"price" TEXT,
		"store_name" TEXT,
		"date_and_time_retrieved" TEXT,
		"map_name" TEXT,
		"map_coordinates" TEXT
	);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return nil, err
	}
	return db, nil
}

