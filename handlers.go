package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Define recurring server events.
var definedEvents = []EventDefinition{
	{
		Name:      "Battlegrounds",
		StartTime: "20:00",
		EndTime:   "21:00",
		Days:      []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday, time.Saturday, time.Sunday},
	},
	{
		Name:      "War of Emperium",
		StartTime: "22:00",
		EndTime:   "22:00",
		Days:      []time.Weekday{time.Sunday},
	},
}

// generateEventIntervals creates a list of event occurrences within a given time window,
// but only for days that have corresponding data points.
func generateEventIntervals(viewStart, viewEnd time.Time, events []EventDefinition, activeDates map[string]struct{}) []map[string]interface{} {
	var intervals []map[string]interface{}
	// Normalize to the start of the day to ensure the loop includes the first day.
	loc := viewStart.Location()
	currentDay := time.Date(viewStart.Year(), viewStart.Month(), viewStart.Day(), 0, 0, 0, 0, loc)

	for currentDay.Before(viewEnd) {
		// Check if the current day has any player data before generating event overlays.
		dateStr := currentDay.Format("2006-01-02")
		if _, exists := activeDates[dateStr]; !exists {
			// Move to the next day if no data exists for the current one.
			currentDay = currentDay.Add(24 * time.Hour)
			continue
		}

		for _, event := range events {
			isEventDay := false
			for _, dayOfWeek := range event.Days {
				if currentDay.Weekday() == dayOfWeek {
					isEventDay = true
					break
				}
			}

			if isEventDay {
				// Parse the event's start and end times for the current day.
				eventStartStr := fmt.Sprintf("%s %s", currentDay.Format("2006-01-02"), event.StartTime)
				eventEndStr := fmt.Sprintf("%s %s", currentDay.Format("2006-01-02"), event.EndTime)

				eventStart, err1 := time.ParseInLocation("2006-01-02 15:04", eventStartStr, loc)
				eventEnd, err2 := time.ParseInLocation("2006-01-02 15:04", eventEndStr, loc)

				if err1 != nil || err2 != nil {
					continue // Skip if times are invalid
				}

				// Add the event only if it overlaps with the user's selected view window.
				if eventStart.Before(viewEnd) && eventEnd.After(viewStart) {
					intervals = append(intervals, map[string]interface{}{
						"name":  event.Name,
						"start": eventStart.Format("2006-01-02 15:04"),
						"end":   eventEnd.Format("2006-01-02 15:04"),
					})
				}
			}
		}
		// Move to the next day.
		currentDay = currentDay.Add(24 * time.Hour)
	}
	return intervals
}

// mapItemTypeToTabData converts a full item type name to a struct with a short name and icon ID.
func mapItemTypeToTabData(typeName string) ItemTypeTab {
	tab := ItemTypeTab{FullName: typeName, ShortName: typeName, IconItemID: 909} // Default to Jellopy
	switch typeName {
	case "Ammunition":
		tab.ShortName = "Ammo"
		tab.IconItemID = 1750 // Arrow
	case "Armor":
		tab.ShortName = "Armor"
		tab.IconItemID = 2301 // Cotton Shirt
	case "Card":
		tab.ShortName = "Cards"
		tab.IconItemID = 4133 // Poring Card
	case "Delayed-Consumable":
		tab.ShortName = "Consume"
		tab.IconItemID = 610 // Blue Potion
	case "Healing Item":
		tab.ShortName = "Healing"
		tab.IconItemID = 501 // Red Potion
	case "Miscellaneous":
		tab.ShortName = "Misc"
		tab.IconItemID = 909 // Jellopy
	case "Monster Egg":
		tab.ShortName = "Eggs"
		tab.IconItemID = 9001 // Poring Egg
	case "Pet Armor":
		tab.ShortName = "Pet Gear"
		tab.IconItemID = 5183 // B.B. Cap
	case "Taming Item":
		tab.ShortName = "Taming"
		tab.IconItemID = 632 // Unripe Apple
	case "Usable Item":
		tab.ShortName = "Usable"
		tab.IconItemID = 601 // Fly Wing
	case "Weapon":
		tab.ShortName = "Weapons"
		tab.IconItemID = 1201 // Main Gauche
	}
	return tab
}

// summaryHandler serves the main summary page (renamed from viewHandler)
func summaryHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	// 1. Get parameters from the request
	searchQuery := r.FormValue("query")
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")
	selectedType := r.FormValue("type")

	// Default to "only available" unless a form was submitted with the box unchecked.
	formSubmitted := len(r.Form) > 0
	showAll := false
	if formSubmitted && r.FormValue("only_available") != "true" {
		showAll = true
	}

	// Get all unique item types for the tabs
	var itemTypes []ItemTypeTab
	typeRows, err := db.Query("SELECT DISTINCT item_type FROM rms_item_cache WHERE item_type IS NOT NULL AND item_type != '' ORDER BY item_type ASC")
	if err != nil {
		log.Printf("⚠️ Could not query for item types: %v", err)
	} else {
		defer typeRows.Close()
		for typeRows.Next() {
			var itemType string
			if err := typeRows.Scan(&itemType); err != nil {
				log.Printf("⚠️ Failed to scan item type: %v", err)
				continue
			}
			itemTypes = append(itemTypes, mapItemTypeToTabData(itemType))
		}
	}

	// 2. Build the query dynamically
	params := []interface{}{}
	baseQuery := `
        SELECT
            i.name_of_the_item,
            MIN(i.item_id) as item_id,
            MIN(CASE WHEN i.is_available = 1 THEN CAST(REPLACE(i.price, ',', '') AS INTEGER) ELSE NULL END) as lowest_price,
            MAX(CASE WHEN i.is_available = 1 THEN CAST(REPLACE(i.price, ',', '') AS INTEGER) ELSE NULL END) as highest_price,
            SUM(CASE WHEN i.is_available = 1 THEN 1 ELSE 0 END) as listing_count
        FROM items i
        LEFT JOIN rms_item_cache rms ON i.item_id = rms.item_id
    `

	var whereConditions []string
	if searchQuery != "" {
		whereConditions = append(whereConditions, "i.name_of_the_item LIKE ?")
		params = append(params, "%"+searchQuery+"%")
	}

	if selectedType != "" {
		whereConditions = append(whereConditions, "rms.item_type = ?")
		params = append(params, selectedType)
	}

	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	groupByClause := " GROUP BY i.name_of_the_item"

	havingClause := ""
	if !showAll {
		havingClause = " HAVING listing_count > 0"
	}

	// 3. Handle sorting securely
	allowedSorts := map[string]string{
		"name":          "i.name_of_the_item",
		"item_id":       "item_id",
		"listings":      "listing_count",
		"lowest_price":  "lowest_price",
		"highest_price": "highest_price",
	}
	orderByClause, ok := allowedSorts[sortBy]
	if !ok {
		orderByClause, sortBy = "i.name_of_the_item", "name" // Default sort
	}
	if strings.ToUpper(order) != "DESC" {
		order = "ASC"
	}

	// Append ORDER BY to the query, with a secondary sort for stability
	query := fmt.Sprintf("%s %s %s %s ORDER BY %s %s, i.name_of_the_item ASC;", baseQuery, whereClause, groupByClause, havingClause, orderByClause, order)

	rows, err := db.Query(query, params...)
	if err != nil {
		http.Error(w, "Database query for summary failed", http.StatusInternalServerError)
		log.Printf("❌ Summary query error: %v, Query: %s, Params: %v", err, query, params)
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
		ItemTypes:      itemTypes,
		SelectedType:   selectedType,
	}
	tmpl.Execute(w, data)
}

// fullListHandler shows the complete, detailed market list.
func fullListHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	searchQuery := r.FormValue("query")
	storeNameQuery := r.FormValue("store_name")
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")
	selectedCols := r.Form["cols"]
	selectedType := r.FormValue("type")

	// Default to "only available" unless a form was submitted with the box unchecked.
	formSubmitted := len(r.Form) > 0
	showAll := false
	if formSubmitted && r.FormValue("only_available") != "true" {
		showAll = true
	}

	// Get all unique item types for the tabs
	var itemTypes []ItemTypeTab
	typeRows, err := db.Query("SELECT DISTINCT item_type FROM rms_item_cache WHERE item_type IS NOT NULL AND item_type != '' ORDER BY item_type ASC")
	if err != nil {
		log.Printf("⚠️ Could not query for item types: %v", err)
	} else {
		defer typeRows.Close()
		for typeRows.Next() {
			var itemType string
			if err := typeRows.Scan(&itemType); err != nil {
				log.Printf("⚠️ Failed to scan item type: %v", err)
				continue
			}
			itemTypes = append(itemTypes, mapItemTypeToTabData(itemType))
		}
	}

	// Get all unique store names for the dropdown
	var allStoreNames []string
	storeRows, err := db.Query("SELECT DISTINCT store_name FROM items WHERE is_available = 1 ORDER BY store_name ASC")
	if err != nil {
		// Log the error but don't fail the whole page request
		log.Printf("⚠️ Could not query for store names: %v", err)
	} else {
		defer storeRows.Close()
		for storeRows.Next() {
			var storeName string
			if err := storeRows.Scan(&storeName); err != nil {
				log.Printf("⚠️ Failed to scan store name: %v", err)
				continue
			}
			allStoreNames = append(allStoreNames, storeName)
		}
	}

	allCols := []Column{
		{ID: "item_id", DisplayName: "Item ID"},
		{ID: "quantity", DisplayName: "Quantity"},
		{ID: "store_name", DisplayName: "Store Name"},
		{ID: "seller_name", DisplayName: "Seller Name"},
		{ID: "map_name", DisplayName: "Map Name"},
		{ID: "map_coordinates", DisplayName: "Map Coords"},
		{ID: "retrieved", DisplayName: "Date Retrieved"},
		//		{ID: "availability", DisplayName: "Availability"},
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
		"name":         "i.name_of_the_item",
		"item_id":      "i.item_id",
		"quantity":     "i.quantity",
		"price":        "CAST(REPLACE(i.price, ',', '') AS INTEGER)",
		"store":        "i.store_name",
		"seller":       "i.seller_name",
		"retrieved":    "i.date_and_time_retrieved",
		"store_name":   "i.store_name",
		"map_name":     "i.map_name",
		"availability": "i.is_available",
	}

	orderByClause, ok := allowedSorts[sortBy]
	if !ok {
		orderByClause, sortBy = "i.name_of_the_item", "name"
	}
	if strings.ToUpper(order) != "DESC" {
		order = "ASC"
	}

	var whereConditions []string
	var queryParams []interface{}

	if searchQuery != "" {
		whereConditions = append(whereConditions, "i.name_of_the_item LIKE ?")
		queryParams = append(queryParams, "%"+searchQuery+"%")
	}

	if storeNameQuery != "" {
		// Changed from LIKE to = for an exact, case-sensitive match.
		whereConditions = append(whereConditions, "i.store_name = ?")
		// Removed wildcards from the parameter for the exact match.
		queryParams = append(queryParams, storeNameQuery)
	}

	if selectedType != "" {
		whereConditions = append(whereConditions, "rms.item_type = ?")
		queryParams = append(queryParams, selectedType)
	}

	if !showAll {
		whereConditions = append(whereConditions, "i.is_available = 1")
	}

	baseQuery := `
		SELECT i.id, i.name_of_the_item, i.item_id, i.quantity, i.price, i.store_name, i.seller_name, i.date_and_time_retrieved, i.map_name, i.map_coordinates, i.is_available
		FROM items i 
		LEFT JOIN rms_item_cache rms ON i.item_id = rms.item_id
	`
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	query := fmt.Sprintf(`%s %s ORDER BY %s %s;`, baseQuery, whereClause, orderByClause, order)

	rows, err := db.Query(query, queryParams...)
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
		StoreNameQuery: storeNameQuery,
		AllStoreNames:  allStoreNames,
		SortBy:         sortBy,
		Order:          order,
		ShowAll:        showAll,
		LastScrapeTime: getLastScrapeTime(),
		VisibleColumns: visibleColumns,
		AllColumns:     allCols,
		ColumnParams:   template.URL(columnParams.Encode()),
		ItemTypes:      itemTypes,
		SelectedType:   selectedType,
	}
	tmpl.Execute(w, data)
}

// activityHandler serves the page for recent market activity.
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

// itemHistoryHandler serves the detailed history page for a single item (renamed from historyHandler)
func itemHistoryHandler(w http.ResponseWriter, r *http.Request) {
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
			// Format for display
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

// In handlers.go

// ... add this new handler function ...

// playerCountHandler serves the page with a graph of online player history.
func playerCountHandler(w http.ResponseWriter, r *http.Request) {
	interval := r.URL.Query().Get("interval")
	if interval == "" {
		interval = "7d" // Default to 7 days
	}

	var whereClause string
	var params []interface{}
	now := time.Now()
	var viewStart time.Time

	switch interval {
	case "30m":
		viewStart = now.Add(-30 * time.Minute)
	case "6h":
		viewStart = now.Add(-6 * time.Hour)
	case "24h":
		viewStart = now.Add(-24 * time.Hour)
	case "7d":
		viewStart = now.Add(-7 * 24 * time.Hour)
	case "30d":
		viewStart = now.Add(-30 * 24 * time.Hour)
	default:
		interval = "7d" // Fallback to default if an invalid value is passed
		viewStart = now.Add(-7 * 24 * time.Hour)
	}
	whereClause = "WHERE timestamp >= ?"
	params = append(params, viewStart.Format(time.RFC3339))

	query := fmt.Sprintf("SELECT timestamp, count, seller_count FROM player_history %s ORDER BY timestamp ASC", whereClause)
	rows, err := db.Query(query, params...)
	if err != nil {
		http.Error(w, "Could not query for player history", http.StatusInternalServerError)
		log.Printf("❌ Could not query for player history: %v", err)
		return
	}
	defer rows.Close()

	var playerHistory []PlayerCountPoint
	// Create a set of dates that have player data to filter event generation.
	activeDatesWithData := make(map[string]struct{})

	for rows.Next() {
		var point PlayerCountPoint
		var timestampStr string
		if err := rows.Scan(&timestampStr, &point.Count, &point.SellerCount); err != nil {
			log.Printf("⚠️ Failed to scan player history row: %v", err)
			continue
		}
		// Calculate the delta between total players and sellers.
		point.Delta = point.Count - point.SellerCount

		parsedTime, err := time.Parse(time.RFC3339, timestampStr)
		if err == nil {
			point.Timestamp = parsedTime.Format("2006-01-02 15:04")
			datePart := parsedTime.Format("2006-01-02")
			activeDatesWithData[datePart] = struct{}{}
		} else {
			point.Timestamp = timestampStr
		}
		playerHistory = append(playerHistory, point)
	}

	playerHistoryJSON, err := json.Marshal(playerHistory)
	if err != nil {
		http.Error(w, "Failed to create chart data", http.StatusInternalServerError)
		return
	}

	// Generate event intervals for the selected view, filtered by days with actual player data.
	eventIntervals := generateEventIntervals(viewStart, now, definedEvents, activeDatesWithData)
	eventIntervalsJSON, err := json.Marshal(eventIntervals)
	if err != nil {
		http.Error(w, "Failed to create event data", http.StatusInternalServerError)
		return
	}

	tmpl, err := template.ParseFiles("players.html")
	if err != nil {
		http.Error(w, "Could not load players template", http.StatusInternalServerError)
		log.Printf("❌ Could not load players.html template: %v", err)
		return
	}

	data := PlayerCountPageData{
		PlayerDataJSON:   template.JS(playerHistoryJSON),
		LastScrapeTime:   getLastScrapeTime(),
		SelectedInterval: interval,
		EventDataJSON:    template.JS(eventIntervalsJSON),
	}
	tmpl.Execute(w, data)
}
