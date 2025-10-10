package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
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
		StartTime: "21:00",
		EndTime:   "22:00",
		Days:      []time.Weekday{time.Sunday},
	},
}

var mvpMobIDs = []string{
	"1038", "1039", "1046", "1059", "1086", "1087", "1112", "1115", "1147",
	"1150", "1157", "1159", "1190", "1251", "1252", "1272", "1312", "1373",
	"1389", "1418", "1492", "1511",
}

var mvpNames = map[string]string{
	"1038": "Osiris",
	"1039": "Baphomet",
	"1046": "Doppelganger",
	"1059": "Mistress",
	"1086": "Golden Thief Bug",
	"1087": "Orc Hero",
	"1112": "Drake",
	"1115": "Eddga",
	"1147": "Maya",
	"1150": "Moonlight Flower",
	"1157": "Pharaoh",
	"1159": "Phreeoni",
	"1190": "Orc Lord",
	"1251": "Stormy Knight",
	"1252": "Hatii",
	"1272": "Dark Lord",
	"1312": "Turtle General",
	"1373": "Lord of Death",
	"1389": "Dracula",
	"1418": "Evil Snake Lord",
	"1492": "Incantation Samurai",
	"1511": "Amon Ra",
}

// generateEventIntervals creates a list of event occurrences within a given time window,
// but only for days that have corresponding data points.
func generateEventIntervals(viewStart, viewEnd time.Time, events []EventDefinition, activeDates map[string]struct{}) []map[string]interface{} {
	var intervals []map[string]interface{}
	// Normalize to the start of the day to ensure the loop includes the first day.
	loc := viewStart.Location()
	currentDay := time.Date(viewStart.Year(), viewStart.Month(), viewStart.Day(), 0, 0, 0, 0, loc)

	for currentDay.Before(viewEnd) {
		// --- THIS CHECK IS NOW RESTORED ---
		// Check if the current day has any player data before generating event overlays.
		dateStr := currentDay.Format("2006-01-02")
		if _, exists := activeDates[dateStr]; !exists {
			// Move to the next day if no data exists for the current one.
			currentDay = currentDay.Add(24 * time.Hour)
			continue
		}
		// --- END OF RESTORED CHECK ---

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
		tab.ShortName = ""
		tab.IconItemID = 1750 // Arrow
	case "Armor":
		tab.ShortName = ""
		tab.IconItemID = 2316 // Cotton Shirt
	case "Card":
		tab.ShortName = ""
		tab.IconItemID = 4133 // Poring Card
	case "Delayed-Consumable":
		tab.ShortName = ""
		tab.IconItemID = 610 // Blue Potion
	case "Healing Item":
		tab.ShortName = ""
		tab.IconItemID = 501 // Red Potion
	case "Miscellaneous":
		tab.ShortName = ""
		tab.IconItemID = 909 // Jellopy
	case "Monster Egg":
		tab.ShortName = ""
		tab.IconItemID = 9001 // Poring Egg
	case "Pet Armor":
		tab.ShortName = ""
		tab.IconItemID = 5183 // B.B. Cap
	case "Taming Item":
		tab.ShortName = ""
		tab.IconItemID = 632 // Unripe Apple
	case "Usable Item":
		tab.ShortName = ""
		tab.IconItemID = 603 // Fly Wing
	case "Weapon":
		tab.ShortName = ""
		tab.IconItemID = 1162 // Main Gauche
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
		sortBy = "highest_price"
		orderByClause = allowedSorts[sortBy]
		order = "DESC"
	} else {
		if strings.ToUpper(order) != "DESC" {
			order = "ASC"
		}
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
		sortBy = "price"
		orderByClause = allowedSorts[sortBy]
		order = "DESC"
	} else {
		if strings.ToUpper(order) != "DESC" {
			order = "ASC"
		}
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
	// 1. Get page parameter
	pageStr := r.URL.Query().Get("page")
	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}
	const eventsPerPage = 50 // Define how many events per page

	// 2. Get total event count for pagination
	var totalEvents int
	err = db.QueryRow("SELECT COUNT(*) FROM market_events").Scan(&totalEvents)
	if err != nil {
		http.Error(w, "Could not count market events", http.StatusInternalServerError)
		log.Printf("❌ Could not count market events: %v", err)
		return
	}

	// 3. Calculate pagination details
	totalPages := 0
	if totalEvents > 0 {
		totalPages = int(math.Ceil(float64(totalEvents) / float64(eventsPerPage)))
	}
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * eventsPerPage

	// 4. Query for paginated events
	eventRows, err := db.Query(`
        SELECT event_timestamp, event_type, item_name, item_id, details
        FROM market_events
        ORDER BY event_timestamp DESC
        LIMIT ? OFFSET ?`, eventsPerPage, offset)
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

	// 5. Populate data struct with pagination info
	data := ActivityPageData{
		MarketEvents:   marketEvents,
		LastScrapeTime: getLastScrapeTime(),
		CurrentPage:    page,
		TotalPages:     totalPages,
		PrevPage:       page - 1,
		NextPage:       page + 1,
		HasPrevPage:    page > 1,
		HasNextPage:    page < totalPages,
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

	// Get page parameter for the listings table
	pageStr := r.URL.Query().Get("page")
	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}
	const listingsPerPage = 50 // Define how many listings per page

	var itemID int
	err = db.QueryRow("SELECT item_id FROM items WHERE name_of_the_item = ? AND item_id > 0 LIMIT 1", itemName).Scan(&itemID)
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

	// --- Pagination for All Listings Table ---
	var totalListings int
	countQuery := "SELECT COUNT(*) FROM items WHERE name_of_the_item = ?"
	err = db.QueryRow(countQuery, itemName).Scan(&totalListings)
	if err != nil {
		http.Error(w, "Could not count item listings", http.StatusInternalServerError)
		log.Printf("❌ Could not count listings for item '%s': %v", itemName, err)
		return
	}

	totalPages := 0
	if totalListings > 0 {
		totalPages = int(math.Ceil(float64(totalListings) / float64(listingsPerPage)))
	}
	if page > totalPages && totalPages > 0 {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * listingsPerPage
	if offset < 0 {
		offset = 0
	}
	// --- End Pagination Logic ---

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
		ORDER BY is_available DESC, date_and_time_retrieved DESC
		LIMIT ? OFFSET ?;
	`
	rowsAll, err := db.Query(allListingsQuery, itemName, listingsPerPage, offset)
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
		// Pagination data
		TotalListings: totalListings,
		CurrentPage:   page,
		TotalPages:    totalPages,
		PrevPage:      page - 1,
		NextPage:      page + 1,
		HasPrevPage:   page > 1,
		HasNextPage:   page < totalPages,
	}
	tmpl.Execute(w, data)
}

// getLastScrapeTime is a helper function to get the most recent market scrape time.
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

// getLastPlayerCountTime is a helper function to get the most recent player count scrape time.
func getLastPlayerCountTime() string {
	var lastScrapeTimestamp sql.NullString
	err := db.QueryRow("SELECT MAX(timestamp) FROM player_history").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last player count time: %v", err)
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

// getLastGuildScrapeTime is a helper function to get the most recent guild scrape time.
func getLastGuildScrapeTime() string {
	var lastScrapeTimestamp sql.NullString
	// Query the 'guilds' table for the most recent 'last_updated' timestamp.
	err := db.QueryRow("SELECT MAX(last_updated) FROM guilds").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last guild scrape time: %v", err)
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

// getLastCharacterScrapeTime is a helper function to get the most recent character scrape time.
func getLastCharacterScrapeTime() string {
	var lastScrapeTimestamp sql.NullString
	// Query the 'characters' table for the most recent 'last_updated' timestamp.
	err := db.QueryRow("SELECT MAX(last_updated) FROM characters").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last character scrape time: %v", err)
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

	const maxGraphDataPoints = 720 // Set a reasonable limit for data points on the graph
	var query string
	var rows *sql.Rows
	var err error

	duration := now.Sub(viewStart)
	// Only downsample if the total number of minutes in the interval exceeds our desired max data points.
	// This prevents downsampling on short intervals like 30m or 6h.
	if duration.Minutes() > maxGraphDataPoints {
		// Calculate the size of each time bucket in seconds.
		// We divide the total duration by the number of points we want.
		// Ensure the bucket is at least 60s (our scrape interval) to make sense.
		bucketSizeInSeconds := int(duration.Seconds()) / maxGraphDataPoints
		if bucketSizeInSeconds < 60 {
			bucketSizeInSeconds = 60
		}

		log.Printf("📊 Player graph: Downsampling data for '%s' interval. Bucket size: %d seconds.", interval, bucketSizeInSeconds)

		// This query groups data into time buckets.
		// It takes the average player/seller count within each bucket.
		// The timestamp for the bucket is the earliest timestamp that falls into it.
		// `unixepoch(timestamp) / %d` creates the grouping key for the time buckets.
		query = fmt.Sprintf(`
			SELECT
				MIN(timestamp),
				CAST(AVG(count) AS INTEGER),
				CAST(AVG(seller_count) AS INTEGER)
			FROM player_history
			%s
			GROUP BY CAST(unixepoch(timestamp) / %d AS INTEGER)
			ORDER BY 1 ASC`, whereClause, bucketSizeInSeconds)
		rows, err = db.Query(query, params...)

	} else {
		// If we don't need to downsample, use the original query to get all data points.
		log.Printf("📊 Player graph: Fetching all data points for '%s' interval.", interval)
		query = fmt.Sprintf("SELECT timestamp, count, seller_count FROM player_history %s ORDER BY timestamp ASC", whereClause)
		rows, err = db.Query(query, params...)
	}

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
		var sellerCount sql.NullInt64
		if err := rows.Scan(&timestampStr, &point.Count, &sellerCount); err != nil {
			log.Printf("⚠️ Failed to scan player history row: %v", err)
			continue
		}
		if sellerCount.Valid {
			point.SellerCount = int(sellerCount.Int64)
		} else {
			point.SellerCount = 0
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

	// Get the latest active player count
	var latestCount, latestSellerCount int
	err = db.QueryRow("SELECT count, seller_count FROM player_history ORDER BY timestamp DESC LIMIT 1").Scan(&latestCount, &latestSellerCount)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ Could not query for latest player count: %v", err)
		latestCount = 0
		latestSellerCount = 0
	}
	latestActivePlayers := latestCount - latestSellerCount

	// Get the historical maximum active players
	var historicalMaxActive int
	var historicalMaxTime string
	var historicalMaxTimestampStr sql.NullString
	err = db.QueryRow("SELECT (count - COALESCE(seller_count, 0)), timestamp FROM player_history ORDER BY (count - COALESCE(seller_count, 0)) DESC LIMIT 1").Scan(&historicalMaxActive, &historicalMaxTimestampStr)

	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ Could not query for historical max player count: %v", err)
		historicalMaxActive = 0
		historicalMaxTime = "N/A"
	} else if !historicalMaxTimestampStr.Valid {
		historicalMaxActive = 0
		historicalMaxTime = "N/A"
	} else {
		// Format the time for display
		parsedTime, parseErr := time.Parse(time.RFC3339, historicalMaxTimestampStr.String)
		if parseErr == nil {
			historicalMaxTime = parsedTime.Format("2006-01-02 15:04")
		} else {
			historicalMaxTime = historicalMaxTimestampStr.String // fallback
		}
	}

	tmpl, err := template.ParseFiles("players.html")
	if err != nil {
		http.Error(w, "Could not load players template", http.StatusInternalServerError)
		log.Printf("❌ Could not load players.html template: %v", err)
		return
	}

	data := PlayerCountPageData{
		PlayerDataJSON:                 template.JS(playerHistoryJSON),
		LastScrapeTime:                 getLastPlayerCountTime(),
		SelectedInterval:               interval,
		EventDataJSON:                  template.JS(eventIntervalsJSON),
		LatestActivePlayers:            latestActivePlayers,
		HistoricalMaxActivePlayers:     historicalMaxActive,
		HistoricalMaxActivePlayersTime: historicalMaxTime,
	}
	tmpl.Execute(w, data)
}

// characterHandler serves the new player characters page.
func characterHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	// --- 1. Get query parameters ---
	searchName := r.FormValue("name_query")
	selectedClass := r.FormValue("class_filter")
	selectedGuild := r.FormValue("guild_filter")
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")
	pageStr := r.FormValue("page")
	selectedCols := r.Form["cols"]
	graphFilter := r.Form["graph_filter"]

	isInitialLoad := len(r.Form) == 0
	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}
	const playersPerPage = 50

	// Define special players who get an emoji
	specialPlayers := map[string]bool{
		"Purity Ring":   true,
		"Bafo MvP":      true,
		"franco bs":     true,
		"franco alchie": true,
		"Afanei":        true,
		"GiupSankino":   true,
		"MacroBot1000":  true,
		"Sileeent":      true,
		"Shiiv":         true,
		"Majim Lipe":    true,
		"Solidao":       true,
		"WildTig3r":     true,
		"No Glory":      true, // was father aesir
	}

	// Get all guild masters to identify leaders
	guildMasters := make(map[string]bool)
	masterRows, err := db.Query("SELECT DISTINCT master FROM guilds WHERE master IS NOT NULL AND master != ''")
	if err != nil {
		log.Printf("⚠️ Could not query for guild masters: %v", err)
	} else {
		defer masterRows.Close()
		for masterRows.Next() {
			var masterName string
			if err := masterRows.Scan(&masterName); err == nil {
				guildMasters[masterName] = true
			}
		}
	}

	// --- 1.5. Define columns and determine visibility ---
	allCols := []Column{
		{ID: "rank", DisplayName: "Rank"},
		{ID: "base_level", DisplayName: "Base Lvl"},
		{ID: "job_level", DisplayName: "Job Lvl"},
		{ID: "experience", DisplayName: "Exp %"},
		{ID: "zeny", DisplayName: "Zeny"},
		{ID: "class", DisplayName: "Class"},
		{ID: "guild", DisplayName: "Guild"},
		{ID: "last_updated", DisplayName: "Last Updated"},
		{ID: "last_active", DisplayName: "Last Active"},
	}

	visibleColumns := make(map[string]bool)
	columnParams := url.Values{}

	if isInitialLoad {
		// Initial page load, set the defaults.
		visibleColumns["base_level"] = true
		visibleColumns["job_level"] = true
		visibleColumns["experience"] = true
		visibleColumns["class"] = true
		visibleColumns["guild"] = true
		visibleColumns["last_active"] = true
		for colID := range visibleColumns {
			columnParams.Add("cols", colID)
		}
		// Default graph filter on initial load
		graphFilter = []string{"second"}
	} else {
		// A form was submitted (filter, sort, columns, or page change).
		// Populate based on selection. If no `cols` param, no optional columns will be visible.
		for _, col := range selectedCols {
			visibleColumns[col] = true
			columnParams.Add("cols", col)
		}
	}

	// Create URL parameters for the graph filter to persist its state
	graphFilterParams := url.Values{}
	for _, f := range graphFilter {
		graphFilterParams.Add("graph_filter", f)
	}

	// --- 2. Get all unique classes for the filter dropdown ---
	var allClasses []string
	classRows, err := db.Query("SELECT DISTINCT class FROM characters ORDER BY class ASC")
	if err != nil {
		log.Printf("⚠️ Could not query for unique player classes: %v", err)
	} else {
		defer classRows.Close()
		for classRows.Next() {
			var className string
			if err := classRows.Scan(&className); err == nil {
				allClasses = append(allClasses, className)
			}
		}
	}

	// --- 3. Build dynamic WHERE clause and parameters ---
	var whereConditions []string
	var params []interface{}
	if searchName != "" {
		whereConditions = append(whereConditions, "name LIKE ?")
		params = append(params, "%"+searchName+"%")
	}
	if selectedClass != "" {
		whereConditions = append(whereConditions, "class = ?")
		params = append(params, selectedClass)
	}
	if selectedGuild != "" {
		whereConditions = append(whereConditions, "guild_name = ?")
		params = append(params, selectedGuild)
	}
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	// --- 3.1. Get Class Distribution for Graph based on the same filters ---
	var classDistribution = make(map[string]int)
	distQuery := fmt.Sprintf("SELECT class, COUNT(*) as count FROM characters %s GROUP BY class", whereClause)
	distRows, err := db.Query(distQuery, params...)
	if err != nil {
		log.Printf("⚠️ Could not query for filtered class distribution: %v", err)
	} else {
		defer distRows.Close()
		for distRows.Next() {
			var className string
			var count int
			if err := distRows.Scan(&className, &count); err == nil {
				classDistribution[className] = count
			}
		}
	}

	// Define class categories
	noviceClasses := map[string]bool{"Aprendiz": true, "Super Aprendiz": true}
	firstClasses := map[string]bool{"Arqueiro": true, "Espadachim": true, "Gatuno": true, "Mago": true, "Mercador": true, "Noviço": true}
	secondClasses := map[string]bool{"Alquimista": true, "Arruaceiro": true, "Bardo": true, "Bruxo": true, "Cavaleiro": true, "Caçador": true, "Ferreiro": true, "Mercenário": true, "Monge": true, "Odalisca": true, "Sacerdote": true, "Sábio": true, "Templário": true}

	graphFilterMap := make(map[string]bool)
	for _, f := range graphFilter {
		graphFilterMap[f] = true
	}

	// Process data for the chart, filtering by category
	chartData := make(map[string]int)
	for class, count := range classDistribution {
		if noviceClasses[class] {
			if graphFilterMap["novice"] {
				chartData[class] = count
			}
		} else if firstClasses[class] {
			if graphFilterMap["first"] {
				chartData[class] = count
			}
		} else if secondClasses[class] {
			if graphFilterMap["second"] {
				chartData[class] = count
			}
		}
	}
	classDistJSON, err := json.Marshal(chartData)
	if err != nil {
		log.Printf("⚠️ Could not marshal class distribution data: %v", err)
		classDistJSON = []byte("{}") // empty object on error
	}

	// --- 3.5 Handle Sorting ---
	allowedSorts := map[string]string{
		"rank":         "rank",
		"name":         "name",
		"base_level":   "base_level",
		"job_level":    "job_level",
		"experience":   "experience",
		"zeny":         "zeny",
		"class":        "class",
		"guild":        "guild_name",
		"last_updated": "last_updated",
		"last_active":  "last_active",
	}
	orderByClause, ok := allowedSorts[sortBy]
	if !ok {
		orderByClause, sortBy = "rank", "rank" // Default sort
	}
	if strings.ToUpper(order) != "DESC" {
		order = "ASC"
	}
	orderByFullClause := fmt.Sprintf("ORDER BY %s %s", orderByClause, order)

	// --- 4. Get the total count and total zeny of matching players for pagination ---
	var totalPlayers int
	var totalZeny sql.NullInt64 // Use NullInt64 in case of no results (SUM would be NULL)
	countQuery := fmt.Sprintf("SELECT COUNT(*), SUM(zeny) FROM characters %s", whereClause)
	err = db.QueryRow(countQuery, params...).Scan(&totalPlayers, &totalZeny)
	if err != nil {
		http.Error(w, "Could not count player characters", http.StatusInternalServerError)
		log.Printf("❌ Could not count player characters: %v", err)
		return
	}

	// --- 5. Calculate pagination details ---
	totalPages := int(math.Ceil(float64(totalPlayers) / float64(playersPerPage)))
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * playersPerPage

	// --- 6. Fetch the paginated player data ---
	query := fmt.Sprintf(`
		SELECT rank, name, base_level, job_level, experience, class, guild_name, zeny, last_updated, last_active
		FROM characters
		%s
		%s
		LIMIT ? OFFSET ?
	`, whereClause, orderByFullClause)
	finalParams := append(params, playersPerPage, offset)

	rows, err := db.Query(query, finalParams...)
	if err != nil {
		http.Error(w, "Could not query for player characters", http.StatusInternalServerError)
		log.Printf("❌ Could not query for player characters: %v", err)
		return
	}
	defer rows.Close()

	var players []PlayerCharacter
	for rows.Next() {
		var p PlayerCharacter
		var lastUpdatedStr, lastActiveStr string
		if err := rows.Scan(&p.Rank, &p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.GuildName, &p.Zeny, &lastUpdatedStr, &lastActiveStr); err != nil {
			log.Printf("⚠️ Failed to scan player character row: %v", err)
			continue
		}

		// Format dates for display
		lastUpdatedTime, err := time.Parse(time.RFC3339, lastUpdatedStr)
		if err == nil {
			p.LastUpdated = lastUpdatedTime.Format("2006-01-02 15:04")
		} else {
			p.LastUpdated = lastUpdatedStr
		}

		lastActiveTime, err := time.Parse(time.RFC3339, lastActiveStr)
		if err == nil {
			p.LastActive = lastActiveTime.Format("2006-01-02 15:04")
		} else {
			p.LastActive = lastActiveStr
		}

		// Set active status
		p.IsActive = (lastUpdatedStr == lastActiveStr) && lastUpdatedStr != ""

		// Check if the player is a guild leader
		if _, isMaster := guildMasters[p.Name]; isMaster {
			p.IsGuildLeader = true
		}

		// Check if the player is a special player
		if _, ok := specialPlayers[p.Name]; ok {
			p.IsSpecial = true
		}

		players = append(players, p)
	}

	// --- 7. Load template and send data ---
	funcMap := template.FuncMap{
		"toggleOrder": func(currentOrder string) string {
			if currentOrder == "ASC" {
				return "DESC"
			}
			return "ASC"
		},
		"formatZeny": func(zeny int64) string {
			s := strconv.FormatInt(zeny, 10)
			if len(s) <= 3 {
				return s
			}
			var result []string
			for i := len(s); i > 0; i -= 3 {
				start := i - 3
				if start < 0 {
					start = 0
				}
				result = append([]string{s[start:i]}, result...)
			}
			return strings.Join(result, ".")
		},
	}

	tmpl, err := template.New("characters.html").Funcs(funcMap).ParseFiles("characters.html")
	if err != nil {
		http.Error(w, "Could not load characters template", http.StatusInternalServerError)
		log.Printf("❌ Could not load characters.html template: %v", err)
		return
	}

	data := CharacterPageData{
		Players:               players,
		LastScrapeTime:        getLastCharacterScrapeTime(),
		SearchName:            searchName,
		SelectedClass:         selectedClass,
		SelectedGuild:         selectedGuild,
		AllClasses:            allClasses,
		SortBy:                sortBy,
		Order:                 order,
		VisibleColumns:        visibleColumns,
		AllColumns:            allCols,
		ColumnParams:          template.URL(columnParams.Encode()),
		CurrentPage:           page,
		TotalPages:            totalPages,
		PrevPage:              page - 1,
		NextPage:              page + 1,
		HasPrevPage:           page > 1,
		HasNextPage:           page < totalPages,
		TotalPlayers:          totalPlayers,
		TotalZeny:             totalZeny.Int64,
		ClassDistributionJSON: template.JS(classDistJSON),
		GraphFilter:           graphFilterMap,
		GraphFilterParams:     template.URL(graphFilterParams.Encode()),
		HasChartData:          len(chartData) > 1,
	}
	tmpl.Execute(w, data)
}

// guildHandler serves the new player guilds page.
func guildHandler(w http.ResponseWriter, r *http.Request) {
	// --- 1. Get query parameters ---
	searchName := r.URL.Query().Get("name_query")
	pageStr := r.URL.Query().Get("page")
	sortBy := r.URL.Query().Get("sort_by")
	order := r.URL.Query().Get("order")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}
	const guildsPerPage = 50

	// --- 2. Build dynamic WHERE clause and parameters ---
	var whereConditions []string
	var params []interface{}
	if searchName != "" {
		whereConditions = append(whereConditions, "name LIKE ?")
		params = append(params, "%"+searchName+"%")
	}
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	// --- 3. Handle Sorting ---
	allowedSorts := map[string]string{
		"rank":      "rank",
		"name":      "name",
		"level":     "level",
		"master":    "master",
		"members":   "member_count",
		"zeny":      "total_zeny",
		"avg_level": "avg_base_level",
	}
	orderByClause, ok := allowedSorts[sortBy]
	isDefaultSort := !ok
	if isDefaultSort {
		orderByClause, sortBy = "level", "level" // Set default sort column
	}

	// Sanitize order parameter, defaulting to ASC
	if strings.ToUpper(order) != "DESC" {
		order = "ASC"
	}

	// Override order to DESC for the initial, default page load
	if isDefaultSort {
		order = "DESC"
	}

	orderByFullClause := fmt.Sprintf("ORDER BY %s %s", orderByClause, order)

	// --- 4. Get the total count of matching guilds for pagination ---
	var totalGuilds int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM guilds %s", whereClause)
	err = db.QueryRow(countQuery, params...).Scan(&totalGuilds)
	if err != nil {
		http.Error(w, "Could not count guilds", http.StatusInternalServerError)
		log.Printf("❌ Could not count guilds: %v", err)
		return
	}

	// --- 5. Calculate pagination details ---
	totalPages := int(math.Ceil(float64(totalGuilds) / float64(guildsPerPage)))
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * guildsPerPage

	// --- 6. Fetch the paginated guild data ---
	// --- 6. Fetch the paginated guild data ---
	query := fmt.Sprintf(`
	SELECT
		name, level, experience, master, emblem_url,
		(SELECT COUNT(*) FROM characters WHERE guild_name = guilds.name) as member_count,
		    COALESCE((SELECT SUM(zeny) FROM characters WHERE guild_name = guilds.name), 0) as total_zeny,
		    COALESCE((SELECT AVG(base_level) FROM characters WHERE guild_name = guilds.name), 0) as avg_base_level
		FROM guilds
		%s
		%s
		LIMIT ? OFFSET ?
	`, whereClause, orderByFullClause)
	finalParams := append(params, guildsPerPage, offset)

	rows, err := db.Query(query, finalParams...)
	if err != nil {
		http.Error(w, "Could not query for guilds", http.StatusInternalServerError)
		log.Printf("❌ Could not query for guilds: %v", err)
		return
	}
	defer rows.Close()

	var guilds []Guild
	for rows.Next() {
		var g Guild
		if err := rows.Scan(&g.Name, &g.Level, &g.Experience, &g.Master, &g.EmblemURL, &g.MemberCount, &g.TotalZeny, &g.AvgBaseLevel); err != nil {
			log.Printf("⚠️ Failed to scan guild row: %v", err)
			continue
		}
		guilds = append(guilds, g)
	}

	// --- 7. Load template and send data ---
	funcMap := template.FuncMap{
		"toggleOrder": func(currentOrder string) string {
			if currentOrder == "ASC" {
				return "DESC"
			}
			return "ASC"
		},
		"formatZeny": func(zeny int64) string {
			s := strconv.FormatInt(zeny, 10)
			if len(s) <= 3 {
				return s
			}
			var result []string
			for i := len(s); i > 0; i -= 3 {
				start := i - 3
				if start < 0 {
					start = 0
				}
				result = append([]string{s[start:i]}, result...)
			}
			return strings.Join(result, ".")
		},
		"formatAvgLevel": func(level float64) string {
			if level == 0 {
				return "N/A"
			}
			return fmt.Sprintf("%.1f", level)
		},
	}

	tmpl, err := template.New("guilds.html").Funcs(funcMap).ParseFiles("guilds.html")
	if err != nil {
		http.Error(w, "Could not load guilds template", http.StatusInternalServerError)
		log.Printf("❌ Could not load guilds.html template: %v", err)
		return
	}

	data := GuildPageData{
		Guilds:              guilds,
		LastGuildUpdateTime: getLastGuildScrapeTime(),
		SearchName:          searchName,
		SortBy:              sortBy,
		Order:               order,
		CurrentPage:         page,
		TotalPages:          totalPages,
		PrevPage:            page - 1,
		NextPage:            page + 1,
		HasPrevPage:         page > 1,
		HasNextPage:         page < totalPages,
		TotalGuilds:         totalGuilds,
	}
	tmpl.Execute(w, data)
}

// mvpKillsHandler serves the page for MVP kill rankings.
func mvpKillsHandler(w http.ResponseWriter, r *http.Request) {
	sortBy := r.URL.Query().Get("sort_by")
	order := r.URL.Query().Get("order")

	// --- 1. Build Headers ---
	headers := []MvpHeader{
		{MobID: "total", MobName: "Total Kills"},
	}
	for _, mobID := range mvpMobIDs {
		headers = append(headers, MvpHeader{
			MobID:   mobID,
			MobName: mvpNames[mobID],
		})
	}

	// --- 2. Handle Sorting ---
	var orderByClause string
	allowedSort := false
	if sortBy != "" {
		if sortBy == "name" {
			allowedSort = true
			orderByClause = "character_name"
		} else if sortBy == "total" {
			// Create the SUM expression for total kills
			var sumParts []string
			for _, mobID := range mvpMobIDs {
				sumParts = append(sumParts, fmt.Sprintf("mvp_%s", mobID))
			}
			orderByClause = fmt.Sprintf("(%s)", strings.Join(sumParts, " + "))
			allowedSort = true
		} else {
			// Check if sorting by a specific MVP ID
			for _, mobID := range mvpMobIDs {
				if sortBy == mobID {
					orderByClause = fmt.Sprintf("mvp_%s", mobID)
					allowedSort = true
					break
				}
			}
		}
	}

	if !allowedSort {
		// Default sort by total kills
		var sumParts []string
		for _, mobID := range mvpMobIDs {
			sumParts = append(sumParts, fmt.Sprintf("mvp_%s", mobID))
		}
		orderByClause = fmt.Sprintf("(%s)", strings.Join(sumParts, " + "))
		sortBy = "total"
		order = "DESC"
	} else {
		if strings.ToUpper(order) != "DESC" {
			order = "ASC"
		}
	}

	// --- 3. Fetch Data ---
	query := fmt.Sprintf("SELECT * FROM character_mvp_kills ORDER BY %s %s", orderByClause, order)
	rows, err := db.Query(query)
	if err != nil {
		http.Error(w, "Could not query MVP kills", http.StatusInternalServerError)
		log.Printf("❌ Could not query for MVP kills: %v", err)
		return
	}
	defer rows.Close()

	// --- 4. Process Rows with Dynamic Columns ---
	cols, _ := rows.Columns()
	var players []MvpKillEntry

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			log.Printf("⚠️ Failed to scan MVP kill row: %v", err)
			continue
		}

		player := MvpKillEntry{
			Kills: make(map[string]int),
		}
		totalKills := 0

		for i, colName := range cols {
			val := columns[i]
			if colName == "character_name" {
				player.CharacterName = val.(string)
			} else if strings.HasPrefix(colName, "mvp_") {
				mobID := strings.TrimPrefix(colName, "mvp_")
				killCount := int(val.(int64))
				player.Kills[mobID] = killCount
				totalKills += killCount
			}
		}
		player.TotalKills = totalKills
		players = append(players, player)
	}

	// --- 5. Render Template ---
	funcMap := template.FuncMap{
		"toggleOrder": func(currentOrder string) string {
			if currentOrder == "ASC" {
				return "DESC"
			}
			return "ASC"
		},
		"getKillCount": func(kills map[string]int, mobID string) int {
			return kills[mobID]
		},
	}

	tmpl, err := template.New("mvp_kills.html").Funcs(funcMap).ParseFiles("mvp_kills.html")
	if err != nil {
		http.Error(w, "Could not load mvp_kills template", http.StatusInternalServerError)
		log.Printf("❌ Could not load mvp_kills.html template: %v", err)
		return
	}

	data := MvpKillPageData{
		Players:        players,
		Headers:        headers,
		SortBy:         sortBy,
		Order:          order,
		LastScrapeTime: getLastCharacterScrapeTime(), // MVP data is scraped with characters
	}
	tmpl.Execute(w, data)
}

// ADDED: characterDetailHandler serves the detailed information page for a single character.
func characterDetailHandler(w http.ResponseWriter, r *http.Request) {
	charName := r.URL.Query().Get("name")
	if charName == "" {
		http.Error(w, "Character name is required", http.StatusBadRequest)
		return
	}

	// Define the class image map
	classImages := map[string]string{
		"Aprendiz":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/8/8b/Icon_jobs_0.png",
		"Super Aprendiz": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c7/Icon_jobs_4001.png",
		"Arqueiro":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/97/Icon_jobs_3.png",
		"Espadachim":     "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/5/5b/Icon_jobs_1.png",
		"Gatuno":         "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/3/3c/Icon_jobs_6.png",
		"Mago":           "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/99/Icon_jobs_2.png",
		"Mercador":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/9e/Icon_jobs_5.png",
		"Noviço":         "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c5/Icon_jobs_4.png",
		"Alquimista":     "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c7/Icon_jobs_18.png",
		"Arruaceiro":     "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/4/48/Icon_jobs_17.png",
		"Bardo":          "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/6/69/Icon_jobs_19.png",
		"Bruxo":          "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/0/09/Icon_jobs_9.png",
		"Cavaleiro":      "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/1/1d/Icon_jobs_7.png",
		"Caçador":        "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/e/eb/Icon_jobs_11.png",
		"Ferreiro":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/7/7b/Icon_jobs_10.png",
		"Mercenário":     "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/9c/Icon_jobs_12.png",
		"Monge":          "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/4/44/Icon_jobs_15.png",
		"Odalisca":       "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/d/dc/Icon_jobs_20.png",
		"Sacerdote":      "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/3/3a/Icon_jobs_8.png",
		"Sábio":          "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/0/0e/Icon_jobs_16.png",
		"Templário":      "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/e/e1/Icon_jobs_14.png",
	}

	// Get changelog page parameter
	clPageStr := r.URL.Query().Get("cl_page")
	clPage, err := strconv.Atoi(clPageStr)
	if err != nil || clPage < 1 {
		clPage = 1
	}
	const entriesPerPage = 25 // A reasonable number for this page

	// --- 1. Fetch main character data ---
	var p PlayerCharacter
	var lastUpdatedStr, lastActiveStr string
	query := `SELECT rank, name, base_level, job_level, experience, class, guild_name, zeny, last_updated, last_active FROM characters WHERE name = ?`
	err = db.QueryRow(query, charName).Scan(
		&p.Rank, &p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.GuildName, &p.Zeny, &lastUpdatedStr, &lastActiveStr,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Character not found", http.StatusNotFound)
		} else {
			http.Error(w, "Database query for character failed", http.StatusInternalServerError)
			log.Printf("❌ Could not query for character '%s': %v", charName, err)
		}
		return
	}

	// Format dates and set active status
	lastUpdatedTime, _ := time.Parse(time.RFC3339, lastUpdatedStr)
	p.LastUpdated = lastUpdatedTime.Format("2006-01-02 15:04")
	lastActiveTime, _ := time.Parse(time.RFC3339, lastActiveStr)
	p.LastActive = lastActiveTime.Format("2006-01-02 15:04")
	p.IsActive = (lastUpdatedStr == lastActiveStr) && lastUpdatedStr != ""

	// --- 2. Fetch Guild Data (if any) ---
	var guild *Guild
	if p.GuildName.Valid {
		g := Guild{}
		guildQuery := `SELECT name, level, master, (SELECT COUNT(*) FROM characters WHERE guild_name = guilds.name) as member_count FROM guilds WHERE name = ?`
		err := db.QueryRow(guildQuery, p.GuildName.String).Scan(&g.Name, &g.Level, &g.Master, &g.MemberCount)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("⚠️ Could not fetch guild details for '%s': %v", p.GuildName.String, err)
		} else if err == nil {
			guild = &g
			if p.Name == g.Master {
				p.IsGuildLeader = true
			}
		}
	}

	// --- 3. Fetch MVP Kills ---
	var mvpKills MvpKillEntry
	mvpKills.CharacterName = p.Name
	mvpKills.Kills = make(map[string]int)

	// Build the column names for the query
	var mvpCols []string
	for _, mobID := range mvpMobIDs {
		mvpCols = append(mvpCols, fmt.Sprintf("mvp_%s", mobID))
	}
	mvpQuery := fmt.Sprintf("SELECT %s FROM character_mvp_kills WHERE character_name = ?", strings.Join(mvpCols, ", "))

	scanDest := make([]interface{}, len(mvpMobIDs))
	for i := range mvpMobIDs {
		scanDest[i] = new(int)
	}

	err = db.QueryRow(mvpQuery, charName).Scan(scanDest...)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ Could not fetch MVP kills for '%s': %v", charName, err)
	} else if err == nil {
		totalKills := 0
		for i, mobID := range mvpMobIDs {
			killCount := *scanDest[i].(*int)
			mvpKills.Kills[mobID] = killCount
			totalKills += killCount
		}
		mvpKills.TotalKills = totalKills
	}

	// Create MVP headers for the template
	var mvpHeaders []MvpHeader
	for _, mobID := range mvpMobIDs {
		if name, ok := mvpNames[mobID]; ok {
			mvpHeaders = append(mvpHeaders, MvpHeader{MobID: mobID, MobName: name})
		}
	}

	// --- ADDED: Fetch Guild History ---
	var guildHistory []CharacterChangelog
	guildHistoryQuery := `
		SELECT change_time, activity_description
		FROM character_changelog
		WHERE character_name = ? AND (activity_description LIKE '%joined guild%' OR activity_description LIKE '%left guild%')
		ORDER BY change_time DESC`
	guildHistoryRows, err := db.Query(guildHistoryQuery, charName)
	if err != nil {
		// Log error but don't fail the page load
		log.Printf("⚠️ Could not query for guild history for '%s': %v", charName, err)
	} else {
		defer guildHistoryRows.Close()
		for guildHistoryRows.Next() {
			var entry CharacterChangelog
			var timestampStr string
			if err := guildHistoryRows.Scan(&timestampStr, &entry.ActivityDescription); err != nil {
				log.Printf("⚠️ Failed to scan guild history row: %v", err)
				continue
			}

			parsedTime, err := time.Parse(time.RFC3339, timestampStr)
			if err == nil {
				//				entry.ChangeTime = parsedTime.Format("2006-01-02 15:04")
				entry.ChangeTime = parsedTime.Format("2006-01-02")
			} else {
				entry.ChangeTime = timestampStr
			}
			guildHistory = append(guildHistory, entry)
		}
	}
	// --- END of addition ---

	// --- 4. Fetch Character Changelog (paginated) ---
	// ... (rest of the function) ...

	// --- 4. Fetch Character Changelog (paginated) ---
	var totalChangelogEntries int
	countQuery := "SELECT COUNT(*) FROM character_changelog WHERE character_name = ?"
	err = db.QueryRow(countQuery, charName).Scan(&totalChangelogEntries)
	if err != nil {
		http.Error(w, "Could not count changelog entries", http.StatusInternalServerError)
		log.Printf("❌ Could not count changelog entries for '%s': %v", charName, err)
		return
	}

	// Calculate pagination details
	clTotalPages := 0
	if totalChangelogEntries > 0 {
		clTotalPages = int(math.Ceil(float64(totalChangelogEntries) / float64(entriesPerPage)))
	}
	if clPage > clTotalPages && clTotalPages > 0 {
		clPage = clTotalPages
	}
	if clPage < 1 {
		clPage = 1
	}
	offset := (clPage - 1) * entriesPerPage
	if offset < 0 {
		offset = 0
	}

	// Query for paginated changelog entries
	var changelogEntries []CharacterChangelog
	changelogQuery := `
        SELECT change_time, activity_description
        FROM character_changelog
        WHERE character_name = ?
        ORDER BY change_time DESC
        LIMIT ? OFFSET ?`
	changelogRows, err := db.Query(changelogQuery, charName, entriesPerPage, offset)
	if err != nil {
		http.Error(w, "Could not query for character changelog", http.StatusInternalServerError)
		log.Printf("❌ Could not query for character changelog for '%s': %v", charName, err)
		return
	}
	defer changelogRows.Close()

	for changelogRows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := changelogRows.Scan(&timestampStr, &entry.ActivityDescription); err != nil {
			log.Printf("⚠️ Failed to scan character changelog row: %v", err)
			continue
		}

		entry.CharacterName = charName // Not needed from query, we already know it
		parsedTime, err := time.Parse(time.RFC3339, timestampStr)
		if err == nil {
			entry.ChangeTime = parsedTime.Format("2006-01-02 15:04:05")
		} else {
			entry.ChangeTime = timestampStr
		}
		changelogEntries = append(changelogEntries, entry)
	}

	// --- 5. Prepare data and render template ---
	data := CharacterDetailPageData{
		Character:            p,
		Guild:                guild,
		MvpKills:             mvpKills,
		MvpHeaders:           mvpHeaders,
		GuildHistory:         guildHistory,
		LastScrapeTime:       getLastCharacterScrapeTime(),
		ClassImageURL:        classImages[p.Class],
		ChangelogEntries:     changelogEntries,
		ChangelogCurrentPage: clPage,
		ChangelogTotalPages:  clTotalPages,
		ChangelogPrevPage:    clPage - 1,
		ChangelogNextPage:    clPage + 1,
		HasChangelogPrevPage: clPage > 1,
		HasChangelogNextPage: clPage < clTotalPages,
	}

	funcMap := template.FuncMap{
		"formatZeny": func(zeny int64) string {
			s := strconv.FormatInt(zeny, 10)
			if len(s) <= 3 {
				return s
			}
			var result []string
			for i := len(s); i > 0; i -= 3 {
				start := i - 3
				if start < 0 {
					start = 0
				}
				result = append([]string{s[start:i]}, result...)
			}
			return strings.Join(result, ".")
		},
		"getKillCount": func(kills map[string]int, mobID string) int {
			return kills[mobID]
		},
	}

	tmpl, err := template.New("character_detail.html").Funcs(funcMap).ParseFiles("character_detail.html")
	if err != nil {
		http.Error(w, "Could not load character detail template", http.StatusInternalServerError)
		log.Printf("❌ Could not load character_detail.html template: %v", err)
		return
	}

	tmpl.Execute(w, data)
}

// characterChangelogHandler serves the page for recent character changes.
func characterChangelogHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Get page parameter
	pageStr := r.URL.Query().Get("page")
	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}
	const entriesPerPage = 100 // Define how many entries per page

	// 2. Get total event count for pagination
	var totalEntries int
	err = db.QueryRow("SELECT COUNT(*) FROM character_changelog").Scan(&totalEntries)
	if err != nil {
		http.Error(w, "Could not count changelog entries", http.StatusInternalServerError)
		log.Printf("❌ Could not count changelog entries: %v", err)
		return
	}

	// 3. Calculate pagination details
	totalPages := 0
	if totalEntries > 0 {
		totalPages = int(math.Ceil(float64(totalEntries) / float64(entriesPerPage)))
	}
	if page > totalPages && totalPages > 0 {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * entriesPerPage
	if offset < 0 {
		offset = 0
	}

	// 4. Query for paginated events
	changelogRows, err := db.Query(`
        SELECT character_name, change_time, activity_description
        FROM character_changelog
        ORDER BY change_time DESC
        LIMIT ? OFFSET ?`, entriesPerPage, offset)
	if err != nil {
		http.Error(w, "Could not query for character changelog", http.StatusInternalServerError)
		log.Printf("❌ Could not query for character changelog: %v", err)
		return
	}
	defer changelogRows.Close()

	var changelogEntries []CharacterChangelog
	for changelogRows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := changelogRows.Scan(&entry.CharacterName, &timestampStr, &entry.ActivityDescription); err != nil {
			log.Printf("⚠️ Failed to scan character changelog row: %v", err)
			continue
		}

		parsedTime, err := time.Parse(time.RFC3339, timestampStr)
		if err == nil {
			entry.ChangeTime = parsedTime.Format("2006-01-02 15:04:05")
		} else {
			entry.ChangeTime = timestampStr
		}

		changelogEntries = append(changelogEntries, entry)
	}

	tmpl, err := template.ParseFiles("character_changelog.html")
	if err != nil {
		http.Error(w, "Could not load character changelog template", http.StatusInternalServerError)
		log.Printf("❌ Could not load character_changelog.html template: %v", err)
		return
	}

	// 5. Populate data struct with pagination info
	data := CharacterChangelogPageData{
		ChangelogEntries: changelogEntries,
		LastScrapeTime:   getLastCharacterScrapeTime(), // Use character scrape time as it's most relevant
		CurrentPage:      page,
		TotalPages:       totalPages,
		PrevPage:         page - 1,
		NextPage:         page + 1,
		HasPrevPage:      page > 1,
		HasNextPage:      page < totalPages,
	}
	tmpl.Execute(w, data)
}

// ADDED: guildDetailHandler serves the new guild detail page.
func guildDetailHandler(w http.ResponseWriter, r *http.Request) {
	guildName := r.URL.Query().Get("name")
	if guildName == "" {
		http.Error(w, "Guild name is required", http.StatusBadRequest)
		return
	}

	// --- 1. Get query parameters for sorting members and changelog pagination ---
	sortBy := r.URL.Query().Get("sort_by")
	order := r.URL.Query().Get("order")
	clPageStr := r.URL.Query().Get("cl_page")
	clPage, err := strconv.Atoi(clPageStr)
	if err != nil || clPage < 1 {
		clPage = 1
	}
	const entriesPerPage = 25

	// --- 2. Fetch main guild data ---
	var g Guild
	guildQuery := `
        SELECT
            name, level, experience, master, emblem_url,
            (SELECT COUNT(*) FROM characters WHERE guild_name = guilds.name) as member_count,
            COALESCE((SELECT SUM(zeny) FROM characters WHERE guild_name = guilds.name), 0) as total_zeny,
            COALESCE((SELECT AVG(base_level) FROM characters WHERE guild_name = guilds.name), 0) as avg_base_level
        FROM guilds
        WHERE name = ?`

	err = db.QueryRow(guildQuery, guildName).Scan(&g.Name, &g.Level, &g.Experience, &g.Master, &g.EmblemURL, &g.MemberCount, &g.TotalZeny, &g.AvgBaseLevel)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Guild not found", http.StatusNotFound)
		} else {
			http.Error(w, "Database query for guild details failed", http.StatusInternalServerError)
			log.Printf("❌ Could not query for guild '%s': %v", guildName, err)
		}
		return
	}

	// --- 3. Handle Sorting for Member List ---
	allowedSorts := map[string]string{
		"rank":        "rank",
		"name":        "name",
		"base_level":  "base_level",
		"job_level":   "job_level",
		"experience":  "experience",
		"zeny":        "zeny",
		"class":       "class",
		"last_active": "last_active",
	}
	orderByClause, ok := allowedSorts[sortBy]
	if !ok {
		orderByClause, sortBy = "base_level", "base_level" // Default sort
		order = "DESC"
	} else {
		if strings.ToUpper(order) != "DESC" {
			order = "ASC"
		}
	}
	orderByFullClause := fmt.Sprintf("ORDER BY %s %s", orderByClause, order)

	// --- 4. Fetch guild members ---
	membersQuery := fmt.Sprintf(`
		SELECT rank, name, base_level, job_level, experience, class, guild_name, zeny, last_updated, last_active
		FROM characters
		WHERE guild_name = ?
		%s
	`, orderByFullClause)

	rows, err := db.Query(membersQuery, guildName)
	if err != nil {
		http.Error(w, "Could not query for guild members", http.StatusInternalServerError)
		log.Printf("❌ Could not query for members of guild '%s': %v", guildName, err)
		return
	}
	defer rows.Close()

	var members []PlayerCharacter
	classDistribution := make(map[string]int)
	for rows.Next() {
		var p PlayerCharacter
		var lastUpdatedStr, lastActiveStr string
		if err := rows.Scan(&p.Rank, &p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.GuildName, &p.Zeny, &lastUpdatedStr, &lastActiveStr); err != nil {
			log.Printf("⚠️ Failed to scan guild member row: %v", err)
			continue
		}
		classDistribution[p.Class]++ // Increment class count for the chart

		lastActiveTime, _ := time.Parse(time.RFC3339, lastActiveStr)
		p.LastActive = lastActiveTime.Format("2006-01-02 15:04")

		if p.Name == g.Master {
			p.IsGuildLeader = true
		}

		members = append(members, p)
	}

	// --- 5. Prepare chart data ---
	classDistJSON, err := json.Marshal(classDistribution)
	if err != nil {
		log.Printf("⚠️ Could not marshal class distribution data for guild '%s': %v", guildName, err)
		classDistJSON = []byte("{}") // empty object on error
	}

	// --- 6. Fetch Guild Changelog (paginated) ---
	var totalChangelogEntries int
	likePattern := "%" + guildName + "%"
	countQuery := "SELECT COUNT(*) FROM character_changelog WHERE activity_description LIKE ?"
	err = db.QueryRow(countQuery, likePattern).Scan(&totalChangelogEntries)
	if err != nil {
		http.Error(w, "Could not count guild changelog entries", http.StatusInternalServerError)
		log.Printf("❌ Could not count changelog entries for guild '%s': %v", guildName, err)
		return
	}

	// Calculate pagination details
	clTotalPages := 0
	if totalChangelogEntries > 0 {
		clTotalPages = int(math.Ceil(float64(totalChangelogEntries) / float64(entriesPerPage)))
	}
	if clPage > clTotalPages && clTotalPages > 0 {
		clPage = clTotalPages
	}
	if clPage < 1 {
		clPage = 1
	}
	offset := (clPage - 1) * entriesPerPage
	if offset < 0 {
		offset = 0
	}

	// Query for paginated changelog entries
	var changelogEntries []CharacterChangelog
	changelogQuery := `
        SELECT change_time, character_name, activity_description
        FROM character_changelog
        WHERE activity_description LIKE ?
        ORDER BY change_time DESC
        LIMIT ? OFFSET ?`
	changelogRows, err := db.Query(changelogQuery, likePattern, entriesPerPage, offset)
	if err != nil {
		http.Error(w, "Could not query for guild changelog", http.StatusInternalServerError)
		log.Printf("❌ Could not query for guild changelog for '%s': %v", guildName, err)
		return
	}
	defer changelogRows.Close()

	for changelogRows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := changelogRows.Scan(&timestampStr, &entry.CharacterName, &entry.ActivityDescription); err != nil {
			log.Printf("⚠️ Failed to scan guild changelog row: %v", err)
			continue
		}
		parsedTime, err := time.Parse(time.RFC3339, timestampStr)
		if err == nil {
			entry.ChangeTime = parsedTime.Format("2006-01-02 15:04:05")
		} else {
			entry.ChangeTime = timestampStr
		}
		changelogEntries = append(changelogEntries, entry)
	}

	// --- 7. Load template and send data ---
	funcMap := template.FuncMap{
		"toggleOrder": func(currentOrder string) string {
			if currentOrder == "ASC" {
				return "DESC"
			}
			return "ASC"
		},
		"formatZeny": func(zeny int64) string {
			s := strconv.FormatInt(zeny, 10)
			if len(s) <= 3 {
				return s
			}
			var result []string
			for i := len(s); i > 0; i -= 3 {
				start := i - 3
				if start < 0 {
					start = 0
				}
				result = append([]string{s[start:i]}, result...)
			}
			return strings.Join(result, ".")
		},
		"formatAvgLevel": func(level float64) string {
			if level == 0 {
				return "N/A"
			}
			return fmt.Sprintf("%.1f", level)
		},
	}

	tmpl, err := template.New("guild_detail.html").Funcs(funcMap).ParseFiles("guild_detail.html")
	if err != nil {
		http.Error(w, "Could not load guild detail template", http.StatusInternalServerError)
		log.Printf("❌ Could not load guild_detail.html template: %v", err)
		return
	}

	data := GuildDetailPageData{
		Guild:                 g,
		Members:               members,
		LastScrapeTime:        getLastGuildScrapeTime(),
		SortBy:                sortBy,
		Order:                 order,
		ClassDistributionJSON: template.JS(classDistJSON),
		HasChartData:          len(classDistribution) > 1,
		ChangelogEntries:      changelogEntries,
		ChangelogCurrentPage:  clPage,
		ChangelogTotalPages:   clTotalPages,
		ChangelogPrevPage:     clPage - 1,
		ChangelogNextPage:     clPage + 1,
		HasChangelogPrevPage:  clPage > 1,
		HasChangelogNextPage:  clPage < clTotalPages,
	}

	tmpl.Execute(w, data)
}
