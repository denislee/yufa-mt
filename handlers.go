package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"html/template"
	"log"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

var (
	// Allows letters (upper/lower), numbers, and spaces.
	nameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)
	// Allows letters, numbers, spaces, and common characters for contact info.
	contactSanitizer = regexp.MustCompile(`[^a-zA-Z0-9\s:.,#@-]+`)
	// Allows letters, numbers, spaces, and basic punctuation for notes.
	notesSanitizer = regexp.MustCompile(`[^a-zA-Z0-9\s.,?!'-]+`)
	// Allows letters, numbers, spaces, and characters common in item names.
	itemSanitizer = regexp.MustCompile(`[^a-zA-Z0-9\s\[\]\+\-]+`)
)

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

// MvpKillCountOffset is a value subtracted from MVP kills for display purposes.
const MvpKillCountOffset = 3

// A single, reusable function map for all templates.
var templateFuncs = template.FuncMap{
	"lower": strings.ToLower,
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
	"getKillCount": func(kills map[string]int, mobID string) int {
		return kills[mobID]
	},
	"formatAvgLevel": func(level float64) string {
		if level == 0 {
			return "N/A"
		}
		return fmt.Sprintf("%.1f", level)
	},
}

// -------------------------
// -- GENERIC HELPERS --
// -------------------------

// PaginationData holds all the data needed to render pagination controls.
type PaginationData struct {
	CurrentPage int
	TotalPages  int
	PrevPage    int
	NextPage    int
	HasPrevPage bool
	HasNextPage bool
	Offset      int
}

// newPaginationData creates a PaginationData object from the request and total item count.
func newPaginationData(r *http.Request, totalItems int, itemsPerPage int) PaginationData {
	pageStr := r.FormValue("page")
	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	pd := PaginationData{}
	if totalItems > 0 {
		pd.TotalPages = int(math.Ceil(float64(totalItems) / float64(itemsPerPage)))
	} else {
		pd.TotalPages = 1 // Ensure at least one page, even if empty
	}

	if page > pd.TotalPages {
		page = pd.TotalPages
	}
	if page < 1 {
		page = 1
	}

	pd.CurrentPage = page
	pd.Offset = (page - 1) * itemsPerPage
	pd.PrevPage = page - 1
	pd.NextPage = page + 1
	pd.HasPrevPage = page > 1
	pd.HasNextPage = page < pd.TotalPages
	return pd
}

// getSortClause validates sorting parameters and constructs an SQL ORDER BY clause.
func getSortClause(r *http.Request, allowedSorts map[string]string, defaultSortBy, defaultOrder string) (string, string, string) {
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")

	orderByColumn, ok := allowedSorts[sortBy]
	if !ok {
		sortBy = defaultSortBy
		order = defaultOrder
		orderByColumn = allowedSorts[sortBy]
	}

	if strings.ToUpper(order) != "ASC" && strings.ToUpper(order) != "DESC" {
		order = defaultOrder
	}

	return fmt.Sprintf("ORDER BY %s %s", orderByColumn, order), sortBy, order
}

// buildItemSearchClause generates the WHERE condition for an item search (by name or ID).
func buildItemSearchClause(searchQuery, tableAlias string) (string, []interface{}, error) {
	if searchQuery == "" {
		return "", nil, nil
	}

	// Sanitize alias to prevent injection, though it's developer-controlled.
	alias := strings.Trim(regexp.MustCompile(`[^a-zA-Z0-9_]`).ReplaceAllString(tableAlias, ""), ".")
	if alias != "" {
		alias += "."
	}

	// Check if the query is a numeric ID.
	if _, err := strconv.Atoi(searchQuery); err == nil {
		return fmt.Sprintf("%sitem_id = ?", alias), []interface{}{searchQuery}, nil
	}

	// If not numeric, search by name using pre-cached IDs.
	idList, err := getCombinedItemIDs(searchQuery)
	if err != nil {
		return "", nil, fmt.Errorf("failed to perform combined item search: %w", err)
	}

	if len(idList) > 0 {
		placeholders := strings.Repeat("?,", len(idList)-1) + "?"
		clause := fmt.Sprintf("%sitem_id IN (%s)", alias, placeholders)
		params := make([]interface{}, len(idList))
		for i, id := range idList {
			params[i] = id
		}
		return clause, params, nil
	}

	// If no IDs are found, return a condition that yields no results.
	return "1 = 0", nil, nil
}

// -------------------------
// -- HELPER FUNCTIONS --
// -------------------------

// renderTemplate is a helper to parse and execute templates, reducing boilerplate.
func renderTemplate(w http.ResponseWriter, tmplFile string, data interface{}) {
	tmpl, err := template.New(tmplFile).Funcs(templateFuncs).ParseFiles(tmplFile)
	if err != nil {
		log.Printf("❌ Could not load template '%s': %v", tmplFile, err)
		http.Error(w, "Could not load template", http.StatusInternalServerError)
		return
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		log.Printf("❌ Could not execute template '%s': %v", tmplFile, err)
		http.Error(w, "Could not render template", http.StatusInternalServerError)
	}
}

// sanitizeString removes unwanted characters from a string based on a given regex sanitizer.
// This is used to prevent injection attacks by enforcing a character whitelist.
func sanitizeString(input string, sanitizer *regexp.Regexp) string {
	return sanitizer.ReplaceAllString(input, "")
}

// getCombinedItemIDs performs a concurrent search on a remote database and the local DB
// to find all relevant item IDs for a given search query.
func getCombinedItemIDs(searchQuery string) ([]int, error) {
	var wg sync.WaitGroup
	scrapedIDsChan := make(chan []int, 1)
	localIDsChan := make(chan []int, 1)

	wg.Add(2)

	// Goroutine 1: Scrape ragnarokdatabase.com
	go func() {
		defer wg.Done()
		ids, err := scrapeRagnarokDatabaseSearch(searchQuery)
		if err != nil {
			log.Printf("⚠️ Concurrent scrape failed for '%s': %v", searchQuery, err)
			scrapedIDsChan <- []int{} // Send empty slice on error
			return
		}
		scrapedIDsChan <- ids
	}()

	// Goroutine 2: Search local DB by name for matching IDs
	go func() {
		defer wg.Done()
		var ids []int
		query := "SELECT DISTINCT item_id FROM items WHERE name_of_the_item LIKE ? AND item_id > 0"
		rows, err := db.Query(query, "%"+searchQuery+"%")
		if err != nil {
			log.Printf("⚠️ Concurrent local ID search failed for '%s': %v", searchQuery, err)
			localIDsChan <- []int{} // Send empty slice on error
			return
		}
		defer rows.Close()
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err == nil {
				ids = append(ids, id)
			}
		}
		localIDsChan <- ids
	}()

	wg.Wait()
	close(scrapedIDsChan)
	close(localIDsChan)

	scrapedIDs := <-scrapedIDsChan
	localIDs := <-localIDsChan

	// Combine and de-duplicate IDs using a map
	combinedIDs := make(map[int]struct{})
	for _, id := range scrapedIDs {
		combinedIDs[id] = struct{}{}
	}
	for _, id := range localIDs {
		combinedIDs[id] = struct{}{}
	}

	if len(combinedIDs) == 0 {
		return []int{}, nil
	}

	idList := make([]int, 0, len(combinedIDs))
	for id := range combinedIDs {
		idList = append(idList, id)
	}
	return idList, nil
}

// getItemTypeTabs queries the database for all unique item types to display as filter tabs.
func getItemTypeTabs() []ItemTypeTab {
	var itemTypes []ItemTypeTab
	rows, err := db.Query("SELECT DISTINCT item_type FROM rms_item_cache WHERE item_type IS NOT NULL AND item_type != '' ORDER BY item_type ASC")
	if err != nil {
		log.Printf("⚠️ Could not query for item types: %v", err)
		return itemTypes // Return empty slice on error
	}
	defer rows.Close()

	for rows.Next() {
		var itemType string
		if err := rows.Scan(&itemType); err != nil {
			log.Printf("⚠️ Failed to scan item type: %v", err)
			continue
		}
		itemTypes = append(itemTypes, mapItemTypeToTabData(itemType))
	}
	return itemTypes
}

// getLastUpdateTime is a generic helper to get the most recent timestamp from a table.
func getLastUpdateTime(tableName, columnName string) string {
	var lastTimestamp sql.NullString
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", columnName, tableName)
	err := db.QueryRow(query).Scan(&lastTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last update time for %s: %v", tableName, err)
	}
	if lastTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastTimestamp.String)
		if err == nil {
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

// generateEventIntervals creates a list of event occurrences within a given time window.
func generateEventIntervals(viewStart, viewEnd time.Time, events []EventDefinition, activeDates map[string]struct{}) []map[string]interface{} {
	var intervals []map[string]interface{}
	loc := viewStart.Location()
	currentDay := time.Date(viewStart.Year(), viewStart.Month(), viewStart.Day(), 0, 0, 0, 0, loc)

	for currentDay.Before(viewEnd) {
		dateStr := currentDay.Format("2006-01-02")
		if _, exists := activeDates[dateStr]; !exists {
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
				eventStartStr := fmt.Sprintf("%s %s", currentDay.Format("2006-01-02"), event.StartTime)
				eventEndStr := fmt.Sprintf("%s %s", currentDay.Format("2006-01-02"), event.EndTime)

				eventStart, err1 := time.ParseInLocation("2006-01-02 15:04", eventStartStr, loc)
				eventEnd, err2 := time.ParseInLocation("2006-01-02 15:04", eventEndStr, loc)

				if err1 != nil || err2 != nil {
					continue
				}

				if eventStart.Before(viewEnd) && eventEnd.After(viewStart) {
					intervals = append(intervals, map[string]interface{}{
						"name":  event.Name,
						"start": eventStart.Format("2006-01-02 15:04"),
						"end":   eventEnd.Format("2006-01-02 15:04"),
					})
				}
			}
		}
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
		tab.IconItemID = 1750
	case "Armor":
		tab.ShortName = ""
		tab.IconItemID = 2316
	case "Card":
		tab.ShortName = ""
		tab.IconItemID = 4133
	case "Delayed-Consumable":
		tab.ShortName = ""
		tab.IconItemID = 610
	case "Healing Item":
		tab.ShortName = ""
		tab.IconItemID = 501
	case "Miscellaneous":
		tab.ShortName = ""
		tab.IconItemID = 909
	case "Monster Egg":
		tab.ShortName = ""
		tab.IconItemID = 9001
	case "Pet Armor":
		tab.ShortName = ""
		tab.IconItemID = 5183
	case "Taming Item":
		tab.ShortName = ""
		tab.IconItemID = 632
	case "Usable Item":
		tab.ShortName = ""
		tab.IconItemID = 603
	case "Weapon":
		tab.ShortName = ""
		tab.IconItemID = 1162
	}
	return tab
}

// -------------------------
// -- HTTP HANDLERS --
// -------------------------

// summaryHandler serves the main summary page.
func summaryHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchQuery := r.FormValue("query")
	selectedType := r.FormValue("type")

	formSubmitted := len(r.Form) > 0
	showAll := false
	if formSubmitted && r.FormValue("only_available") != "true" {
		showAll = true
	}

	var params []interface{}
	baseQuery := `
        SELECT
            i.name_of_the_item,
            rms.name_pt,
            MIN(i.item_id) as item_id,
            MIN(CASE WHEN i.is_available = 1 THEN CAST(REPLACE(i.price, ',', '') AS INTEGER) ELSE NULL END) as lowest_price,
            MAX(CASE WHEN i.is_available = 1 THEN CAST(REPLACE(i.price, ',', '') AS INTEGER) ELSE NULL END) as highest_price,
            SUM(CASE WHEN i.is_available = 1 THEN 1 ELSE 0 END) as listing_count
        FROM items i
        LEFT JOIN rms_item_cache rms ON i.item_id = rms.item_id
    `
	var whereConditions []string

	if searchClause, searchParams, err := buildItemSearchClause(searchQuery, "i"); err != nil {
		http.Error(w, "Failed to build item search query", http.StatusInternalServerError)
		return
	} else if searchClause != "" {
		whereConditions = append(whereConditions, searchClause)
		params = append(params, searchParams...)
	}

	if selectedType != "" {
		whereConditions = append(whereConditions, "rms.item_type = ?")
		params = append(params, selectedType)
	}

	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	groupByClause := " GROUP BY i.name_of_the_item, rms.name_pt"
	havingClause := ""
	if !showAll {
		havingClause = " HAVING listing_count > 0"
	}

	allowedSorts := map[string]string{
		"name": "i.name_of_the_item", "item_id": "item_id", "listings": "listing_count",
		"lowest_price": "lowest_price", "highest_price": "highest_price",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "highest_price", "DESC")

	query := fmt.Sprintf("%s %s %s %s %s, i.name_of_the_item ASC;", baseQuery, whereClause, groupByClause, havingClause, orderByClause)

	rows, err := db.Query(query, params...)
	if err != nil {
		log.Printf("❌ Summary query error: %v, Query: %s, Params: %v", err, query, params)
		http.Error(w, "Database query for summary failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ItemSummary
	for rows.Next() {
		var item ItemSummary
		if err := rows.Scan(&item.Name, &item.NamePT, &item.ItemID, &item.LowestPrice, &item.HighestPrice, &item.ListingCount); err != nil {
			log.Printf("⚠️ Failed to scan summary row: %v", err)
			continue
		}
		items = append(items, item)
	}

	var totalVisitors int
	db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&totalVisitors)

	data := SummaryPageData{
		Items:          items,
		SearchQuery:    searchQuery,
		SortBy:         sortBy,
		Order:          order,
		ShowAll:        showAll,
		LastScrapeTime: getLastUpdateTime("scrape_history", "timestamp"),
		ItemTypes:      getItemTypeTabs(),
		SelectedType:   selectedType,
		TotalVisitors:  totalVisitors,
	}
	renderTemplate(w, "index.html", data)
}

// fullListHandler shows the complete, detailed market list.
func fullListHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchQuery := r.FormValue("query")
	storeNameQuery := r.FormValue("store_name")
	selectedCols := r.Form["cols"]
	selectedType := r.FormValue("type")

	formSubmitted := len(r.Form) > 0
	showAll := false
	if formSubmitted && r.FormValue("only_available") != "true" {
		showAll = true
	}

	var allStoreNames []string
	storeRows, err := db.Query("SELECT DISTINCT store_name FROM items WHERE is_available = 1 ORDER BY store_name ASC")
	if err != nil {
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
		{ID: "item_id", DisplayName: "Item ID"}, {ID: "quantity", DisplayName: "Quantity"},
		{ID: "store_name", DisplayName: "Store Name"}, {ID: "seller_name", DisplayName: "Seller Name"},
		{ID: "map_name", DisplayName: "Map Name"}, {ID: "map_coordinates", DisplayName: "Map Coords"},
		{ID: "retrieved", DisplayName: "Date Retrieved"},
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
		"name": "i.name_of_the_item", "item_id": "i.item_id", "quantity": "i.quantity",
		"price": "CAST(REPLACE(i.price, ',', '') AS INTEGER)", "store": "i.store_name", "seller": "i.seller_name",
		"retrieved": "i.date_and_time_retrieved", "store_name": "i.store_name", "map_name": "i.map_name",
		"availability": "i.is_available",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "price", "DESC")

	var whereConditions []string
	var queryParams []interface{}

	if searchClause, searchParams, err := buildItemSearchClause(searchQuery, "i"); err != nil {
		http.Error(w, "Failed to build item search query", http.StatusInternalServerError)
		return
	} else if searchClause != "" {
		whereConditions = append(whereConditions, searchClause)
		queryParams = append(queryParams, searchParams...)
	}

	if storeNameQuery != "" {
		whereConditions = append(whereConditions, "i.store_name = ?")
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
		SELECT i.id, i.name_of_the_item, rms.name_pt, i.item_id, i.quantity, i.price, i.store_name, i.seller_name, i.date_and_time_retrieved, i.map_name, i.map_coordinates, i.is_available
		FROM items i 
		LEFT JOIN rms_item_cache rms ON i.item_id = rms.item_id
	`
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	query := fmt.Sprintf(`%s %s %s;`, baseQuery, whereClause, orderByClause)

	rows, err := db.Query(query, queryParams...)
	if err != nil {
		log.Printf("❌ Database query error: %v", err)
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []Item
	for rows.Next() {
		var item Item
		var retrievedTime string
		err := rows.Scan(&item.ID, &item.Name, &item.NamePT, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &retrievedTime, &item.MapName, &item.MapCoordinates, &item.IsAvailable)
		if err != nil {
			log.Printf("⚠️ Failed to scan row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, retrievedTime); err == nil {
			item.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			item.Timestamp = retrievedTime
		}
		items = append(items, item)
	}

	data := PageData{
		Items: items, SearchQuery: searchQuery, StoreNameQuery: storeNameQuery, AllStoreNames: allStoreNames,
		SortBy: sortBy, Order: order, ShowAll: showAll, LastScrapeTime: getLastUpdateTime("scrape_history", "timestamp"),
		VisibleColumns: visibleColumns, AllColumns: allCols, ColumnParams: template.URL(columnParams.Encode()),
		ItemTypes: getItemTypeTabs(), SelectedType: selectedType,
	}
	renderTemplate(w, "full_list.html", data)
}

// activityHandler serves the page for recent market activity.
func activityHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchQuery := r.FormValue("query")
	soldOnly := r.FormValue("sold_only") == "true"
	const eventsPerPage = 50

	var whereConditions []string
	var params []interface{}

	if searchClause, searchParams, err := buildItemSearchClause(searchQuery, "me"); err != nil {
		http.Error(w, "Failed to build item search query", http.StatusInternalServerError)
		return
	} else if searchClause != "" {
		whereConditions = append(whereConditions, searchClause)
		params = append(params, searchParams...)
	}

	if soldOnly {
		whereConditions = append(whereConditions, "event_type = ?")
		params = append(params, "SOLD")
	}
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	var totalEvents int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM market_events me LEFT JOIN rms_item_cache rms ON me.item_id = rms.item_id %s", whereClause)
	if err := db.QueryRow(countQuery, params...).Scan(&totalEvents); err != nil {
		log.Printf("❌ Could not count market events: %v", err)
		http.Error(w, "Could not count market events", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalEvents, eventsPerPage)
	query := fmt.Sprintf(`
        SELECT me.event_timestamp, me.event_type, me.item_name, rms.name_pt, me.item_id, me.details
        FROM market_events me
        LEFT JOIN rms_item_cache rms ON me.item_id = rms.item_id %s
        ORDER BY me.event_timestamp DESC LIMIT ? OFFSET ?`, whereClause)

	finalParams := append(params, eventsPerPage, pagination.Offset)
	eventRows, err := db.Query(query, finalParams...)
	if err != nil {
		log.Printf("❌ Could not query for market events: %v", err)
		http.Error(w, "Could not query for market events", http.StatusInternalServerError)
		return
	}
	defer eventRows.Close()

	var marketEvents []MarketEvent
	for eventRows.Next() {
		var event MarketEvent
		var detailsStr, timestampStr string
		if err := eventRows.Scan(&timestampStr, &event.EventType, &event.ItemName, &event.NamePT, &event.ItemID, &detailsStr); err != nil {
			log.Printf("⚠️ Failed to scan market event row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			event.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			event.Timestamp = timestampStr
		}
		json.Unmarshal([]byte(detailsStr), &event.Details)
		marketEvents = append(marketEvents, event)
	}

	data := ActivityPageData{
		MarketEvents:   marketEvents,
		LastScrapeTime: getLastUpdateTime("scrape_history", "timestamp"),
		SearchQuery:    searchQuery,
		SoldOnly:       soldOnly,
		Pagination:     pagination,
	}
	renderTemplate(w, "activity.html", data)
}

// itemHistoryHandler serves the detailed history page for a single item
func itemHistoryHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	itemName := r.FormValue("name")
	if itemName == "" {
		http.Error(w, "Item name is required", http.StatusBadRequest)
		return
	}

	var itemID int
	var itemNamePT sql.NullString
	db.QueryRow(`
		SELECT i.item_id, rms.name_pt 
		FROM items i 
		LEFT JOIN rms_item_cache rms ON i.item_id = rms.item_id
		WHERE i.name_of_the_item = ? AND i.item_id > 0 
		LIMIT 1`, itemName).Scan(&itemID, &itemNamePT)

	var rmsItemDetails *RMSItem
	if itemID > 0 {
		cachedItem, err := getItemDetailsFromCache(itemID)
		if err == nil {
			rmsItemDetails = cachedItem
		} else {
			scrapedItem, scrapeErr := scrapeRMSItemDetails(itemID)
			if scrapeErr == nil {
				rmsItemDetails = scrapedItem
				if saveErr := saveItemDetailsToCache(rmsItemDetails); saveErr != nil {
					log.Printf("⚠️ Failed to save item ID %d to cache: %v", itemID, saveErr)
				}
			}
		}
	}

	currentListingsQuery := `
		SELECT CAST(REPLACE(price, ',', '') AS INTEGER) as price_int, quantity, store_name, seller_name, map_name, map_coordinates, date_and_time_retrieved
		FROM items WHERE name_of_the_item = ? AND is_available = 1 ORDER BY price_int ASC;
	`
	rowsCurrent, err := db.Query(currentListingsQuery, itemName)
	if err != nil {
		log.Printf("❌ Current listings query error: %v", err)
		http.Error(w, "Database query for current listings failed", http.StatusInternalServerError)
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
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			listing.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			listing.Timestamp = timestampStr
		}
		currentListings = append(currentListings, listing)
	}

	var currentLowest, currentHighest *ItemListing
	if len(currentListings) > 0 {
		currentLowest = &currentListings[0]
		currentHighest = &currentListings[len(currentListings)-1]
	}
	currentLowestJSON, _ := json.Marshal(currentLowest)
	currentHighestJSON, _ := json.Marshal(currentHighest)

	var overallLowest, overallHighest sql.NullInt64
	db.QueryRow(`
        SELECT MIN(CAST(REPLACE(price, ',', '') AS INTEGER)), MAX(CAST(REPLACE(price, ',', '') AS INTEGER))
        FROM items WHERE name_of_the_item = ?;
    `, itemName).Scan(&overallLowest, &overallHighest)

	priceChangeQuery := `
		WITH RankedItems AS (
			SELECT quantity, CAST(REPLACE(price, ',', '') AS INTEGER) as price_int, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates,
				ROW_NUMBER() OVER(PARTITION BY date_and_time_retrieved ORDER BY CAST(REPLACE(price, ',', '') AS INTEGER) ASC) as rn_asc,
				ROW_NUMBER() OVER(PARTITION BY date_and_time_retrieved ORDER BY CAST(REPLACE(price, ',', '') AS INTEGER) DESC) as rn_desc
			FROM items WHERE name_of_the_item = ?
		)
		SELECT t_lowest.date_and_time_retrieved, t_lowest.price_int, t_lowest.quantity, t_lowest.store_name, t_lowest.seller_name, t_lowest.map_name, t_lowest.map_coordinates,
			t_highest.price_int, t_highest.quantity, t_highest.store_name, t_highest.seller_name, t_highest.map_name, t_highest.map_coordinates
		FROM (SELECT * FROM RankedItems WHERE rn_asc = 1) AS t_lowest
		JOIN (SELECT * FROM RankedItems WHERE rn_desc = 1) AS t_highest ON t_lowest.date_and_time_retrieved = t_highest.date_and_time_retrieved
		ORDER BY t_lowest.date_and_time_retrieved ASC;
    `
	rows, err := db.Query(priceChangeQuery, itemName)
	if err != nil {
		log.Printf("❌ History change query error: %v", err)
		http.Error(w, "Database query for changes failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	priceEvents := make(map[string]PricePointDetails)
	for rows.Next() {
		var p PricePointDetails
		var timestampStr string
		err := rows.Scan(&timestampStr, &p.LowestPrice, &p.LowestQuantity, &p.LowestStoreName, &p.LowestSellerName, &p.LowestMapName, &p.LowestMapCoords,
			&p.HighestPrice, &p.HighestQuantity, &p.HighestStoreName, &p.HighestSellerName, &p.HighestMapName, &p.HighestMapCoords)
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
		if err := scrapeHistoryRows.Scan(&ts); err == nil {
			allScrapeTimes = append(allScrapeTimes, ts)
		}
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
			if finalPriceHistory[len(finalPriceHistory)-1].LowestPrice != fullPriceHistory[i].LowestPrice ||
				finalPriceHistory[len(finalPriceHistory)-1].HighestPrice != fullPriceHistory[i].HighestPrice {
				finalPriceHistory = append(finalPriceHistory, fullPriceHistory[i])
			}
		}
	}
	priceHistoryJSON, _ := json.Marshal(finalPriceHistory)

	const listingsPerPage = 50
	var totalListings int
	db.QueryRow("SELECT COUNT(*) FROM items WHERE name_of_the_item = ?", itemName).Scan(&totalListings)

	pagination := newPaginationData(r, totalListings, listingsPerPage)
	allListingsQuery := `
		SELECT i.id, i.name_of_the_item, rms.name_pt, i.item_id, i.quantity, i.price, i.store_name, i.seller_name, i.date_and_time_retrieved, i.map_name, i.map_coordinates, i.is_available
		FROM items i 
		LEFT JOIN rms_item_cache rms ON i.item_id = rms.item_id 
		WHERE i.name_of_the_item = ? ORDER BY i.is_available DESC, i.date_and_time_retrieved DESC LIMIT ? OFFSET ?;`
	rowsAll, err := db.Query(allListingsQuery, itemName, listingsPerPage, pagination.Offset)
	if err != nil {
		log.Printf("❌ All listings query error: %v", err)
		http.Error(w, "Database query for all listings failed", http.StatusInternalServerError)
		return
	}
	defer rowsAll.Close()

	var allListings []Item
	for rowsAll.Next() {
		var listing Item
		var timestampStr string
		if err := rowsAll.Scan(&listing.ID, &listing.Name, &listing.NamePT, &listing.ItemID, &listing.Quantity, &listing.Price, &listing.StoreName, &listing.SellerName, &timestampStr, &listing.MapName, &listing.MapCoordinates, &listing.IsAvailable); err != nil {
			log.Printf("⚠️ Failed to scan all listing row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			listing.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			listing.Timestamp = timestampStr
		}
		allListings = append(allListings, listing)
	}

	data := HistoryPageData{
		ItemName:           itemName,
		ItemNamePT:         itemNamePT,
		PriceDataJSON:      template.JS(priceHistoryJSON),
		OverallLowest:      int(overallLowest.Int64),
		OverallHighest:     int(overallHighest.Int64),
		CurrentLowestJSON:  template.JS(currentLowestJSON),
		CurrentHighestJSON: template.JS(currentHighestJSON),
		ItemDetails:        rmsItemDetails,
		AllListings:        allListings,
		LastScrapeTime:     getLastUpdateTime("scrape_history", "timestamp"),
		TotalListings:      totalListings,
		Pagination:         pagination,
	}
	renderTemplate(w, "history.html", data)
}

// playerCountHandler serves the page with a graph of online player history.
func playerCountHandler(w http.ResponseWriter, r *http.Request) {
	interval := r.URL.Query().Get("interval")
	if interval == "" {
		interval = "7d"
	}

	now := time.Now()
	var viewStart time.Time

	switch interval {
	case "6h":
		viewStart = now.Add(-6 * time.Hour)
	case "24h":
		viewStart = now.Add(-24 * time.Hour)
	case "30d":
		viewStart = now.Add(-30 * 24 * time.Hour)
	case "7d":
		fallthrough
	default:
		interval = "7d"
		viewStart = now.Add(-7 * 24 * time.Hour)
	}

	whereClause := "WHERE timestamp >= ?"
	params := []interface{}{viewStart.Format(time.RFC3339)}
	const maxGraphDataPoints = 720
	var query string
	duration := now.Sub(viewStart)

	if duration.Minutes() > maxGraphDataPoints {
		bucketSizeInSeconds := int(duration.Seconds()) / maxGraphDataPoints
		if bucketSizeInSeconds < 60 {
			bucketSizeInSeconds = 60
		}
		log.Printf("📊 Player graph: Downsampling data for '%s' interval. Bucket size: %d seconds.", interval, bucketSizeInSeconds)
		query = fmt.Sprintf(`
			SELECT MIN(timestamp), CAST(AVG(count) AS INTEGER), CAST(AVG(seller_count) AS INTEGER)
			FROM player_history %s GROUP BY CAST(unixepoch(timestamp) / %d AS INTEGER) ORDER BY 1 ASC`, whereClause, bucketSizeInSeconds)
	} else {
		log.Printf("📊 Player graph: Fetching all data points for '%s' interval.", interval)
		query = fmt.Sprintf("SELECT timestamp, count, seller_count FROM player_history %s ORDER BY timestamp ASC", whereClause)
	}

	rows, err := db.Query(query, params...)
	if err != nil {
		log.Printf("❌ Could not query for player history: %v", err)
		http.Error(w, "Could not query for player history", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var playerHistory []PlayerCountPoint
	activeDatesWithData := make(map[string]struct{})
	for rows.Next() {
		var point PlayerCountPoint
		var timestampStr string
		var sellerCount sql.NullInt64
		if err := rows.Scan(&timestampStr, &point.Count, &sellerCount); err != nil {
			log.Printf("⚠️ Failed to scan player history row: %v", err)
			continue
		}
		point.SellerCount = int(sellerCount.Int64)
		point.Delta = point.Count - point.SellerCount

		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			point.Timestamp = parsedTime.Format("2006-01-02 15:04")
			activeDatesWithData[parsedTime.Format("2006-01-02")] = struct{}{}
		} else {
			point.Timestamp = timestampStr
		}
		playerHistory = append(playerHistory, point)
	}
	playerHistoryJSON, _ := json.Marshal(playerHistory)
	eventIntervals := generateEventIntervals(viewStart, now, definedEvents, activeDatesWithData)
	eventIntervalsJSON, _ := json.Marshal(eventIntervals)

	var latestCount, latestSellerCount int
	db.QueryRow("SELECT count, seller_count FROM player_history ORDER BY timestamp DESC LIMIT 1").Scan(&latestCount, &latestSellerCount)
	latestActivePlayers := latestCount - latestSellerCount

	var historicalMaxActive int
	var historicalMaxTimestampStr sql.NullString
	db.QueryRow("SELECT (count - COALESCE(seller_count, 0)), timestamp FROM player_history ORDER BY 1 DESC LIMIT 1").Scan(&historicalMaxActive, &historicalMaxTimestampStr)

	historicalMaxTime := "N/A"
	if historicalMaxTimestampStr.Valid {
		if parsedTime, err := time.Parse(time.RFC3339, historicalMaxTimestampStr.String); err == nil {
			historicalMaxTime = parsedTime.Format("2006-01-02 15:04")
		}
	}

	data := PlayerCountPageData{
		PlayerDataJSON: template.JS(playerHistoryJSON), LastScrapeTime: getLastUpdateTime("player_history", "timestamp"),
		SelectedInterval: interval, EventDataJSON: template.JS(eventIntervalsJSON), LatestActivePlayers: latestActivePlayers,
		HistoricalMaxActivePlayers: historicalMaxActive, HistoricalMaxActivePlayersTime: historicalMaxTime,
	}
	renderTemplate(w, "players.html", data)
}

// characterHandler serves the player characters page.
func characterHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchName := r.FormValue("name_query")
	selectedClass := r.FormValue("class_filter")
	selectedGuild := r.FormValue("guild_filter")
	selectedCols := r.Form["cols"]
	graphFilter := r.Form["graph_filter"]

	isInitialLoad := len(r.Form) == 0
	const playersPerPage = 50

	specialPlayers := map[string]bool{
		"Purity Ring": true, "Bafo MvP": true, "franco bs": true, "franco alchie": true, "Afanei": true,
		"GiupSankino": true, "MacroBot1000": true, "Sileeent": true, "Shiiv": true, "Majim Lipe": true,
		"Solidao": true, "WildTig3r": true, "No Glory": true,
	}
	guildMasters := make(map[string]bool)
	masterRows, err := db.Query("SELECT DISTINCT master FROM guilds WHERE master IS NOT NULL AND master != ''")
	if err == nil {
		defer masterRows.Close()
		for masterRows.Next() {
			var masterName string
			if err := masterRows.Scan(&masterName); err == nil {
				guildMasters[masterName] = true
			}
		}
	}

	allCols := []Column{
		{ID: "rank", DisplayName: "Rank"}, {ID: "base_level", DisplayName: "Base Lvl"}, {ID: "job_level", DisplayName: "Job Lvl"},
		{ID: "experience", DisplayName: "Exp %"}, {ID: "zeny", DisplayName: "Zeny"}, {ID: "class", DisplayName: "Class"},
		{ID: "guild", DisplayName: "Guild"}, {ID: "last_updated", DisplayName: "Last Updated"}, {ID: "last_active", DisplayName: "Last Active"},
	}
	visibleColumns := make(map[string]bool)
	columnParams := url.Values{}
	graphFilterParams := url.Values{}

	if isInitialLoad {
		visibleColumns["base_level"], visibleColumns["job_level"], visibleColumns["experience"] = true, true, true
		visibleColumns["class"], visibleColumns["guild"], visibleColumns["last_active"] = true, true, true
		for colID := range visibleColumns {
			columnParams.Add("cols", colID)
		}
		graphFilter = []string{"second"}
	} else {
		for _, col := range selectedCols {
			visibleColumns[col] = true
			columnParams.Add("cols", col)
		}
	}
	for _, f := range graphFilter {
		graphFilterParams.Add("graph_filter", f)
	}

	var allClasses []string
	classRows, err := db.Query("SELECT DISTINCT class FROM characters ORDER BY class ASC")
	if err == nil {
		defer classRows.Close()
		for classRows.Next() {
			var className string
			if err := classRows.Scan(&className); err == nil {
				allClasses = append(allClasses, className)
			}
		}
	}

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

	classDistribution := make(map[string]int)
	distQuery := fmt.Sprintf("SELECT class, COUNT(*) FROM characters %s GROUP BY class", whereClause)
	distRows, err := db.Query(distQuery, params...)
	if err == nil {
		defer distRows.Close()
		for distRows.Next() {
			var className string
			var count int
			if err := distRows.Scan(&className, &count); err == nil {
				classDistribution[className] = count
			}
		}
	}

	noviceClasses := map[string]bool{"Aprendiz": true, "Super Aprendiz": true}
	firstClasses := map[string]bool{"Arqueiro": true, "Espadachim": true, "Gatuno": true, "Mago": true, "Mercador": true, "Noviço": true}
	secondClasses := map[string]bool{"Alquimista": true, "Arruaceiro": true, "Bardo": true, "Bruxo": true, "Cavaleiro": true, "Caçador": true, "Ferreiro": true, "Mercenário": true, "Monge": true, "Odalisca": true, "Sacerdote": true, "Sábio": true, "Templário": true}
	graphFilterMap := make(map[string]bool)
	for _, f := range graphFilter {
		graphFilterMap[f] = true
	}

	chartData := make(map[string]int)
	for class, count := range classDistribution {
		if noviceClasses[class] && graphFilterMap["novice"] {
			chartData[class] = count
		} else if firstClasses[class] && graphFilterMap["first"] {
			chartData[class] = count
		} else if secondClasses[class] && graphFilterMap["second"] {
			chartData[class] = count
		}
	}
	classDistJSON, _ := json.Marshal(chartData)

	allowedSorts := map[string]string{
		"rank": "rank", "name": "name", "base_level": "base_level", "job_level": "job_level", "experience": "experience",
		"zeny": "zeny", "class": "class", "guild": "guild_name", "last_updated": "last_updated", "last_active": "last_active",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "rank", "ASC")

	var totalPlayers int
	var totalZeny sql.NullInt64
	countQuery := fmt.Sprintf("SELECT COUNT(*), SUM(zeny) FROM characters %s", whereClause)
	if err := db.QueryRow(countQuery, params...).Scan(&totalPlayers, &totalZeny); err != nil {
		http.Error(w, "Could not count player characters", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalPlayers, playersPerPage)
	query := fmt.Sprintf(`SELECT rank, name, base_level, job_level, experience, class, guild_name, zeny, last_updated, last_active
		FROM characters %s %s LIMIT ? OFFSET ?`, whereClause, orderByClause)
	finalParams := append(params, playersPerPage, pagination.Offset)

	rows, err := db.Query(query, finalParams...)
	if err != nil {
		http.Error(w, "Could not query for player characters", http.StatusInternalServerError)
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
		if t, err := time.Parse(time.RFC3339, lastUpdatedStr); err == nil {
			p.LastUpdated = t.Format("2006-01-02 15:04")
		}
		if t, err := time.Parse(time.RFC3339, lastActiveStr); err == nil {
			p.LastActive = t.Format("2006-01-02 15:04")
		}
		p.IsActive = (lastUpdatedStr == lastActiveStr) && lastUpdatedStr != ""
		p.IsGuildLeader = guildMasters[p.Name]
		p.IsSpecial = specialPlayers[p.Name]
		players = append(players, p)
	}

	data := CharacterPageData{
		Players: players, LastScrapeTime: getLastUpdateTime("characters", "last_updated"), SearchName: searchName,
		SelectedClass: selectedClass, SelectedGuild: selectedGuild, AllClasses: allClasses, SortBy: sortBy, Order: order,
		VisibleColumns: visibleColumns, AllColumns: allCols, ColumnParams: template.URL(columnParams.Encode()),
		Pagination: pagination, TotalPlayers: totalPlayers, TotalZeny: totalZeny.Int64,
		ClassDistributionJSON: template.JS(classDistJSON), GraphFilter: graphFilterMap, GraphFilterParams: template.URL(graphFilterParams.Encode()),
		HasChartData: len(chartData) > 1,
	}
	renderTemplate(w, "characters.html", data)
}

// guildHandler serves the player guilds page.
func guildHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchName := r.FormValue("name_query")
	const guildsPerPage = 50

	var whereConditions []string
	var params []interface{}
	if searchName != "" {
		whereConditions = append(whereConditions, "g.name LIKE ?")
		params = append(params, "%"+searchName+"%")
	}
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	allowedSorts := map[string]string{
		"rank": "rank", "name": "g.name", "level": "g.level", "master": "g.master",
		"members": "member_count", "zeny": "total_zeny", "avg_level": "avg_base_level",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "level", "DESC")

	var totalGuilds int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM guilds g %s", whereClause)
	if err := db.QueryRow(countQuery, params...).Scan(&totalGuilds); err != nil {
		http.Error(w, "Could not count guilds", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalGuilds, guildsPerPage)
	query := fmt.Sprintf(`
		SELECT g.name, g.level, g.experience, g.master, g.emblem_url,
			COALESCE(cs.member_count, 0) as member_count,
			COALESCE(cs.total_zeny, 0) as total_zeny,
			COALESCE(cs.avg_base_level, 0) as avg_base_level
		FROM guilds g
		LEFT JOIN (
			SELECT guild_name, COUNT(*) as member_count, SUM(zeny) as total_zeny, AVG(base_level) as avg_base_level
			FROM characters
			WHERE guild_name IS NOT NULL AND guild_name != ''
			GROUP BY guild_name
		) cs ON g.name = cs.guild_name
		%s %s LIMIT ? OFFSET ?`, whereClause, orderByClause)
	finalParams := append(params, guildsPerPage, pagination.Offset)

	rows, err := db.Query(query, finalParams...)
	if err != nil {
		log.Printf("❌ Could not query for guilds: %v", err)
		http.Error(w, "Could not query for guilds", http.StatusInternalServerError)
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

	data := GuildPageData{
		Guilds:              guilds,
		LastGuildUpdateTime: getLastUpdateTime("guilds", "last_updated"),
		SearchName:          searchName,
		SortBy:              sortBy,
		Order:               order,
		Pagination:          pagination,
		TotalGuilds:         totalGuilds,
	}
	renderTemplate(w, "guilds.html", data)
}

// mvpKillsHandler serves the page for MVP kill rankings.
func mvpKillsHandler(w http.ResponseWriter, r *http.Request) {
	headers := []MvpHeader{{MobID: "total", MobName: "Total Kills"}}
	for _, mobID := range mvpMobIDs {
		headers = append(headers, MvpHeader{MobID: mobID, MobName: mvpNames[mobID]})
	}

	allowedSorts := map[string]string{"name": "character_name"}
	var sumParts []string
	for _, mobID := range mvpMobIDs {
		colName := fmt.Sprintf("mvp_%s", mobID)
		allowedSorts[mobID] = colName
		sumParts = append(sumParts, colName)
	}
	allowedSorts["total"] = fmt.Sprintf("(%s)", strings.Join(sumParts, " + "))

	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "total", "DESC")

	query := fmt.Sprintf("SELECT * FROM character_mvp_kills %s", orderByClause)
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("❌ Could not query for MVP kills: %v", err)
		http.Error(w, "Could not query MVP kills", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

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
		player := MvpKillEntry{Kills: make(map[string]int)}
		totalKills := 0
		for i, colName := range cols {
			val := columns[i]
			if colName == "character_name" {
				player.CharacterName = val.(string)
			} else if strings.HasPrefix(colName, "mvp_") {
				mobID := strings.TrimPrefix(colName, "mvp_")
				displayKillCount := int(val.(int64)) - MvpKillCountOffset
				if displayKillCount < 0 {
					displayKillCount = 0
				}
				player.Kills[mobID] = displayKillCount
				totalKills += displayKillCount
			}
		}
		player.TotalKills = totalKills
		players = append(players, player)
	}

	data := MvpKillPageData{
		Players: players, Headers: headers, SortBy: sortBy, Order: order,
		LastScrapeTime: getLastUpdateTime("characters", "last_updated"),
	}
	renderTemplate(w, "mvp_kills.html", data)
}

// characterDetailHandler serves the detailed information page for a single character.
func characterDetailHandler(w http.ResponseWriter, r *http.Request) {
	charName := r.URL.Query().Get("name")
	if charName == "" {
		http.Error(w, "Character name is required", http.StatusBadRequest)
		return
	}

	classImages := map[string]string{
		"Aprendiz": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/8/8b/Icon_jobs_0.png", "Super Aprendiz": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c7/Icon_jobs_4001.png",
		"Arqueiro": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/97/Icon_jobs_3.png", "Espadachim": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/5/5b/Icon_jobs_1.png",
		"Gatuno": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/3/3c/Icon_jobs_6.png", "Mago": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/99/Icon_jobs_2.png",
		"Mercador": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/9e/Icon_jobs_5.png", "Noviço": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c5/Icon_jobs_4.png",
		"Alquimista": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/c/c7/Icon_jobs_18.png", "Arruaceiro": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/4/48/Icon_jobs_17.png",
		"Bardo": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/6/69/Icon_jobs_19.png", "Bruxo": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/0/09/Icon_jobs_9.png",
		"Cavaleiro": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/1/1d/Icon_jobs_7.png", "Caçador": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/e/eb/Icon_jobs_11.png",
		"Ferreiro": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/7/7b/Icon_jobs_10.png", "Mercenário": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/9/9c/Icon_jobs_12.png",
		"Monge": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/4/44/Icon_jobs_15.png", "Odalisca": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/d/dc/Icon_jobs_20.png",
		"Sacerdote": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/3/3a/Icon_jobs_8.png", "Sábio": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/0/0e/Icon_jobs_16.png",
		"Templário": "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/e/e1/Icon_jobs_14.png",
	}

	var p PlayerCharacter
	var lastUpdatedStr, lastActiveStr string
	query := `SELECT rank, name, base_level, job_level, experience, class, guild_name, zeny, last_updated, last_active FROM characters WHERE name = ?`
	err := db.QueryRow(query, charName).Scan(&p.Rank, &p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.GuildName, &p.Zeny, &lastUpdatedStr, &lastActiveStr)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Character not found", http.StatusNotFound)
		} else {
			log.Printf("❌ Could not query for character '%s': %v", charName, err)
			http.Error(w, "Database query for character failed", http.StatusInternalServerError)
		}
		return
	}

	if t, err := time.Parse(time.RFC3339, lastUpdatedStr); err == nil {
		p.LastUpdated = t.Format("2006-01-02 15:04")
	}
	if t, err := time.Parse(time.RFC3339, lastActiveStr); err == nil {
		p.LastActive = t.Format("2006-01-02 15:04")
	}
	p.IsActive = (lastUpdatedStr == lastActiveStr) && lastUpdatedStr != ""

	var guild *Guild
	if p.GuildName.Valid {
		g := Guild{}
		guildQuery := `SELECT name, level, master, (SELECT COUNT(*) FROM characters WHERE guild_name = guilds.name) FROM guilds WHERE name = ?`
		if err := db.QueryRow(guildQuery, p.GuildName.String).Scan(&g.Name, &g.Level, &g.Master, &g.MemberCount); err == nil {
			guild = &g
			p.IsGuildLeader = (p.Name == g.Master)
		}
	}

	mvpKills := MvpKillEntry{CharacterName: p.Name, Kills: make(map[string]int)}
	var mvpCols []string
	for _, mobID := range mvpMobIDs {
		mvpCols = append(mvpCols, fmt.Sprintf("mvp_%s", mobID))
	}
	mvpQuery := fmt.Sprintf("SELECT %s FROM character_mvp_kills WHERE character_name = ?", strings.Join(mvpCols, ", "))
	scanDest := make([]interface{}, len(mvpMobIDs))
	for i := range mvpMobIDs {
		scanDest[i] = new(int)
	}

	if err := db.QueryRow(mvpQuery, charName).Scan(scanDest...); err == nil {
		totalKills := 0
		for i, mobID := range mvpMobIDs {
			killCount := *scanDest[i].(*int)
			mvpKills.Kills[mobID] = killCount
			totalKills += killCount
		}
		mvpKills.TotalKills = totalKills
	}

	var mvpHeaders []MvpHeader
	for _, mobID := range mvpMobIDs {
		if name, ok := mvpNames[mobID]; ok {
			mvpHeaders = append(mvpHeaders, MvpHeader{MobID: mobID, MobName: name})
		}
	}

	var guildHistory []CharacterChangelog
	guildHistoryQuery := `SELECT change_time, activity_description FROM character_changelog
		WHERE character_name = ? AND (activity_description LIKE '%joined guild%' OR activity_description LIKE '%left guild%')
		ORDER BY change_time DESC`
	guildHistoryRows, err := db.Query(guildHistoryQuery, charName)
	if err == nil {
		defer guildHistoryRows.Close()
		for guildHistoryRows.Next() {
			var entry CharacterChangelog
			var timestampStr string
			if err := guildHistoryRows.Scan(&timestampStr, &entry.ActivityDescription); err == nil {
				if t, err := time.Parse(time.RFC3339, timestampStr); err == nil {
					entry.ChangeTime = t.Format("2006-01-02")
				}
				guildHistory = append(guildHistory, entry)
			}
		}
	}

	const entriesPerPage = 25
	var totalChangelogEntries int
	db.QueryRow("SELECT COUNT(*) FROM character_changelog WHERE character_name = ?", charName).Scan(&totalChangelogEntries)

	pagination := newPaginationData(r, totalChangelogEntries, entriesPerPage)
	changelogQuery := `SELECT change_time, activity_description FROM character_changelog WHERE character_name = ? ORDER BY change_time DESC LIMIT ? OFFSET ?`
	changelogRows, err := db.Query(changelogQuery, charName, entriesPerPage, pagination.Offset)
	if err != nil {
		http.Error(w, "Could not query for character changelog", http.StatusInternalServerError)
		return
	}
	defer changelogRows.Close()

	var changelogEntries []CharacterChangelog
	for changelogRows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := changelogRows.Scan(&timestampStr, &entry.ActivityDescription); err == nil {
			if t, err := time.Parse(time.RFC3339, timestampStr); err == nil {
				entry.ChangeTime = t.Format("2006-01-02 15:04:05")
			}
			changelogEntries = append(changelogEntries, entry)
		}
	}

	data := CharacterDetailPageData{
		Character:           p,
		Guild:               guild,
		MvpKills:            mvpKills,
		MvpHeaders:          mvpHeaders,
		GuildHistory:        guildHistory,
		LastScrapeTime:      getLastUpdateTime("characters", "last_updated"),
		ClassImageURL:       classImages[p.Class],
		ChangelogEntries:    changelogEntries,
		ChangelogPagination: pagination,
	}
	renderTemplate(w, "character_detail.html", data)
}

// characterChangelogHandler serves the page for recent character changes.
func characterChangelogHandler(w http.ResponseWriter, r *http.Request) {
	const entriesPerPage = 100
	var totalEntries int
	if err := db.QueryRow("SELECT COUNT(*) FROM character_changelog").Scan(&totalEntries); err != nil {
		http.Error(w, "Could not count changelog entries", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalEntries, entriesPerPage)
	query := `SELECT character_name, change_time, activity_description FROM character_changelog ORDER BY change_time DESC LIMIT ? OFFSET ?`
	rows, err := db.Query(query, entriesPerPage, pagination.Offset)
	if err != nil {
		http.Error(w, "Could not query for character changelog", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var changelogEntries []CharacterChangelog
	for rows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := rows.Scan(&entry.CharacterName, &timestampStr, &entry.ActivityDescription); err != nil {
			log.Printf("⚠️ Failed to scan character changelog row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			entry.ChangeTime = parsedTime.Format("2006-01-02 15:04:05")
		} else {
			entry.ChangeTime = timestampStr
		}
		changelogEntries = append(changelogEntries, entry)
	}

	data := CharacterChangelogPageData{
		ChangelogEntries: changelogEntries,
		LastScrapeTime:   getLastUpdateTime("characters", "last_updated"),
		Pagination:       pagination,
	}
	renderTemplate(w, "character_changelog.html", data)
}

// guildDetailHandler serves the new guild detail page.
func guildDetailHandler(w http.ResponseWriter, r *http.Request) {
	guildName := r.URL.Query().Get("name")
	if guildName == "" {
		http.Error(w, "Guild name is required", http.StatusBadRequest)
		return
	}
	const entriesPerPage = 25

	var g Guild
	guildQuery := `
        SELECT name, level, experience, master, emblem_url,
            (SELECT COUNT(*) FROM characters WHERE guild_name = guilds.name),
            COALESCE((SELECT SUM(zeny) FROM characters WHERE guild_name = guilds.name), 0),
            COALESCE((SELECT AVG(base_level) FROM characters WHERE guild_name = guilds.name), 0)
        FROM guilds WHERE name = ?`

	err := db.QueryRow(guildQuery, guildName).Scan(&g.Name, &g.Level, &g.Experience, &g.Master, &g.EmblemURL, &g.MemberCount, &g.TotalZeny, &g.AvgBaseLevel)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Guild not found", http.StatusNotFound)
		} else {
			http.Error(w, "Could not query for guild details", http.StatusInternalServerError)
		}
		return
	}

	allowedSorts := map[string]string{
		"rank": "rank", "name": "name", "base_level": "base_level", "job_level": "job_level",
		"experience": "experience", "zeny": "zeny", "class": "class", "last_active": "last_active",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "base_level", "DESC")

	membersQuery := fmt.Sprintf(`SELECT rank, name, base_level, job_level, experience, class, zeny, last_active FROM characters
		WHERE guild_name = ? %s`, orderByClause)
	rows, err := db.Query(membersQuery, guildName)
	if err != nil {
		http.Error(w, "Could not query for guild members", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var members []PlayerCharacter
	classDistribution := make(map[string]int)
	for rows.Next() {
		var p PlayerCharacter
		var lastActiveStr string
		if err := rows.Scan(&p.Rank, &p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.Zeny, &lastActiveStr); err != nil {
			log.Printf("⚠️ Failed to scan guild member row: %v", err)
			continue
		}
		classDistribution[p.Class]++
		if t, err := time.Parse(time.RFC3339, lastActiveStr); err == nil {
			p.LastActive = t.Format("2006-01-02 15:04")
		}
		p.IsGuildLeader = (p.Name == g.Master)
		members = append(members, p)
	}
	classDistJSON, _ := json.Marshal(classDistribution)

	likePattern := "%" + guildName + "%"
	var totalChangelogEntries int
	db.QueryRow("SELECT COUNT(*) FROM character_changelog WHERE activity_description LIKE ?", likePattern).Scan(&totalChangelogEntries)

	pagination := newPaginationData(r, totalChangelogEntries, entriesPerPage)
	changelogQuery := `SELECT change_time, character_name, activity_description FROM character_changelog
        WHERE activity_description LIKE ? ORDER BY change_time DESC LIMIT ? OFFSET ?`
	changelogRows, err := db.Query(changelogQuery, likePattern, entriesPerPage, pagination.Offset)
	if err != nil {
		http.Error(w, "Could not query for guild changelog", http.StatusInternalServerError)
		return
	}
	defer changelogRows.Close()

	var changelogEntries []CharacterChangelog
	for changelogRows.Next() {
		var entry CharacterChangelog
		var timestampStr string
		if err := changelogRows.Scan(&timestampStr, &entry.CharacterName, &entry.ActivityDescription); err == nil {
			if t, err := time.Parse(time.RFC3339, timestampStr); err == nil {
				entry.ChangeTime = t.Format("2006-01-02 15:04:05")
			}
			changelogEntries = append(changelogEntries, entry)
		}
	}

	data := GuildDetailPageData{
		Guild:                 g,
		Members:               members,
		LastScrapeTime:        getLastUpdateTime("guilds", "last_updated"),
		SortBy:                sortBy,
		Order:                 order,
		ClassDistributionJSON: template.JS(classDistJSON),
		HasChartData:          len(classDistribution) > 1,
		ChangelogEntries:      changelogEntries,
		ChangelogPagination:   pagination,
	}
	renderTemplate(w, "guild_detail.html", data)
}

// storeDetailHandler serves the detailed information page for a single store.
func storeDetailHandler(w http.ResponseWriter, r *http.Request) {
	storeName := r.URL.Query().Get("name")
	sellerNameQuery := r.URL.Query().Get("seller")
	if storeName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	allowedSorts := map[string]string{
		"name": "name_of_the_item", "item_id": "item_id", "quantity": "quantity",
		"price": "CAST(REPLACE(price, ',', '') AS INTEGER)",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "price", "DESC")

	var sellerName, mapName, mapCoords, mostRecentTimestampStr string
	var signatureQueryArgs []interface{}
	signatureQuery := `SELECT seller_name, map_name, map_coordinates, date_and_time_retrieved FROM items WHERE store_name = ?`
	signatureQueryArgs = append(signatureQueryArgs, storeName)
	if sellerNameQuery != "" {
		signatureQuery += " AND seller_name = ?"
		signatureQueryArgs = append(signatureQueryArgs, sellerNameQuery)
	}
	signatureQuery += ` ORDER BY date_and_time_retrieved DESC, id DESC LIMIT 1`
	err := db.QueryRow(signatureQuery, signatureQueryArgs...).Scan(&sellerName, &mapName, &mapCoords, &mostRecentTimestampStr)

	var items []Item
	if err == nil {
		query := fmt.Sprintf(`
			WITH RankedItems AS (
				SELECT i.*, rms.name_pt, ROW_NUMBER() OVER(PARTITION BY i.name_of_the_item ORDER BY i.id DESC) as rn
				FROM items i
				LEFT JOIN rms_item_cache rms ON i.item_id = rms.item_id
				WHERE i.store_name = ? AND i.seller_name = ? AND i.map_name = ? AND i.map_coordinates = ?
			)
			SELECT id, name_of_the_item, name_pt, item_id, quantity, price, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates, is_available
			FROM RankedItems WHERE rn = 1 %s`, orderByClause)

		rows, queryErr := db.Query(query, storeName, sellerName, mapName, mapCoords)
		if queryErr != nil {
			http.Error(w, "Could not query for store items", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item Item
			var retrievedTime string
			if err := rows.Scan(&item.ID, &item.Name, &item.NamePT, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &retrievedTime, &item.MapName, &item.MapCoordinates, &item.IsAvailable); err == nil {
				if t, err := time.Parse(time.RFC3339, retrievedTime); err == nil {
					item.Timestamp = t.Format("2006-01-02 15:04")
				}
				items = append(items, item)
			}
		}
	} else if err != sql.ErrNoRows {
		http.Error(w, "Database error finding store", http.StatusInternalServerError)
		return
	}

	data := StoreDetailPageData{
		StoreName: storeName, SellerName: sellerName, MapName: strings.ToLower(mapName), MapCoordinates: mapCoords,
		Items: items, LastScrapeTime: getLastUpdateTime("scrape_history", "timestamp"), SortBy: sortBy, Order: order,
	}
	renderTemplate(w, "store_detail.html", data)
}

// generateSecretToken creates a cryptographically secure random token.
func generateSecretToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// tradingPostListHandler displays the list of all trading posts.
func tradingPostListHandler(w http.ResponseWriter, r *http.Request) {
	searchQuery := r.URL.Query().Get("query")

	var postParams []interface{}
	postQuery := `SELECT id, title, post_type, character_name, contact_info, created_at, notes FROM trading_posts`

	if searchQuery != "" {
		if _, err := strconv.Atoi(searchQuery); err == nil {
			postQuery += ` WHERE id IN (SELECT DISTINCT post_id FROM trading_post_items WHERE item_id = ?)`
			postParams = append(postParams, searchQuery)
		} else {
			idList, _ := getCombinedItemIDs(searchQuery)
			postIDsByName, _ := db.Query("SELECT DISTINCT post_id FROM trading_post_items WHERE item_name LIKE ?", "%"+searchQuery+"%")
			var directMatchPostIDs []int
			if postIDsByName != nil {
				defer postIDsByName.Close()
				for postIDsByName.Next() {
					var postID int
					if err := postIDsByName.Scan(&postID); err == nil {
						directMatchPostIDs = append(directMatchPostIDs, postID)
					}
				}
			}

			if len(idList) > 0 || len(directMatchPostIDs) > 0 {
				var whereClauses []string
				if len(idList) > 0 {
					placeholders := strings.Repeat("?,", len(idList)-1) + "?"
					whereClauses = append(whereClauses, fmt.Sprintf("id IN (SELECT DISTINCT post_id FROM trading_post_items WHERE item_id IN (%s))", placeholders))
					for _, id := range idList {
						postParams = append(postParams, id)
					}
				}
				if len(directMatchPostIDs) > 0 {
					placeholders := strings.Repeat("?,", len(directMatchPostIDs)-1) + "?"
					whereClauses = append(whereClauses, fmt.Sprintf("id IN (%s)", placeholders))
					for _, id := range directMatchPostIDs {
						postParams = append(postParams, id)
					}
				}
				postQuery += " WHERE " + strings.Join(whereClauses, " OR ")
			} else {
				postQuery += " WHERE 1 = 0"
			}
		}
	}
	postQuery += ` ORDER BY created_at DESC`

	postRows, err := db.Query(postQuery, postParams...)
	if err != nil {
		log.Printf("❌ Trading Post query error: %v", err)
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		return
	}
	defer postRows.Close()

	var posts []TradingPost
	postMap := make(map[int]int)
	var postIDs []interface{}

	for postRows.Next() {
		var post TradingPost
		if err := postRows.Scan(&post.ID, &post.Title, &post.PostType, &post.CharacterName, &post.ContactInfo, &post.CreatedAt, &post.Notes); err == nil {
			post.Items = []TradingPostItem{}
			posts = append(posts, post)
			postMap[post.ID] = len(posts) - 1
			postIDs = append(postIDs, post.ID)
		}
	}

	if len(postIDs) > 0 {
		placeholders := strings.Repeat("?,", len(postIDs)-1) + "?"
		itemQuery := fmt.Sprintf(`
            SELECT tpi.post_id, tpi.item_name, rms.name_pt, tpi.item_id, tpi.quantity, tpi.price, tpi.currency 
            FROM trading_post_items tpi
            LEFT JOIN rms_item_cache rms ON tpi.item_id = rms.item_id
            WHERE tpi.post_id IN (%s)`, placeholders)
		itemRows, err := db.Query(itemQuery, postIDs...)
		if err == nil {
			defer itemRows.Close()
			for itemRows.Next() {
				var item TradingPostItem
				var postID int
				if err := itemRows.Scan(&postID, &item.ItemName, &item.NamePT, &item.ItemID, &item.Quantity, &item.Price, &item.Currency); err == nil {
					if index, ok := postMap[postID]; ok {
						posts[index].Items = append(posts[index].Items, item)
					}
				}
			}
		}
	}
	data := TradingPostPageData{Posts: posts, LastScrapeTime: getLastUpdateTime("scrape_history", "timestamp"), SearchQuery: searchQuery}
	renderTemplate(w, "trading_post.html", data)
}

// tradingPostFormHandler handles both displaying the form and processing the submission.
func tradingPostFormHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		data := TradingPostFormPageData{
			Title: "Create a New Trading Post", ActionURL: "/trading-post/new",
			Post: TradingPost{PostType: "selling"},
		}
		renderTemplate(w, "trading_post_form.html", data)
		return
	}

	if r.Method == http.MethodPost {
		r.ParseForm()
		postType := r.FormValue("post_type")
		if postType != "selling" && postType != "buying" {
			http.Error(w, "Invalid post type specified.", http.StatusBadRequest)
			return
		}
		title := sanitizeString(r.FormValue("title"), notesSanitizer)
		characterName := sanitizeString(r.FormValue("character_name"), nameSanitizer)
		contactInfo := sanitizeString(r.FormValue("contact_info"), contactSanitizer)
		notes := sanitizeString(r.FormValue("notes"), notesSanitizer)

		if strings.TrimSpace(characterName) == "" || strings.TrimSpace(title) == "" {
			http.Error(w, "Character name and title are required.", http.StatusBadRequest)
			return
		}

		token, err := generateSecretToken(16)
		if err != nil {
			http.Error(w, "Could not generate security token.", http.StatusInternalServerError)
			return
		}
		tokenHash, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
		if err != nil {
			http.Error(w, "Could not secure post.", http.StatusInternalServerError)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, "Failed to start database transaction.", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback() // Rollback on any error

		res, err := tx.Exec(`INSERT INTO trading_posts (title, post_type, character_name, contact_info, notes, created_at, edit_token_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)`, title, postType, characterName, contactInfo, notes, time.Now().Format(time.RFC3339), string(tokenHash))
		if err != nil {
			http.Error(w, "Failed to save post.", http.StatusInternalServerError)
			return
		}
		postID, _ := res.LastInsertId()

		itemNames, quantities, prices := r.Form["item_name[]"], r.Form["quantity[]"], r.Form["price[]"]
		itemIDs, currencies := r.Form["item_id[]"], r.Form["currency[]"]
		if len(itemNames) == 0 {
			http.Error(w, "At least one item is required.", http.StatusBadRequest)
			return
		}

		stmt, err := tx.Prepare("INSERT INTO trading_post_items (post_id, item_name, item_id, quantity, price, currency) VALUES (?, ?, ?, ?, ?, ?)")
		if err != nil {
			http.Error(w, "Database preparation failed.", http.StatusInternalServerError)
			return
		}
		defer stmt.Close()

		for i, rawItemName := range itemNames {
			itemName := sanitizeString(rawItemName, itemSanitizer)
			if strings.TrimSpace(itemName) == "" {
				continue
			}
			quantity, _ := strconv.Atoi(quantities[i])
			price, _ := strconv.ParseInt(prices[i], 10, 64)
			var itemID sql.NullInt64
			if id, err := strconv.ParseInt(itemIDs[i], 10, 64); err == nil && id > 0 {
				itemID = sql.NullInt64{Int64: id, Valid: true}
			}
			currency := "zeny"
			if currencies[i] == "rmt" {
				currency = "rmt"
			}

			if quantity <= 0 || price <= 0 {
				http.Error(w, "All items must have a valid quantity and price.", http.StatusBadRequest)
				return
			}
			if _, err := stmt.Exec(postID, itemName, itemID, quantity, price, currency); err != nil {
				http.Error(w, "Failed to save one of the items.", http.StatusInternalServerError)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, "Failed to finalize transaction.", http.StatusInternalServerError)
			return
		}

		successData := map[string]string{"ItemName": "your items", "EditToken": token}
		renderTemplate(w, "trading_post_success.html", successData)
	}
}

// tradingPostManageHandler handles editing and deleting posts.
func tradingPostManageHandler(w http.ResponseWriter, r *http.Request) {
	postIDStr := r.URL.Query().Get("id")
	action := r.URL.Query().Get("action")

	if r.Method == http.MethodGet {
		data := map[string]string{"PostID": postIDStr, "Action": action}
		renderTemplate(w, "trading_post_manage.html", data)
		return
	}

	r.ParseForm()
	token := r.FormValue("edit_token")
	var tokenHash string
	err := db.QueryRow("SELECT edit_token_hash FROM trading_posts WHERE id = ?", postIDStr).Scan(&tokenHash)
	if err != nil {
		http.Error(w, "Post not found or invalid ID.", http.StatusNotFound)
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(tokenHash), []byte(token)) != nil {
		w.WriteHeader(http.StatusForbidden)
		data := map[string]string{"PostID": postIDStr, "Action": action, "Error": "Invalid Edit/Delete Key provided."}
		renderTemplate(w, "trading_post_manage.html", data)
		return
	}

	switch action {
	case "delete":
		db.Exec("DELETE FROM trading_posts WHERE id = ?", postIDStr)
		fmt.Fprintln(w, "Post successfully deleted. You can now close this page.")
	case "edit":
		var post TradingPost
		err := db.QueryRow(`SELECT id, title, post_type, character_name, contact_info, notes FROM trading_posts WHERE id = ?`, postIDStr).
			Scan(&post.ID, &post.Title, &post.PostType, &post.CharacterName, &post.ContactInfo, &post.Notes)
		if err != nil {
			http.Error(w, "Could not retrieve post to edit.", http.StatusInternalServerError)
			return
		}
		itemRows, _ := db.Query("SELECT item_name, item_id, quantity, price, currency FROM trading_post_items WHERE post_id = ?", postIDStr)
		defer itemRows.Close()
		for itemRows.Next() {
			var item TradingPostItem
			if err := itemRows.Scan(&item.ItemName, &item.ItemID, &item.Quantity, &item.Price, &item.Currency); err == nil {
				post.Items = append(post.Items, item)
			}
		}
		data := TradingPostFormPageData{
			Title: "Edit Your Trading Post", ActionURL: fmt.Sprintf("/trading-post/manage?action=update&id=%s", postIDStr),
			Post: post, EditToken: token,
		}
		renderTemplate(w, "trading_post_form.html", data)
	case "update":
		tx, _ := db.Begin()
		defer tx.Rollback()
		postType := r.FormValue("post_type")
		if postType != "selling" && postType != "buying" {
			http.Error(w, "Invalid post type.", http.StatusBadRequest)
			return
		}
		title := sanitizeString(r.FormValue("title"), notesSanitizer)
		characterName := sanitizeString(r.FormValue("character_name"), nameSanitizer)
		contactInfo := sanitizeString(r.FormValue("contact_info"), contactSanitizer)
		notes := sanitizeString(r.FormValue("notes"), notesSanitizer)

		if strings.TrimSpace(title) == "" {
			http.Error(w, "Title cannot be empty.", http.StatusBadRequest)
			return
		}
		tx.Exec(`UPDATE trading_posts SET title=?, post_type=?, character_name=?, contact_info=?, notes=? WHERE id=?`,
			title, postType, characterName, contactInfo, notes, postIDStr)
		tx.Exec("DELETE FROM trading_post_items WHERE post_id = ?", postIDStr)

		itemNames := r.Form["item_name[]"]
		stmt, _ := tx.Prepare("INSERT INTO trading_post_items (post_id, item_name, item_id, quantity, price, currency) VALUES (?, ?, ?, ?, ?, ?)")
		defer stmt.Close()

		for i, rawItemName := range itemNames {
			itemName := sanitizeString(rawItemName, itemSanitizer)
			if strings.TrimSpace(itemName) == "" {
				continue
			}
			quantity, _ := strconv.Atoi(r.Form["quantity[]"][i])
			price, _ := strconv.ParseInt(r.Form["price[]"][i], 10, 64)
			var itemID sql.NullInt64
			if id, err := strconv.ParseInt(r.Form["item_id[]"][i], 10, 64); err == nil && id > 0 {
				itemID = sql.NullInt64{Int64: id, Valid: true}
			}
			currency := "zeny"
			if r.Form["currency[]"][i] == "rmt" {
				currency = "rmt"
			}
			if quantity <= 0 || price <= 0 {
				http.Error(w, "All items must have a valid quantity and price.", http.StatusBadRequest)
				return
			}
			stmt.Exec(postIDStr, itemName, itemID, quantity, price, currency)
		}
		tx.Commit()
		http.Redirect(w, r, "/trading-post", http.StatusSeeOther)
	default:
		http.Error(w, "Invalid action.", http.StatusBadRequest)
	}
}

// apiItemDetailsHandler serves item details as JSON for the form preview.
func apiItemDetailsHandler(w http.ResponseWriter, r *http.Request) {
	itemIDStr := r.URL.Query().Get("id")
	itemID, err := strconv.Atoi(itemIDStr)
	if err != nil || itemID <= 0 {
		http.Error(w, "Invalid Item ID format", http.StatusBadRequest)
		return
	}

	itemDetails, err := getItemDetailsFromCache(itemID)
	if err != nil {
		log.Printf("ℹ️ API Cache MISS for item ID %d. Scraping RMS...", itemID)
		itemDetails, err = scrapeRMSItemDetails(itemID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Item with ID %d not found.", itemID), http.StatusNotFound)
			return
		}
		saveItemDetailsToCache(itemDetails)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(itemDetails)
}

// ItemSearchResult is a lightweight struct for API search results.
type ItemSearchResult struct {
	ID       int    `json:"ID"`
	Name     string `json:"Name"`
	ImageURL string `json:"ImageURL"`
}

// apiItemSearchHandler searches for items by name from the cache, with a fallback to a live RMS scrape.
func apiItemSearchHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	if len(query) < 3 {
		http.Error(w, "Search query must be at least 3 characters long", http.StatusBadRequest)
		return
	}

	rows, err := db.Query(`SELECT item_id, name, image_url FROM rms_item_cache WHERE name LIKE ? ORDER BY name LIMIT 20`, "%"+query+"%")
	if err != nil {
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []ItemSearchResult
	for rows.Next() {
		var item ItemSearchResult
		if err := rows.Scan(&item.ID, &item.Name, &item.ImageURL); err == nil {
			results = append(results, item)
		}
	}

	if len(results) == 0 {
		log.Printf("ℹ️ API search cache MISS for '%s'. Performing live scrape...", query)
		scrapedResults, scrapeErr := scrapeRMSItemSearch(query)
		if scrapeErr == nil {
			results = scrapedResults
			if len(results) > 0 {
				go func() {
					for _, item := range results {
						scrapeAndCacheItemIfNotExists(item.ID, item.Name)
						time.Sleep(500 * time.Millisecond)
					}
					log.Printf("✅ Finished background caching for '%s' search results.", query)
				}()
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}
