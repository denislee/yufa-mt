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

	"github.com/agnivade/levenshtein" // <-- ADD THIS IMPORT
)

type ItemSearchResult struct {
	ID       int    `json:"ID"`
	Name     string `json:"Name"`
	ImageURL string `json:"ImageURL"`
}

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
	nameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

	contactSanitizer = regexp.MustCompile(`[^a-zA-Z0-9\s:.,#@-]+`)

	notesSanitizer = regexp.MustCompile(`[^a-zA-Z0-9\s.,?!'-]+`)

	itemSanitizer = regexp.MustCompile(`[^\p{L}0-9\s\[\]\+\-]+`)

	reCardRemover = regexp.MustCompile(`(?i)\s*\b(card|carta)\b\s*`)

	reSlotRemover = regexp.MustCompile(`\s*\[\d+\]\s*`)
)

var classImages = map[string]string{
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

const MvpKillCountOffset = 3

var templateFuncs = template.FuncMap{
	"lower": strings.ToLower,
	"cleanCardName": func(cardName string) string {
		return strings.TrimSpace(reCardRemover.ReplaceAllString(cardName, " "))
	},
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
	"formatRMT": func(rmt int64) string {

		return fmt.Sprintf("R$ %d", rmt)
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
	"getClassImageURL": func(class string) string {
		if url, ok := classImages[class]; ok {
			return url
		}
		// Fallback icon (Aprendiz)
		return "https://static.wikia.nocookie.net/ragnarok-online-encyclopedia/images/8/8b/Icon_jobs_0.png"
	},
}

type PaginationData struct {
	CurrentPage int
	TotalPages  int
	PrevPage    int
	NextPage    int
	HasPrevPage bool
	HasNextPage bool
	Offset      int
}

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
		pd.TotalPages = 1
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

func buildItemSearchClause(searchQuery, tableAlias string) (string, []interface{}, error) {
	if searchQuery == "" {
		return "", nil, nil
	}

	alias := strings.Trim(regexp.MustCompile(`[^a-zA-Z0-9_]`).ReplaceAllString(tableAlias, ""), ".")
	if alias != "" {
		alias += "."
	}

	if _, err := strconv.Atoi(searchQuery); err == nil {
		return fmt.Sprintf("%sitem_id = ?", alias), []interface{}{searchQuery}, nil
	}

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

	return "1 = 0", nil, nil
}

func renderTemplate(w http.ResponseWriter, tmplFile string, data interface{}) {

	tmpl, ok := templateCache[tmplFile]
	if !ok {

		log.Printf("[E] [HTTP] Could not find template '%s' in cache!", tmplFile)
		http.Error(w, "Could not load template", http.StatusInternalServerError)
		return
	}

	err := tmpl.Execute(w, data)
	if err != nil {
		log.Printf("[E] [HTTP] Could not execute template '%s': %v", tmplFile, err)
	}
}

func sanitizeString(input string, sanitizer *regexp.Regexp) string {
	return sanitizer.ReplaceAllString(input, "")
}

func getCombinedItemIDs(searchQuery string) ([]int, error) {
	var wg sync.WaitGroup
	scrapedIDsChan := make(chan []int, 1)
	localIDsChan := make(chan []int, 1)

	wg.Add(2)

	go func() {
		defer wg.Done()

		results, err := scrapeRODatabaseSearch(searchQuery, 0)
		if err != nil {
			log.Printf("[W] [HTTP] Concurrent scrape failed for '%s': %v", searchQuery, err)
			scrapedIDsChan <- []int{}
			return
		}

		var ids []int
		if results != nil {
			for _, res := range results {
				ids = append(ids, res.ID)
			}
		}
		scrapedIDsChan <- ids
	}()

	go func() {
		defer wg.Done()
		var ids []int

		// --- MODIFICATION: Query internal_item_db ---
		query := `
			SELECT item_id FROM (
				SELECT DISTINCT item_id FROM items WHERE name_of_the_item LIKE ? AND item_id > 0
				UNION
				SELECT item_id FROM internal_item_db WHERE name LIKE ?
				UNION
				SELECT item_id FROM internal_item_db WHERE name_pt LIKE ?
			)`

		likeQuery := "%" + searchQuery + "%"
		rows, err := db.Query(query, likeQuery, likeQuery, likeQuery)
		// --- END MODIFICATION ---

		if err != nil {
			log.Printf("[W] [HTTP] Concurrent local ID search failed for '%s': %v", searchQuery, err)
			localIDsChan <- []int{}
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
	// --- THIS IS THE FIX ---
	// Read the results from the channels *after* wg.Wait()
	scrapedIDs := <-scrapedIDsChan
	localIDs := <-localIDsChan
	// --- END FIX ---
	close(scrapedIDsChan)
	close(localIDsChan)

	combinedIDs := make(map[int]struct{})
	for _, id := range scrapedIDs { // Now 'scrapedIDs' is defined
		combinedIDs[id] = struct{}{}
	}
	for _, id := range localIDs { // Now 'localIDs' is defined
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

func getItemTypeTabs() []ItemTypeTab {
	var itemTypes []ItemTypeTab
	// --- MODIFICATION: Query internal_item_db (using the 'type' column) ---
	rows, err := db.Query("SELECT DISTINCT type FROM internal_item_db WHERE type IS NOT NULL AND type != '' ORDER BY type ASC")
	// --- END MODIFICATION ---
	if err != nil {
		log.Printf("[W] [HTTP] Could not query for item types: %v", err)
		return itemTypes
	}
	defer rows.Close()

	for rows.Next() {
		var itemType string
		if err := rows.Scan(&itemType); err != nil {
			log.Printf("[W] [HTTP] Failed to scan item type: %v", err)
			continue
		}
		// --- MODIFICATION: The mapItemTypeToTabData function expects "CamelCase"
		// The internal_item_db stores "Usable", "Etc", "Weapon", "Armor".
		// We will map these.
		var mappedType string
		switch strings.ToLower(itemType) {
		case "healing":
			mappedType = "Healing Item"
		case "usable":
			mappedType = "Usable Item"
		case "etc":
			mappedType = "Miscellaneous"
		case "ammo":
			mappedType = "Ammunition"
		case "card":
			mappedType = "Card"
		case "petegg":
			mappedType = "Monster Egg"
		case "petarmor":
			mappedType = "Pet Armor"
		case "petequip":
			mappedType = "Pet Armor"
		case "weapon":
			mappedType = "Weapon"
		case "armor":
			mappedType = "Armor"
		case "shadowgear":
			mappedType = "Armor" // Grouping shadow gear with armor
		case "cash":
			mappedType = "Cash Shop Item"
		default:
			mappedType = itemType // Fallback
		}
		itemTypes = append(itemTypes, mapItemTypeToTabData(mappedType))
		// --- END MODIFICATION ---
	}
	return itemTypes
}

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

func mapItemTypeToTabData(typeName string) ItemTypeTab {
	tab := ItemTypeTab{FullName: typeName, ShortName: typeName, IconItemID: 909}
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
	case "Delayconsume":
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
	case "Cash Shop Item":
		tab.ShortName = ""
		tab.IconItemID = 200441

	}
	return tab
}

// in handlers.go

var templateCache = make(map[string]*template.Template)

func init() {

	templates := []string{
		"index.html",
		"full_list.html",
		"activity.html",
		"history.html",
		"players.html",
		"characters.html",
		"guilds.html",
		"mvp_kills.html",
		"character_detail.html",
		"character_changelog.html",
		"guild_detail.html",
		"store_detail.html",
		"trading_post.html",
		"woe_rankings.html", // --- ADD THIS LINE ---
	}

	navbarPath := "navbar.html"

	log.Println("[I] [HTTP] Parsing all application templates...")
	for _, tmplName := range templates {

		tmpl, err := template.New(tmplName).Funcs(templateFuncs).ParseFiles(tmplName, navbarPath)
		if err != nil {
			// --- REFACTOR ---
			// The old code had a complex if/else if block here to check for
			// "admin_edit_post.html" and "admin.html".
			//
			// Those files are NOT in the 'templates' list above, so that
			// error-handling code was unreachable dead code.
			//
			// We can safely remove it, leaving only the fatal error check.
			// Admin templates are parsed on-demand in their own handlers.
			log.Fatalf("[F] [HTTP] Could not parse template '%s': %v", tmplName, err)
			// --- END REFACTOR ---
		}

		templateCache[tmplName] = tmpl
	}
	log.Println("[I] [HTTP] All templates parsed and cached successfully.")
}

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

	var innerWhereConditions []string
	var innerParams []interface{}
	var outerWhereConditions []string
	var outerParams []interface{}

	// 1. Item search (name/ID) filters the 'items' table directly.
	if searchClause, searchParams, err := buildItemSearchClause(searchQuery, "i"); err != nil {
		http.Error(w, "Failed to build item search query", http.StatusInternalServerError)
		return
	} else if searchClause != "" {
		innerWhereConditions = append(innerWhereConditions, searchClause)
		innerParams = append(innerParams, searchParams...)
	}

	// 2. Type filter checks 'internal_item_db'.
	if selectedType != "" {
		var dbType string
		switch selectedType {
		case "Healing Item":
			dbType = "Healing"
		case "Usable Item":
			dbType = "Usable"
		case "Miscellaneous":
			dbType = "Etc"
		case "Ammunition":
			dbType = "Ammo"
		case "Card":
			dbType = "Card"
		case "Monster Egg":
			dbType = "PetEgg"
		case "Pet Armor":
			dbType = "PetArmor"
		case "Weapon":
			dbType = "Weapon"
		case "Armor":
			dbType = "Armor"
		case "Cash Shop Item":
			dbType = "Cash"
		default:
			dbType = selectedType
		}
		outerWhereConditions = append(outerWhereConditions, "local_db.type = ?")
		outerParams = append(outerParams, dbType)
	}

	// --- FIX: Availability filter now goes into the OUTER WHERE clause ---
	if !showAll {
		// It filters on 't', the alias for the inner subquery.
		outerWhereConditions = append(outerWhereConditions, "t.listing_count > 0")
	}
	// --- END FIX ---

	// Build clause strings
	innerWhereClause := ""
	if len(innerWhereConditions) > 0 {
		innerWhereClause = "WHERE " + strings.Join(innerWhereConditions, " AND ")
	}
	outerWhereClause := ""
	if len(outerWhereConditions) > 0 {
		// This will now correctly prepend "WHERE"
		// e.g., "WHERE local_db.type = ? AND t.listing_count > 0"
		outerWhereClause = "WHERE " + strings.Join(outerWhereConditions, " AND ")
	}

	// --- FIX: The separate 'havingClause' variable has been removed ---

	// --- FIX: Restructured COUNT query to use outerWhereClause ---
	countQuery := `
		SELECT COUNT(*)
		FROM (
			SELECT 1
			FROM (
				SELECT
					MAX(i.item_id) as item_id,
					SUM(CASE WHEN i.is_available = 1 THEN 1 ELSE 0 END) as listing_count
				FROM items i
				%s -- innerWhereClause
				GROUP BY i.name_of_the_item
			) AS t
			LEFT JOIN internal_item_db local_db ON t.item_id = local_db.item_id
			%s -- outerWhereClause
		) AS UniqueItems
	`
	// --- END FIX ---

	var totalUniqueItems int
	// Combine params for count query
	countParams := append(innerParams, outerParams...)
	err := db.QueryRow(fmt.Sprintf(countQuery, innerWhereClause, outerWhereClause), countParams...).Scan(&totalUniqueItems)
	if err != nil {
		log.Printf("[E] [HTTP] Summary count query error: %v", err)
		totalUniqueItems = 0
	}

	// --- FIX: Restructured main SELECT query ---
	selectQuery := `
        SELECT
            t.name_of_the_item,
            local_db.name_pt,
            t.item_id,
            t.lowest_price,
            t.highest_price,
            t.listing_count
        FROM (
            SELECT
                i.name_of_the_item,
                MAX(i.item_id) as item_id, 
                MIN(CASE WHEN i.is_available = 1 THEN CAST(REPLACE(i.price, ',', '') AS INTEGER) ELSE NULL END) as lowest_price,
                MAX(CASE WHEN i.is_available = 1 THEN CAST(REPLACE(i.price, ',', '') AS INTEGER) ELSE NULL END) as highest_price,
                SUM(CASE WHEN i.is_available = 1 THEN 1 ELSE 0 END) as listing_count
            FROM items i
            %s -- innerWhereClause
            GROUP BY i.name_of_the_item
        ) AS t
        LEFT JOIN internal_item_db local_db ON t.item_id = local_db.item_id
        %s -- outerWhereClause
    `
	// --- END FIX ---

	allowedSorts := map[string]string{
		"name":          "t.name_of_the_item",
		"item_id":       "t.item_id",
		"listings":      "t.listing_count",
		"lowest_price":  "t.lowest_price",
		"highest_price": "t.highest_price",
	}
	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "highest_price", "DESC")

	query := fmt.Sprintf("%s %s, t.name_of_the_item ASC;",
		fmt.Sprintf(selectQuery, innerWhereClause, outerWhereClause),
		orderByClause,
	)

	// Combine params for main query
	mainParams := append(innerParams, outerParams...)
	rows, err := db.Query(query, mainParams...)
	if err != nil {
		log.Printf("[E] [HTTP] Summary query error: %v, Query: %s, Params: %v", err, query, mainParams)
		http.Error(w, "Database query for summary failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ItemSummary
	for rows.Next() {
		var item ItemSummary
		if err := rows.Scan(&item.Name, &item.NamePT, &item.ItemID, &item.LowestPrice, &item.HighestPrice, &item.ListingCount); err != nil {
			log.Printf("[W] [HTTP] Failed to scan summary row: %v", err)
			continue
		}
		items = append(items, item)
	}

	var totalVisitors int
	db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&totalVisitors)

	data := SummaryPageData{
		Items:            items,
		SearchQuery:      searchQuery,
		SortBy:           sortBy,
		Order:            order,
		ShowAll:          showAll,
		LastScrapeTime:   GetLastScrapeTime(),
		ItemTypes:        getItemTypeTabs(),
		SelectedType:     selectedType,
		TotalVisitors:    totalVisitors,
		TotalUniqueItems: totalUniqueItems,
		PageTitle:        "Summary",
	}
	renderTemplate(w, "index.html", data)
}

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
		log.Printf("[W] [HTTP] Could not query for store names: %v", err)
	} else {
		defer storeRows.Close()
		for storeRows.Next() {
			var storeName string
			if err := storeRows.Scan(&storeName); err != nil {
				log.Printf("[W] [HTTP] Failed to scan store name: %v", err)
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
		// --- MODIFICATION: Query internal_item_db's 'type' column ---
		// (Duplicating the logic from summaryHandler)
		var dbType string
		switch selectedType {
		case "Healing Item":
			dbType = "Healing"
		case "Usable Item":
			dbType = "Usable"
		case "Miscellaneous":
			dbType = "Etc"
		case "Ammunition":
			dbType = "Ammo"
		case "Card":
			dbType = "Card"
		case "Monster Egg":
			dbType = "PetEgg"
		case "Pet Armor":
			dbType = "PetArmor"
		case "Weapon":
			dbType = "Weapon"
		case "Armor":
			dbType = "Armor"
		case "Cash Shop Item":
			dbType = "Cash"
		default:
			dbType = selectedType
		}
		whereConditions = append(whereConditions, "local_db.type = ?")
		// --- END MODIFICATION ---
		queryParams = append(queryParams, dbType)
	}
	if !showAll {
		whereConditions = append(whereConditions, "i.is_available = 1")
	}

	// --- MODIFICATION: Join internal_item_db ---
	baseQuery := `
		SELECT i.id, i.name_of_the_item, local_db.name_pt, i.item_id, i.quantity, i.price, i.store_name, i.seller_name, i.date_and_time_retrieved, i.map_name, i.map_coordinates, i.is_available
		FROM items i 
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
	`
	// --- END MODIFICATION ---

	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	query := fmt.Sprintf(`%s %s %s;`, baseQuery, whereClause, orderByClause)

	rows, err := db.Query(query, queryParams...)
	if err != nil {
		log.Printf("[E] [HTTP] Database query error: %v", err)
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
			log.Printf("[W] [HTTP] Failed to scan row: %v", err)
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
		SortBy: sortBy, Order: order, ShowAll: showAll, LastScrapeTime: GetLastScrapeTime(),
		VisibleColumns: visibleColumns, AllColumns: allCols, ColumnParams: template.URL(columnParams.Encode()),
		ItemTypes: getItemTypeTabs(), SelectedType: selectedType,
		PageTitle: "Full List",
	}
	renderTemplate(w, "full_list.html", data)
}

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
	// --- MODIFICATION: Join internal_item_db ---
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM market_events me LEFT JOIN internal_item_db local_db ON me.item_id = local_db.item_id %s", whereClause)
	// --- END MODIFICATION ---
	if err := db.QueryRow(countQuery, params...).Scan(&totalEvents); err != nil {
		log.Printf("[E] [HTTP] Could not count market events: %v", err)
		http.Error(w, "Could not count market events", http.StatusInternalServerError)
		return
	}

	pagination := newPaginationData(r, totalEvents, eventsPerPage)
	// --- MODIFICATION: Join internal_item_db ---
	query := fmt.Sprintf(`
        SELECT me.event_timestamp, me.event_type, me.item_name, local_db.name_pt, me.item_id, me.details
        FROM market_events me
        LEFT JOIN internal_item_db local_db ON me.item_id = local_db.item_id %s
        ORDER BY me.event_timestamp DESC LIMIT ? OFFSET ?`, whereClause)
	// --- END MODIFICATION ---

	finalParams := append(params, eventsPerPage, pagination.Offset)
	eventRows, err := db.Query(query, finalParams...)
	if err != nil {
		// --- THIS IS THE FIX ---
		log.Printf("[E] [HTTP] Could not query for market events: %v", err)
		// --- END FIX ---
		http.Error(w, "Could not query for market events", http.StatusInternalServerError)
		return
	}
	defer eventRows.Close()

	var marketEvents []MarketEvent
	for eventRows.Next() {
		var event MarketEvent
		var detailsStr, timestampStr string
		if err := eventRows.Scan(&timestampStr, &event.EventType, &event.ItemName, &event.NamePT, &event.ItemID, &detailsStr); err != nil {
			log.Printf("[W] [HTTP] Failed to scan market event row: %v", err)
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
		LastScrapeTime: GetLastScrapeTime(),
		SearchQuery:    searchQuery,
		SoldOnly:       soldOnly,
		Pagination:     pagination,
		PageTitle:      "Activity",
	}
	renderTemplate(w, "activity.html", data)
}

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
	log.Printf("[D] [HTTP/History] Handling request for item: '%s'", itemName)

	var itemID int
	var itemNamePT sql.NullString
	// --- MODIFICATION: Join internal_item_db ---
	err := db.QueryRow(`
		SELECT i.item_id, local_db.name_pt 
		FROM items i 
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
		WHERE i.name_of_the_item = ? AND i.item_id > 0 
		LIMIT 1`, itemName).Scan(&itemID, &itemNamePT)
	// --- END MODIFICATION ---
	if err != nil {
		log.Printf("[D] [HTTP/History] Step 1: Initial item ID/NamePT query failed for '%s': %v", itemName, err)
	} else {
		log.Printf("[D] [HTTP/History] Step 1: Found ItemID: %d, NamePT: '%s'", itemID, itemNamePT.String)
	}

	// --- MODIFICATION: This now calls the new getItemDetailsFromCache (from rms.go)
	// which queries internal_item_db.
	rmsItemDetails := fetchItemDetails(itemID)
	// --- END MODIFICATION ---
	if rmsItemDetails != nil {
		log.Printf("[D] [HTTP/History] Step 2: Successfully fetched details from internal_item_db for ID %d.", itemID)
	} else {
		log.Printf("[D] [HTTP/History] Step 2: No details found in internal_item_db for ID %d.", itemID)
	}

	currentListings, err := fetchCurrentListings(itemName)
	if err != nil {
		log.Printf("[E] [HTTP/History] Current listings query error: %v", err)
		http.Error(w, "Database query for current listings failed", http.StatusInternalServerError)
		return
	}
	log.Printf("[D] [HTTP/History] Step 3: Found %d current (available) listings.", len(currentListings))

	var currentLowest, currentHighest *ItemListing
	if len(currentListings) > 0 {
		currentLowest = &currentListings[0]
		currentHighest = &currentListings[len(currentListings)-1]
		log.Printf("[D] [HTTP/History]     -> Current Lowest: %d z, Current Highest: %d z", currentLowest.Price, currentHighest.Price)
	}

	// --- FIX IS HERE ---
	// Marshal the lowest/highest listings for the script
	// Use "null" as a default JSON value if no item is found
	var currentLowestJSON, currentHighestJSON []byte
	if currentLowest != nil {
		currentLowestJSON, _ = json.Marshal(currentLowest)
	} else {
		currentLowestJSON = []byte("null")
	}

	if currentHighest != nil {
		currentHighestJSON, _ = json.Marshal(currentHighest)
	} else {
		currentHighestJSON = []byte("null")
	}
	// --- END FIX ---

	finalPriceHistory, err := fetchPriceHistory(itemName)
	if err != nil {
		log.Printf("[E] [HTTP/History] History change query error: %v", err)
		http.Error(w, "Database query for changes failed", http.StatusInternalServerError)
		return
	}
	log.Printf("[D] [HTTP/History] Step 4: Found %d unique price points for history graph.", len(finalPriceHistory))
	priceHistoryJSON, _ := json.Marshal(finalPriceHistory)

	var overallLowest, overallHighest sql.NullInt64
	db.QueryRow(`
        SELECT MIN(CAST(REPLACE(REPLACE(price, ',', ''), 'z', '') AS INTEGER)), 
               MAX(CAST(REPLACE(REPLACE(price, ',', ''), 'z', '') AS INTEGER))
        FROM items WHERE name_of_the_item = ?;
    `, itemName).Scan(&overallLowest, &overallHighest)
	log.Printf("[D] [HTTP/History] Step 5: Found Overall Lowest: %d z, Overall Highest: %d z", overallLowest.Int64, overallHighest.Int64)

	const listingsPerPage = 50
	pagination := newPaginationData(r, 0, listingsPerPage) // Initial
	allListings, totalListings, err := fetchAllListings(itemName, pagination, listingsPerPage)
	if err != nil {
		log.Printf("[E] [HTTP/History] All listings query error: %v", err)
		http.Error(w, "Database query for all listings failed", http.StatusInternalServerError)
		return
	}
	log.Printf("[D] [HTTP/History] Step 6: Found %d total historical listings. Returning %d for this page.", totalListings, len(allListings))
	pagination = newPaginationData(r, totalListings, listingsPerPage) // Recalculate

	data := HistoryPageData{
		ItemName:           itemName,
		ItemNamePT:         itemNamePT,
		PriceDataJSON:      template.JS(priceHistoryJSON),
		CurrentLowestJSON:  template.JS(currentLowestJSON),  // <-- ADDED
		CurrentHighestJSON: template.JS(currentHighestJSON), // <-- ADDED
		OverallLowest:      int(overallLowest.Int64),
		OverallHighest:     int(overallHighest.Int64),
		CurrentLowest:      currentLowest,
		CurrentHighest:     currentHighest,
		ItemDetails:        rmsItemDetails,
		AllListings:        allListings,
		LastScrapeTime:     GetLastScrapeTime(),
		TotalListings:      totalListings,
		Pagination:         pagination,
		PageTitle:          itemName,
	}

	log.Printf("[D] [HTTP/History] Rendering template for '%s' with all data.", itemName)
	renderTemplate(w, "history.html", data)
}

// I am also including the helper functions that were extracted in the refactor,
// as they are called by the main handler.

// fetchItemDetails attempts to get RMSItem details from the internal item DB.
func fetchItemDetails(itemID int) *RMSItem {
	if itemID <= 0 {
		return nil
	}

	// --- MODIFICATION: Only query the internal_item_db. ---
	// Fallback scraping and saving logic has been removed.
	cachedItem, err := getItemDetailsFromCache(itemID)
	if err == nil {
		log.Printf("[D] [ItemDB] Found item %d in internal_item_db.", itemID)
		return cachedItem // Found in local DB
	}

	log.Printf("[D] [ItemDB] Item %d not found in internal_item_db: %v", itemID, err)
	return nil // Not found
	// --- END MODIFICATION ---
}

// fetchCurrentListings gets all currently available listings for an item.
func fetchCurrentListings(itemName string) ([]ItemListing, error) {
	query := `
		SELECT CAST(REPLACE(REPLACE(price, ',', ''), 'z', '') AS INTEGER) as price_int, 
		       quantity, store_name, seller_name, map_name, map_coordinates, date_and_time_retrieved
		FROM items WHERE name_of_the_item = ? AND is_available = 1 
		ORDER BY price_int ASC;
	`
	rows, err := db.Query(query, itemName)
	if err != nil {
		return nil, fmt.Errorf("current listings query error: %w", err)
	}
	defer rows.Close()

	var listings []ItemListing
	for rows.Next() {
		var listing ItemListing
		var timestampStr string
		if err := rows.Scan(&listing.Price, &listing.Quantity, &listing.StoreName, &listing.SellerName, &listing.MapName, &listing.MapCoordinates, &timestampStr); err != nil {
			log.Printf("[W] [HTTP/History] Failed to scan current listing row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			listing.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			listing.Timestamp = timestampStr
		}
		listings = append(listings, listing)
	}
	return listings, nil
}

// fetchPriceHistory aggregates the lowest/highest price points over time for the graph.
func fetchPriceHistory(itemName string) ([]PricePointDetails, error) {
	priceChangeQuery := `
		SELECT
			t_lowest.date_and_time_retrieved,
			t_lowest.price_int,
			t_lowest.quantity,
			t_lowest.store_name,
			t_lowest.seller_name,
			t_lowest.map_name,
			t_lowest.map_coordinates,
			t_highest.price_int,
			t_highest.quantity,
			t_highest.store_name,
			t_highest.seller_name,
			t_highest.map_name,
			t_highest.map_coordinates
		FROM
			(
				-- Subquery to find the row with the lowest price for each timestamp
				SELECT 
					i1.date_and_time_retrieved,
					CAST(REPLACE(REPLACE(i1.price, ',', ''), 'z', '') AS INTEGER) as price_int,
					i1.quantity,
					i1.store_name,
					i1.seller_name,
					i1.map_name,
					i1.map_coordinates
				FROM items i1
				WHERE i1.name_of_the_item = ?
				AND i1.id = (
					SELECT i_min.id
					FROM items i_min
					WHERE i_min.name_of_the_item = i1.name_of_the_item
					  AND i_min.date_and_time_retrieved = i1.date_and_time_retrieved
					ORDER BY CAST(REPLACE(REPLACE(i_min.price, ',', ''), 'z', '') AS INTEGER) ASC, i_min.id DESC
					LIMIT 1
				)
			) AS t_lowest
		JOIN
			(
				-- Subquery to find the row with the highest price for each timestamp
				SELECT 
					i2.date_and_time_retrieved,
					CAST(REPLACE(REPLACE(i2.price, ',', ''), 'z', '') AS INTEGER) as price_int,
					i2.quantity,
					i2.store_name,
					i2.seller_name,
					i2.map_name,
					i2.map_coordinates
				FROM items i2
				WHERE i2.name_of_the_item = ?
				AND i2.id = (
					SELECT i_max.id
					FROM items i_max
					WHERE i_max.name_of_the_item = i2.name_of_the_item
					  AND i_max.date_and_time_retrieved = i2.date_and_time_retrieved
					ORDER BY CAST(REPLACE(REPLACE(i_max.price, ',', ''), 'z', '') AS INTEGER) DESC, i_max.id DESC
					LIMIT 1
				)
			) AS t_highest 
		ON t_lowest.date_and_time_retrieved = t_highest.date_and_time_retrieved
		ORDER BY t_lowest.date_and_time_retrieved ASC;
    `
	rows, err := db.Query(priceChangeQuery, itemName, itemName)
	if err != nil {
		return nil, fmt.Errorf("history change query error: %w", err)
	}
	defer rows.Close()

	var finalPriceHistory []PricePointDetails
	for rows.Next() {
		var p PricePointDetails
		var timestampStr string
		err := rows.Scan(&timestampStr, &p.LowestPrice, &p.LowestQuantity, &p.LowestStoreName, &p.LowestSellerName, &p.LowestMapName, &p.LowestMapCoords,
			&p.HighestPrice, &p.HighestQuantity, &p.HighestStoreName, &p.HighestSellerName, &p.HighestMapName, &p.HighestMapCoords)
		if err != nil {
			log.Printf("[W] [HTTP/History] Failed to scan history row: %v", err)
			continue
		}

		t, _ := time.Parse(time.RFC3339, timestampStr)
		p.Timestamp = t.Format("2006-01-02 15:04")

		// This logic de-duplicates consecutive identical price points
		if len(finalPriceHistory) == 0 ||
			finalPriceHistory[len(finalPriceHistory)-1].LowestPrice != p.LowestPrice ||
			finalPriceHistory[len(finalPriceHistory)-1].HighestPrice != p.HighestPrice {
			finalPriceHistory = append(finalPriceHistory, p)
		}
	}
	return finalPriceHistory, nil
}

// fetchAllListings retrieves a paginated list of all historical listings for an item.
// --- FIX IS HERE: Added listingsPerPage parameter ---
func fetchAllListings(itemName string, pagination PaginationData, listingsPerPage int) ([]Item, int, error) {
	var totalListings int
	err := db.QueryRow("SELECT COUNT(*) FROM items WHERE name_of_the_item = ?", itemName).Scan(&totalListings)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count all listings: %w", err)
	}

	// --- MODIFICATION: Join internal_item_db ---
	query := `
		SELECT i.id, i.name_of_the_item, local_db.name_pt, i.item_id, i.quantity, i.price, 
		       i.store_name, i.seller_name, i.date_and_time_retrieved, i.map_name, 
			   i.map_coordinates, i.is_available
		FROM items i 
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id 
		WHERE i.name_of_the_item = ? 
		ORDER BY i.is_available DESC, i.date_and_time_retrieved DESC 
		LIMIT ? OFFSET ?;
	`
	// --- END MODIFICATION ---
	// --- FIX IS HERE: Use the listingsPerPage parameter ---
	rows, err := db.Query(query, itemName, listingsPerPage, pagination.Offset)
	if err != nil {
		return nil, totalListings, fmt.Errorf("all listings query error: %w", err)
	}
	defer rows.Close()

	var allListings []Item
	for rows.Next() {
		var listing Item
		var timestampStr string
		if err := rows.Scan(&listing.ID, &listing.Name, &listing.NamePT, &listing.ItemID, &listing.Quantity, &listing.Price, &listing.StoreName, &listing.SellerName, &timestampStr, &listing.MapName, &listing.MapCoordinates, &listing.IsAvailable); err != nil {
			log.Printf("[W] [HTTP/History] Failed to scan all listing row: %v", err)
			continue
		}
		if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
			listing.Timestamp = parsedTime.Format("2006-01-02 15:04")
		} else {
			listing.Timestamp = timestampStr
		}
		allListings = append(allListings, listing)
	}
	return allListings, totalListings, nil
}

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
		log.Printf("[D] [HTTP/Player] Player graph: Downsampling data for '%s' interval. Bucket size: %d seconds.", interval, bucketSizeInSeconds)
		query = fmt.Sprintf(`
			SELECT MIN(timestamp), CAST(AVG(count) AS INTEGER), CAST(AVG(seller_count) AS INTEGER)
			FROM player_history %s GROUP BY CAST(unixepoch(timestamp) / %d AS INTEGER) ORDER BY 1 ASC`, whereClause, bucketSizeInSeconds)
	} else {
		log.Printf("[D] [HTTP/Player] Player graph: Fetching all data points for '%s' interval.", interval)
		query = fmt.Sprintf("SELECT timestamp, count, seller_count FROM player_history %s ORDER BY timestamp ASC", whereClause)
	}

	rows, err := db.Query(query, params...)
	if err != nil {
		log.Printf("[E] [HTTP/Player] Could not query for player history: %v", err)
		http.Error(w, "Could not query for player history", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var playerHistory []PlayerCountPoint
	activeDatesWithData := make(map[string]struct{})

	// --- NEW: Variables for interval stats ---
	var maxActiveInterval int = -1
	var minActiveInterval int = -1
	var totalActiveInterval int64 = 0
	var maxActiveIntervalTime string = "N/A"
	var dataPointCount int64 = 0
	// --- END NEW ---

	for rows.Next() {
		var point PlayerCountPoint
		var timestampStr string
		var sellerCount sql.NullInt64
		if err := rows.Scan(&timestampStr, &point.Count, &sellerCount); err != nil {
			log.Printf("[W] [HTTP/Player] Failed to scan player history row: %v", err)
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

		// --- NEW: Calculate interval stats ---
		activePlayers := point.Delta
		if maxActiveInterval == -1 || activePlayers > maxActiveInterval {
			maxActiveInterval = activePlayers
			maxActiveIntervalTime = point.Timestamp
		}
		if minActiveInterval == -1 || activePlayers < minActiveInterval {
			minActiveInterval = activePlayers
		}
		totalActiveInterval += int64(activePlayers)
		dataPointCount++
		// --- END NEW ---
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

	// --- NEW: Finalize interval stats ---
	var avgActiveInterval int
	if dataPointCount > 0 {
		avgActiveInterval = int(totalActiveInterval / dataPointCount)
	}
	if maxActiveInterval == -1 {
		maxActiveInterval = 0
	}
	if minActiveInterval == -1 {
		minActiveInterval = 0
	}
	// --- END NEW ---

	data := PlayerCountPageData{
		PlayerDataJSON:                 template.JS(playerHistoryJSON),
		LastScrapeTime:                 GetLastScrapeTime(),
		SelectedInterval:               interval,
		EventDataJSON:                  template.JS(eventIntervalsJSON),
		LatestActivePlayers:            latestActivePlayers,
		HistoricalMaxActivePlayers:     historicalMaxActive,
		HistoricalMaxActivePlayersTime: historicalMaxTime,
		PageTitle:                      "Player Count",

		// --- NEW: Pass data to template ---
		IntervalPeakActive:     maxActiveInterval,
		IntervalPeakActiveTime: maxActiveIntervalTime,
		IntervalAvgActive:      avgActiveInterval,
		IntervalLowActive:      minActiveInterval,
		// --- END NEW ---
	}
	renderTemplate(w, "players.html", data)
}

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
			log.Printf("[W] [HTTP/Char] Failed to scan player character row: %v", err)
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
		Players: players, LastScrapeTime: GetLastScrapeTime(),
		SelectedClass: selectedClass, SelectedGuild: selectedGuild, AllClasses: allClasses, SortBy: sortBy, Order: order,
		VisibleColumns: visibleColumns, AllColumns: allCols, ColumnParams: template.URL(columnParams.Encode()),
		Pagination: pagination, TotalPlayers: totalPlayers, TotalZeny: totalZeny.Int64,
		ClassDistributionJSON: template.JS(classDistJSON), GraphFilter: graphFilterMap, GraphFilterParams: template.URL(graphFilterParams.Encode()),
		HasChartData: len(chartData) > 1,
		PageTitle:    "Characters",
	}
	renderTemplate(w, "characters.html", data)
}

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
		log.Printf("[E] [HTTP/Guild] Could not query for guilds: %v", err)
		http.Error(w, "Could not query for guilds", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var guilds []Guild
	for rows.Next() {
		var g Guild
		if err := rows.Scan(&g.Name, &g.Level, &g.Experience, &g.Master, &g.EmblemURL, &g.MemberCount, &g.TotalZeny, &g.AvgBaseLevel); err != nil {
			log.Printf("[W] [HTTP/Guild] Failed to scan guild row: %v", err)
			continue
		}
		guilds = append(guilds, g)
	}

	data := GuildPageData{
		Guilds:              guilds,
		LastGuildUpdateTime: GetLastScrapeTime(),
		SearchName:          searchName,
		SortBy:              sortBy,
		Order:               order,
		Pagination:          pagination,
		TotalGuilds:         totalGuilds,
		PageTitle:           "Guilds",
	}
	renderTemplate(w, "guilds.html", data)
}

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
		log.Printf("[E] [HTTP/MVP] Could not query for MVP kills: %v", err)
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
			log.Printf("[W] [HTTP/MVP] Failed to scan MVP kill row: %v", err)
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
		LastScrapeTime: GetLastScrapeTime(),
		PageTitle:      "MVP Kills",
	}
	renderTemplate(w, "mvp_kills.html", data)
}

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
			log.Printf("[E] [HTTP/Char] Could not query for character '%s': %v", charName, err)
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
		LastScrapeTime:      GetLastScrapeTime(),
		ClassImageURL:       classImages[p.Class],
		ChangelogEntries:    changelogEntries,
		ChangelogPagination: pagination,
		PageTitle:           p.Name,
	}
	renderTemplate(w, "character_detail.html", data)
}

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
			log.Printf("[W] [HTTP/Changelog] Failed to scan character changelog row: %v", err)
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
		LastScrapeTime:   GetLastScrapeTime(),
		Pagination:       pagination,
		PageTitle:        "Character Changelog",
	}
	renderTemplate(w, "character_changelog.html", data)
}

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
			log.Printf("[W] [HTTP/Guild] Failed to scan guild member row: %v", err)
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
		LastScrapeTime:        GetLastScrapeTime(),
		SortBy:                sortBy,
		Order:                 order,
		ClassDistributionJSON: template.JS(classDistJSON),
		HasChartData:          len(classDistribution) > 1,
		ChangelogEntries:      changelogEntries,
		ChangelogPagination:   pagination,
		PageTitle:             g.Name,
	}
	renderTemplate(w, "guild_detail.html", data)
}

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
		// --- MODIFICATION: Join internal_item_db ---
		query := fmt.Sprintf(`
			WITH RankedItems AS (
				SELECT i.*, local_db.name_pt, ROW_NUMBER() OVER(PARTITION BY i.name_of_the_item ORDER BY i.id DESC) as rn
				FROM items i
				LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
				WHERE i.store_name = ? AND i.seller_name = ? AND i.map_name = ? AND i.map_coordinates = ?
			)
			SELECT id, name_of_the_item, name_pt, item_id, quantity, price, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates, is_available
			FROM RankedItems WHERE rn = 1 %s`, orderByClause)
		// --- END MODIFICATION ---

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
		StoreName:      storeName,
		SellerName:     sellerName,
		MapName:        strings.ToLower(mapName),
		MapCoordinates: mapCoords,
		Items:          items,
		LastScrapeTime: GetLastScrapeTime(),
		SortBy:         sortBy,
		Order:          order,
		PageTitle:      storeName,
	}
	renderTemplate(w, "store_detail.html", data)
}

func generateSecretToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func tradingPostListHandler(w http.ResponseWriter, r *http.Request) {
	searchQuery := r.URL.Query().Get("query")
	filterType := r.URL.Query().Get("filter_type")
	if filterType == "" {
		filterType = "all"
	}
	filterCurrency := r.URL.Query().Get("filter_currency")
	if filterCurrency == "" {
		filterCurrency = "all"
	}

	var queryParams []interface{}
	// --- MODIFICATION: Join internal_item_db ---
	baseQuery := `
		SELECT
			p.id, p.title, p.post_type, p.character_name, p.contact_info, p.created_at, p.notes,
			i.item_name, local_db.name_pt, i.item_id, i.quantity, i.price_zeny, i.price_rmt, 
			i.payment_methods, i.refinement, i.card1, i.card2, i.card3, i.card4
		FROM trading_post_items i
		JOIN trading_posts p ON i.post_id = p.id
		LEFT JOIN internal_item_db local_db ON i.item_id = local_db.item_id
	`
	// --- END MODIFICATION ---
	var whereConditions []string

	if searchQuery != "" {

		if _, err := strconv.Atoi(searchQuery); err == nil {
			whereConditions = append(whereConditions, "i.item_id = ?")
			queryParams = append(queryParams, searchQuery)
		} else {

			idList, _ := getCombinedItemIDs(searchQuery)

			var nameClauses []string
			nameClauses = append(nameClauses, "i.item_name LIKE ?")
			queryParams = append(queryParams, "%"+searchQuery+"%")

			if len(idList) > 0 {
				placeholders := strings.Repeat("?,", len(idList)-1) + "?"
				nameClauses = append(nameClauses, fmt.Sprintf("i.item_id IN (%s)", placeholders))
				for _, id := range idList {
					queryParams = append(queryParams, id)
				}
			}
			whereConditions = append(whereConditions, "("+strings.Join(nameClauses, " OR ")+")")
		}
	}

	if filterType == "selling" {
		whereConditions = append(whereConditions, "p.post_type = ?")
		queryParams = append(queryParams, "selling")
	} else if filterType == "buying" {
		whereConditions = append(whereConditions, "p.post_type = ?")
		queryParams = append(queryParams, "buying")
	}

	if filterCurrency == "zeny" {

		whereConditions = append(whereConditions, "(i.price_zeny > 0 OR i.payment_methods IN ('zeny', 'both'))")
	} else if filterCurrency == "rmt" {

		whereConditions = append(whereConditions, "(i.price_rmt > 0 OR i.payment_methods IN ('rmt', 'both'))")
	}

	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	allowedSorts := map[string]string{
		"item_name": "i.item_name",
		"quantity":  "i.quantity",
		"seller":    "p.character_name",
		"posted":    "p.created_at",
	}

	if filterCurrency == "rmt" {
		allowedSorts["price"] = "CASE WHEN i.price_rmt = 0 THEN 9223372036854775807 ELSE i.price_rmt END"
	} else {
		allowedSorts["price"] = "CASE WHEN i.price_zeny = 0 THEN 9223372036854775807 ELSE i.price_zeny END"
	}

	orderByClause, sortBy, order := getSortClause(r, allowedSorts, "posted", "DESC")

	finalQuery := baseQuery + whereClause + " " + orderByClause

	rows, err := db.Query(finalQuery, queryParams...)
	if err != nil {
		log.Printf("[E] [HTTP/Trade] Trading Post flat list query error: %v", err)
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []FlatTradingPostItem
	for rows.Next() {
		var item FlatTradingPostItem
		err := rows.Scan(
			&item.PostID, &item.Title, &item.PostType, &item.CharacterName, &item.ContactInfo, &item.CreatedAt, &item.Notes,
			&item.ItemName, &item.NamePT, &item.ItemID, &item.Quantity, &item.PriceZeny, &item.PriceRMT,
			&item.PaymentMethods,
			&item.Refinement, &item.Card1, &item.Card2, &item.Card3, &item.Card4,
		)
		if err != nil {
			log.Printf("[W] [HTTP/Trade] Failed to scan flat trading post item: %v", err)
			continue
		}
		items = append(items, item)
	}

	data := TradingPostPageData{
		Items:          items,
		LastScrapeTime: GetLastScrapeTime(),
		SearchQuery:    searchQuery,
		FilterType:     filterType,
		FilterCurrency: filterCurrency,
		SortBy:         sortBy,
		Order:          order,
		PageTitle:      "Discord",
	}
	renderTemplate(w, "trading_post.html", data)
}

// findItemIDInCache attempts to find an item ID using the local item DB.
// This version uses LIKE and Levenshtein distance for proximity matching.
// --- MODIFICATION: Added 'slots' parameter ---
func findItemIDInCache(cleanItemName string, slots int) (sql.NullInt64, bool) {

	// --- MODIFICATION: Build case-insensitive query ---
	// Lowercase the search term *once*
	lowerCleanItemName := strings.ToLower(cleanItemName)
	likeQuery := "%" + strings.ReplaceAll(lowerCleanItemName, " ", "%") + "%"

	var queryParams []interface{}
	queryParams = append(queryParams, likeQuery, likeQuery)

	var slotClause string
	if slots == 0 {
		// If slots are 0, match 0 or NULL (since NULL slots also mean 0)
		slotClause = "AND (slots = 0 OR slots IS NULL)"
	} else {
		// If slots > 0, match that exact number
		slotClause = "AND slots = ?"
		queryParams = append(queryParams, slots)
	}

	// Use LOWER() on columns for case-insensitive matching
	query := fmt.Sprintf(`
		SELECT item_id, name, name_pt 
		FROM internal_item_db 
		WHERE (LOWER(name) LIKE ? OR LOWER(name_pt) LIKE ?)
		%s
		LIMIT 10`, slotClause) // Limit to 10 potential matches

	rows, err := db.Query(query, queryParams...)
	// --- END MODIFICATION ---
	if err != nil {
		log.Printf("[W] [ItemID] Error during LIKE query for '%s' (slots: %d): %v", likeQuery, slots, err)
		return sql.NullInt64{Valid: false}, false
	}
	defer rows.Close()

	type potentialMatch struct {
		id     int64
		name   string // This is the original name from the DB (e.g., "Jur")
		namePT string // This is the original PT name (e.g., "Jur")
	}
	var potentialMatches []potentialMatch

	for rows.Next() {
		var match potentialMatch
		var namePT sql.NullString
		if err := rows.Scan(&match.id, &match.name, &namePT); err == nil {
			if namePT.Valid {
				match.namePT = namePT.String
			}
			potentialMatches = append(potentialMatches, match)
		}
	}

	if len(potentialMatches) == 1 {
		itemID := potentialMatches[0].id
		log.Printf("[D] [ItemID] Found unique local LIKE match for '%s': ID %d", cleanItemName, itemID)
		return sql.NullInt64{Int64: itemID, Valid: true}, true
	}

	// Disambiguation logic (Levenshtein) remains the same as before
	// We already have lowerCleanItemName from above.

	// 1. Check for a perfect match first
	for _, match := range potentialMatches {
		if strings.ToLower(match.name) == lowerCleanItemName || strings.ToLower(match.namePT) == lowerCleanItemName {
			log.Printf("[D] [ItemID] Found perfect match '%s' (ID %d) within %d LIKE results.", cleanItemName, match.id, len(potentialMatches))
			return sql.NullInt64{Int64: match.id, Valid: true}, true
		}
	}

	// 2. No perfect match, find the closest Levenshtein distance.
	const maxLevenshteinDistance = 2

	bestMatchID := int64(-1)
	bestMatchName := ""
	minDistance := 100

	log.Printf("[D] [ItemID/Levenshtein] No perfect match for '%s'. Calculating proximity for %d candidates.", cleanItemName, len(potentialMatches))

	for _, match := range potentialMatches {
		lowerNameEN := strings.ToLower(match.name)
		distEN := levenshtein.ComputeDistance(lowerCleanItemName, lowerNameEN)

		currentBestName := match.name
		currentMinDist := distEN

		logMsg := fmt.Sprintf("[D] [ItemID/Levenshtein] ...vs EN '%s' (ID %d): dist %d.", lowerNameEN, match.id, distEN)

		if match.namePT != "" {
			lowerNamePT := strings.ToLower(match.namePT)
			distPT := levenshtein.ComputeDistance(lowerCleanItemName, lowerNamePT)
			logMsg += fmt.Sprintf(" vs PT '%s': dist %d.", lowerNamePT, distPT)

			if distPT < currentMinDist {
				currentMinDist = distPT
				currentBestName = match.namePT
			}
		}

		log.Print(logMsg)

		if currentMinDist < minDistance {
			minDistance = currentMinDist
			bestMatchID = match.id
			bestMatchName = currentBestName
		}
	}

	log.Printf("[DF] [ItemID/Levenshtein] Best proximity match for '%s' is '%s' (ID %d) with distance %d.", cleanItemName, bestMatchName, bestMatchID, minDistance)

	// 3. Check if the best match is within our acceptable threshold
	if bestMatchID != -1 && minDistance <= maxLevenshteinDistance {
		log.Printf("[D] [ItemID] Accepting proximity match for '%s' (ID %d). Distance %d is <= %d.", cleanItemName, bestMatchID, minDistance, maxLevenshteinDistance)
		return sql.NullInt64{Int64: bestMatchID, Valid: true}, true
	}

	// 4. Fallback for cards (unchanged)
	if strings.Contains(lowerCleanItemName, "card") || strings.Contains(lowerCleanItemName, "carta") {
		if len(potentialMatches) > 0 {
			itemID := potentialMatches[0].id // Trust the first result from LIKE
			log.Printf("[D] [ItemID] Found %d LIKE matches for '%s'. Proximity match rejected (dist %d > %d). Using first result (ID %d) due to 'card'/'carta' keyword.", len(potentialMatches), cleanItemName, minDistance, maxLevenshteinDistance, itemID)
			return sql.NullInt64{Int64: itemID, Valid: true}, true
		}
	}

	// 5. All ambiguity checks failed.
	log.Printf("[D] [ItemID] Found %d ambiguous LIKE matches for '%s'. Proximity match rejected (dist %d > %d). Proceeding to online search.", len(potentialMatches), cleanItemName, minDistance, maxLevenshteinDistance)
	return sql.NullInt64{Valid: false}, false
}

// findItemIDOnline performs concurrent web scrapes to find an item ID.
func findItemIDOnline(cleanItemName string, slots int) (sql.NullInt64, bool) {
	log.Printf("[D] [ItemID] No local FTS match for '%s'. Initiating online search...", cleanItemName)

	var wg sync.WaitGroup
	// --- MODIFICATION: Removed rmsResults and rmsErr ---
	var rdbResults []ItemSearchResult
	var rodbErr error

	// --- MODIFICATION: Removed one item from wg.Add() ---
	wg.Add(1)
	// --- MODIFICATION: Removed goroutine for scrapeRMSItemSearch ---
	// go func() { ... }()
	// --- END MODIFICATION ---
	go func() {
		defer wg.Done()
		rdbResults, rodbErr = scrapeRODatabaseSearch(cleanItemName, slots)
		if rodbErr != nil {
			log.Printf("[W] [ItemID] RDB Search failed for '%s': %v", cleanItemName, rodbErr)
		}
	}()
	wg.Wait()

	combinedIDs := make(map[int]string)
	// --- MODIFICATION: Removed loop for rmsResults ---
	if rdbResults != nil {
		for _, res := range rdbResults {
			if _, ok := combinedIDs[res.ID]; !ok {
				combinedIDs[res.ID] = res.Name
			}
		}
	}
	// --- END MODIFICATION ---

	if len(combinedIDs) == 1 {
		var foundID int
		// var foundName string // No longer needed
		for id, name := range combinedIDs {
			foundID = id
			_ = name // foundName = name
		}
		log.Printf("[D] [ItemID] Found unique ONLINE match for '%s': ID %d", cleanItemName, foundID)
		// --- MODIFICATION: Removed background caching ---
		// go scrapeAndCacheItemIfNotExists(foundID, foundName) // This function no longer exists
		// --- END MODIFICATION ---
		return sql.NullInt64{Int64: int64(foundID), Valid: true}, true
	}

	if len(combinedIDs) > 1 {
		// Check for a perfect match among online results
		for id, name := range combinedIDs {
			nameWithoutSlots := reSlotRemover.ReplaceAllString(name, " ")
			nameWithoutSlots = strings.TrimSpace(nameWithoutSlots)

			if name == cleanItemName || (nameWithoutSlots != "" && nameWithoutSlots == cleanItemName) {
				log.Printf("[D] [ItemID] Found perfect match (exact or slot-stripped) '%s' (ID %d) within ONLINE results.", cleanItemName, id)
				// --- MODIFICATION: Removed background caching ---
				// go scrapeAndCacheItemIfNotExists(id, name) // This function no longer exists
				// --- END MODIFICATION ---
				return sql.NullInt64{Int64: int64(id), Valid: true}, true
			}
		}

		// Fallback for cards: trust the first result
		lowerCleanItemName := strings.ToLower(cleanItemName)
		if strings.Contains(lowerCleanItemName, "card") || strings.Contains(lowerCleanItemName, "carta") {
			var foundID int
			// var foundName string // No longer needed
			for id, name := range combinedIDs {
				foundID = id
				_ = name // foundName = name
				break    // Get the first one
			}
			log.Printf("[D] [ItemID] Found %d ONLINE matches for '%s'. Using first result (ID %d) due to 'card'/'carta' keyword.", len(combinedIDs), cleanItemName, foundID)
			// --- MODIFICATION: Removed background caching ---
			// go scrapeAndCacheItemIfNotExists(foundID, foundName) // This function no longer exists
			// --- END MODIFICATION ---
			return sql.NullInt64{Int64: int64(foundID), Valid: true}, true
		}

		log.Printf("[D] [ItemID] Found %d ambiguous ONLINE matches for '%s'. Not selecting.", len(combinedIDs), cleanItemName)
	}

	// No online matches or ambiguous matches
	return sql.NullInt64{Valid: false}, false
}

// woeRankingsHandler fetches and displays WoE rankings for characters or guilds.
func woeRankingsHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	searchQuery := r.FormValue("query")
	activeTab := r.FormValue("tab")
	if activeTab == "" {
		activeTab = "characters" // Default to character view
	}

	var characters []WoeCharacterRank
	var guilds []WoeGuildRank
	var allowedSorts map[string]string
	var orderByClause, sortBy, order string
	var whereConditions []string
	var queryParams []interface{}
	var whereClause string

	if activeTab == "guilds" {
		// --- GUILD RANKING LOGIC ---
		allowedSorts = map[string]string{
			"guild":    "guild_name",
			"members":  "member_count",
			"kills":    "total_kills",
			"deaths":   "total_deaths",
			"kd":       "kd_ratio",
			"damage":   "total_damage",
			"healing":  "total_healing",
			"emperium": "total_emp_kills",
			"points":   "total_points",
		}
		// Default sort by total kills DESC
		orderByClause, sortBy, order = getSortClause(r, allowedSorts, "kills", "DESC")

		if searchQuery != "" {
			whereConditions = append(whereConditions, "guild_name LIKE ?")
			queryParams = append(queryParams, "%"+searchQuery+"%")
		}
		if len(whereConditions) > 0 {
			whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
		}

		// Query to aggregate stats by guild
		query := fmt.Sprintf(`
			SELECT
				guild_name,
				guild_id,
				COUNT(*) AS member_count,
				SUM(kill_count) AS total_kills,
				SUM(death_count) AS total_deaths,
				SUM(damage_done) AS total_damage,
				SUM(healing_done) AS total_healing,
				SUM(emperium_kill) AS total_emp_kills,
				SUM(points) AS total_points,
				CASE
					WHEN SUM(death_count) = 0 THEN SUM(kill_count) -- Avoid division by zero
					ELSE CAST(SUM(kill_count) AS REAL) / SUM(death_count)
				END AS kd_ratio
			FROM woe_character_rankings
			%s -- whereClause
			GROUP BY guild_name, guild_id
			%s -- orderByClause
		`, whereClause, orderByClause)

		rows, err := db.Query(query, queryParams...)
		if err != nil {
			log.Printf("[E] [HTTP/WoE] Could not query for WoE guild rankings: %v | Query: %s | Params: %v", err, query, queryParams)
			http.Error(w, "Could not query WoE guild rankings", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var g WoeGuildRank
			err := rows.Scan(
				&g.GuildName, &g.GuildID, &g.MemberCount,
				&g.TotalKills, &g.TotalDeaths, &g.TotalDamage,
				&g.TotalHealing, &g.TotalEmpKills, &g.TotalPoints,
				&g.KillDeathRatio,
			)
			if err != nil {
				log.Printf("[W] [HTTP/WoE] Failed to scan WoE guild row: %v", err)
				continue
			}
			guilds = append(guilds, g)
		}

	} else {
		// --- CHARACTER RANKING LOGIC (Original logic) ---
		allowedSorts = map[string]string{
			"name":     "name",
			"class":    "class",
			"guild":    "guild_name",
			"kills":    "kill_count",
			"deaths":   "death_count",
			"damage":   "damage_done",
			"emperium": "emperium_kill",
			"healing":  "healing_done",
			"score":    "score",
			"points":   "points",
		}
		// Default sort by kills DESC
		orderByClause, sortBy, order = getSortClause(r, allowedSorts, "kills", "DESC")

		if searchQuery != "" {
			// Use LIKE for partial matching
			whereConditions = append(whereConditions, "name LIKE ?")
			queryParams = append(queryParams, "%"+searchQuery+"%")
		}
		if len(whereConditions) > 0 {
			whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
		}

		query := fmt.Sprintf(`
			SELECT name, class, guild_id, guild_name,
				   kill_count, death_count, damage_done, emperium_kill,
				   healing_done, score, points
			FROM woe_character_rankings
			%s -- whereClause
			%s -- orderByClause
		`, whereClause, orderByClause)

		rows, err := db.Query(query, queryParams...)
		if err != nil {
			log.Printf("[E] [HTTP/WoE] Could not query for WoE character rankings: %v | Query: %s | Params: %v", err, query, queryParams)
			http.Error(w, "Could not query WoE rankings", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var c WoeCharacterRank
			err := rows.Scan(
				&c.Name, &c.Class, &c.GuildID, &c.GuildName,
				&c.KillCount, &c.DeathCount, &c.DamageDone, &c.EmperiumKill,
				&c.HealingDone, &c.Score, &c.Points,
			)
			if err != nil {
				log.Printf("[W] [HTTP/WoE] Failed to scan WoE character row: %v", err)
				continue
			}
			characters = append(characters, c)
		}
	}

	data := WoePageData{
		Characters:     characters,
		Guilds:         guilds,
		ActiveTab:      activeTab,
		LastScrapeTime: GetLastUpdateTime("last_updated", "woe_character_rankings"),
		SortBy:         sortBy,
		Order:          order,
		SearchQuery:    searchQuery,
		PageTitle:      "WoE Rankings",
	}
	renderTemplate(w, "woe_rankings.html", data)
}

// findItemIDByName orchestrates searching the cache and online for an item ID.
// This function remains unchanged but is shown for context.
func findItemIDByName(itemName string, allowRetry bool, slots int) (sql.NullInt64, error) {
	// 1. Clean the name
	reRefine := regexp.MustCompile(`\s*\+\d+\s*`)
	// --- MODIFICATION: Use reSlotRemover to strip [1], etc. ---
	cleanItemName := reSlotRemover.ReplaceAllString(itemName, " ")
	cleanItemName = reRefine.ReplaceAllString(cleanItemName, "")
	// --- END MODIFICATION ---
	cleanItemName = strings.TrimSpace(cleanItemName)
	cleanItemName = sanitizeString(cleanItemName, itemSanitizer)

	if strings.TrimSpace(cleanItemName) == "" {
		return sql.NullInt64{Valid: false}, nil
	}

	// 2. Handle special case: "Zeny"
	if strings.ToLower(cleanItemName) == "zeny" {
		log.Printf("[D] [ItemID] Detected special item 'Zeny'. Skipping ID search.")
		return sql.NullInt64{Valid: false}, nil
	}

	// 3. Try local FTS cache first (This is the modified function)
	// --- MODIFICATION: Pass 'slots' parameter ---
	if itemID, found := findItemIDInCache(cleanItemName, slots); found {
		// --- END MODIFICATION ---
		return itemID, nil
	}

	// 4. If not found, try online search
	if itemID, found := findItemIDOnline(cleanItemName, slots); found {
		return itemID, nil
	}

	// 5. If still not found, handle retry logic for "card" or "carta"
	lowerCleanItemName := strings.ToLower(cleanItemName)
	isCard := strings.Contains(lowerCleanItemName, "card") || strings.Contains(lowerCleanItemName, "carta")

	if allowRetry && isCard {
		newName := reCardRemover.ReplaceAllString(itemName, " ")
		newName = strings.TrimSpace(newName)

		if newName != "" && newName != cleanItemName {
			log.Printf("[D] [ItemID] No results for '%s'. Retrying search without 'card'/'carta' as: '%s'", cleanItemName, newName)
			// Call recursively, but with allowRetry=false to prevent infinite loops
			return findItemIDByName(newName, false, slots)
		}
	}

	// 6. All attempts failed
	log.Printf("[D] [ItemID] All searches for '%s' returned no results or were ambiguous. Storing name only.", cleanItemName)
	return sql.NullInt64{Valid: false}, nil
}

// findAndDeleteOldPosts performs a pre-transaction to clean up duplicate posts.
func findAndDeleteOldPosts(tx *sql.Tx, characterName, discordContact, postType string, itemNames []string, itemParams []interface{}) {
	if len(itemNames) == 0 {
		return
	}

	placeholders := strings.Repeat("?,", len(itemNames)-1) + "?"
	findQuery := fmt.Sprintf(`
		SELECT DISTINCT p.id
		FROM trading_posts p
		JOIN trading_post_items i ON p.id = i.post_id
		WHERE p.character_name = ?
		  AND p.contact_info = ?
		  AND p.post_type = ?
		  AND i.item_name IN (%s)
	`, placeholders)

	findParams := []interface{}{characterName, discordContact, postType}
	findParams = append(findParams, itemParams...)

	rows, err := tx.Query(findQuery, findParams...)
	if err != nil {
		log.Printf("[W] [Discord] Failed to query for old posts to delete for '%s': %v", characterName, err)
		return // Don't fail the transaction, just log the error
	}
	defer rows.Close()

	var postIDsToDelete []interface{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err == nil {
			postIDsToDelete = append(postIDsToDelete, id)
		}
	}

	if len(postIDsToDelete) > 0 {
		delPlaceholders := strings.Repeat("?,", len(postIDsToDelete)-1) + "?"
		delQuery := fmt.Sprintf("DELETE FROM trading_posts WHERE id IN (%s)", delPlaceholders)

		delRes, err := tx.Exec(delQuery, postIDsToDelete...)
		if err != nil {
			log.Printf("[W] [Discord] Failed to delete old post(s) for '%s': %v", characterName, err)
		} else if deletedCount, _ := delRes.RowsAffected(); deletedCount > 0 {
			log.Printf("[I] [Discord] Deleted %d old '%s' post(s) for user '%s' because they contained matching items.", deletedCount, postType, characterName)
		}
	}
}

// createSingleTradingPost now uses the helper for cleanup.
func createSingleTradingPost(authorName, originalMessage, postType string, items []GeminiTradeItem) (int64, error) {
	characterName := sanitizeString(authorName, nameSanitizer)
	if strings.TrimSpace(characterName) == "" {
		return 0, fmt.Errorf("author name is empty after sanitization")
	}

	title := fmt.Sprintf("%s items via Discord", strings.Title(postType))
	discordContact := fmt.Sprintf("Discord: %s", authorName)

	// Generate token hash
	token, err := generateSecretToken(16)
	if err != nil {
		return 0, fmt.Errorf("could not generate security token: %w", err)
	}
	tokenHash, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		return 0, fmt.Errorf("could not hash token: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to start database transaction: %w", err)
	}
	defer tx.Rollback()

	// --- Refactored Part ---
	// 1. Find and delete old posts within the transaction
	var itemNames []string
	var itemParams []interface{}
	for _, item := range items {
		itemName := sanitizeString(item.Name, itemSanitizer)
		if strings.TrimSpace(itemName) != "" {
			itemNames = append(itemNames, itemName)
			itemParams = append(itemParams, itemName)
		}
	}
	findAndDeleteOldPosts(tx, characterName, discordContact, postType, itemNames, itemParams)
	// --- End Refactored Part ---

	// 2. Insert the new main post
	res, err := tx.Exec(`INSERT INTO trading_posts (title, post_type, character_name, contact_info, notes, created_at, edit_token_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
		title, postType, characterName, discordContact,
		originalMessage, time.Now().Format(time.RFC3339), string(tokenHash),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to save post from discord: %w", err)
	}
	postID, _ := res.LastInsertId()

	// 3. Prepare and insert all items
	stmt, err := tx.Prepare("INSERT INTO trading_post_items (post_id, item_name, item_id, quantity, price_zeny, price_rmt, payment_methods, refinement, slots, card1, card2, card3, card4) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return 0, fmt.Errorf("database preparation failed for discord post items: %w", err)
	}
	defer stmt.Close()

	for _, item := range items {
		itemName := sanitizeString(item.Name, itemSanitizer)
		if strings.TrimSpace(itemName) == "" {
			continue
		}

		itemID, findErr := findItemIDByName(itemName, true, item.Slots)
		if findErr != nil {
			log.Printf("[W] [Discord] Error finding item ID for '%s': %v. Proceeding without ID.", itemName, findErr)
		}

		paymentMethods := "zeny"
		if item.PaymentMethods == "rmt" || item.PaymentMethods == "both" {
			paymentMethods = item.PaymentMethods
		}

		card1 := sql.NullString{String: item.Card1, Valid: item.Card1 != ""}
		card2 := sql.NullString{String: item.Card2, Valid: item.Card2 != ""}
		card3 := sql.NullString{String: item.Card3, Valid: item.Card3 != ""}
		card4 := sql.NullString{String: item.Card4, Valid: item.Card4 != ""}

		if _, err := stmt.Exec(postID, itemName, itemID, item.Quantity, item.PriceZeny, item.PriceRMT, paymentMethods, item.Refinement, item.Slots, card1, card2, card3, card4); err != nil {
			return 0, fmt.Errorf("failed to save item '%s' for discord post: %w", itemName, err)
		}
	}

	// 4. Commit
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to finalize discord post transaction: %w", err)
	}

	return postID, nil
}

func CreateTradingPostFromDiscord(authorName string, originalMessage string, tradeData *GeminiTradeResult) ([]int64, error) {
	var buyingItems []GeminiTradeItem
	var sellingItems []GeminiTradeItem
	var postIDs []int64
	var finalError error

	for _, item := range tradeData.Items {
		if item.Action == "buying" {
			buyingItems = append(buyingItems, item)
		} else {

			sellingItems = append(sellingItems, item)
		}
	}

	if len(buyingItems) > 0 {
		postID, err := createSingleTradingPost(authorName, originalMessage, "buying", buyingItems)
		if err != nil {
			log.Printf("[E] [Discord] Failed to create 'buying' post for '%s': %v", authorName, err)
			finalError = err
		} else {
			postIDs = append(postIDs, postID)
		}
	}

	if len(sellingItems) > 0 {
		postID, err := createSingleTradingPost(authorName, originalMessage, "selling", sellingItems)
		if err != nil {
			log.Printf("[E] [Discord] Failed to create 'selling' post for '%s': %v", authorName, err)
			finalError = err
		} else {
			postIDs = append(postIDs, postID)
		}
	}

	return postIDs, finalError
}
