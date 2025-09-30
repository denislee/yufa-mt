package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
)

// startBackgroundScraper starts a ticker to scrape data at a fixed interval.
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

// scrapeData performs a single scrape of the market data.
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

// areItemSetsIdentical compares two slices of Items to see if they are identical.
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
