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
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
)

// --- CONFIGURATION ---
// These constants control verbose logging for each specific scraper module.
// Set to true to see detailed, step-by-step progress for a given job.
const enablePlayerCountDebugLogs = false
const enableCharacterScraperDebugLogs = false
const enableGuildScraperDebugLogs = false
const enableMarketScraperDebugLogs = true

// newOptimizedAllocator creates a new chromedp allocator context with optimized flags for scraping.
// It disables unnecessary resources like images and extensions to improve performance.
func newOptimizedAllocator() (context.Context, context.CancelFunc) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.UserAgent(`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36`),
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-extensions", true),              // Disable extensions
		chromedp.Flag("blink-settings", "imagesEnabled=false"), // Disable images
	)
	return chromedp.NewExecAllocator(context.Background(), opts...)
}

// scrapeAndStorePlayerCount fetches the online player count, queries for the unique seller count, and saves them.
func scrapeAndStorePlayerCount() {
	log.Println("📊 [Counter] Checking player and seller count...")

	// Use the optimized allocator
	allocCtx, cancel := newOptimizedAllocator()
	defer cancel()
	ctx, cancel := chromedp.NewContext(allocCtx)
	defer cancel()
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second) // 30-second timeout
	defer cancel()

	var playerCountText string
	err := chromedp.Run(ctx,
		chromedp.Navigate("https://projetoyufa.com/info"),

		chromedp.WaitVisible(`span[data-slot="badge"] p`), // Wait for the element to be visible
		chromedp.Text(`span[data-slot="badge"] p`, &playerCountText, chromedp.ByQuery),
	)

	if err != nil {
		log.Printf("❌ [Counter] Failed to get player info: %v", err)
		return
	}

	var onlineCount int
	var found bool

	if playerCountText != "" {
		re := regexp.MustCompile(`\d+`)
		numStr := re.FindString(playerCountText)
		if num, err := strconv.Atoi(numStr); err == nil {
			onlineCount = num
			found = true
		}
	}

	if !found {
		log.Println("⚠️ [Counter] Could not find player count on the info page after successful load.")
		return
	}

	// Get the number of unique sellers with available items directly from the database.
	var sellerCount int
	err = db.QueryRow("SELECT COUNT(DISTINCT seller_name) FROM items WHERE is_available = 1").Scan(&sellerCount)
	if err != nil {
		log.Printf("⚠️ [Counter] Could not query for unique seller count: %v", err)
		// Don't return, as we can still store the player count. Default sellerCount to 0.
		sellerCount = 0
	}

	// Check if the latest counts are different from the new ones to avoid duplicate entries.
	var lastPlayerCount int
	var lastSellerCount sql.NullInt64
	err = db.QueryRow("SELECT count, seller_count FROM player_history ORDER BY timestamp DESC LIMIT 1").Scan(&lastPlayerCount, &lastSellerCount)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ [Counter] Could not query for last player/seller count: %v", err)
		return
	}

	// If both counts are the same as the last record, do nothing.
	if err != sql.ErrNoRows && onlineCount == lastPlayerCount && lastSellerCount.Valid && sellerCount == int(lastSellerCount.Int64) {
		if enablePlayerCountDebugLogs {
			log.Printf("[Counter] Player/seller count unchanged (%d players, %d sellers). No update needed.", onlineCount, sellerCount)
		}
		return
	}

	retrievalTime := time.Now().Format(time.RFC3339)
	_, err = db.Exec("INSERT INTO player_history (timestamp, count, seller_count) VALUES (?, ?, ?)", retrievalTime, onlineCount, sellerCount)
	if err != nil {
		log.Printf("❌ [Counter] Failed to insert new player/seller count: %v", err)
		return
	}

	log.Printf("✅ [Counter] Player/seller count updated. New values: %d players, %d sellers", onlineCount, sellerCount)
}

func scrapePlayerCharacters() {
	log.Println("🏆 [Characters] Starting player character scrape...")

	const maxRetries = 3
	const batchSize = 1 // Number of pages to scrape in parallel

	// Use the optimized allocator for the entire job
	allocCtx, cancel := newOptimizedAllocator()
	defer cancel()

	// --- NEW: Find the last page number first ---
	var lastPage = 1 // Default to 1 page
	log.Println("🏆 [Characters] Determining total number of pages...")
	firstPageCtx, cancelFirstPage := chromedp.NewContext(allocCtx)
	defer cancelFirstPage()
	firstPageCtx, cancelTimeout := context.WithTimeout(firstPageCtx, 60*time.Second)
	defer cancelTimeout()

	var initialHtmlContent string
	err := chromedp.Run(firstPageCtx,
		chromedp.Navigate("https://projetoyufa.com/rankings?page=1"),
		chromedp.WaitVisible(`nav[aria-label="pagination"]`), // Wait for the pagination nav bar
		chromedp.OuterHTML("html", &initialHtmlContent),
	)

	if err != nil {
		log.Printf("⚠️ [Characters] Could not find pagination on page 1. Assuming only one page. Error: %v", err)
	} else {
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(initialHtmlContent))
		pageRegex := regexp.MustCompile(`page=(\d+)`)
		// Find all links within the pagination bar
		doc.Find(`nav[aria-label="pagination"] a[href*="?page="]`).Each(func(i int, s *goquery.Selection) {
			if href, exists := s.Attr("href"); exists {
				matches := pageRegex.FindStringSubmatch(href)
				if len(matches) > 1 {
					if p, err := strconv.Atoi(matches[1]); err == nil {
						if p > lastPage {
							lastPage = p // Keep track of the highest page number found
						}
					}
				}
			}
		})
	}
	log.Printf("✅ [Characters] Found %d total pages to scrape.", lastPage)
	// --- END of finding last page ---

	// Fetch existing player data for comparison ONCE at the beginning.
	if enableCharacterScraperDebugLogs {
		log.Println("    -> [DB] Fetching existing player data for activity comparison...")
	}
	existingPlayers := make(map[string]PlayerCharacter)
	rowsPre, err := db.Query("SELECT name, experience, last_active FROM characters")
	if err != nil {
		log.Printf("❌ [DB] Failed to query existing characters for comparison: %v", err)
	} else {
		defer rowsPre.Close()
		for rowsPre.Next() {
			var p PlayerCharacter
			if err := rowsPre.Scan(&p.Name, &p.Experience, &p.LastActive); err != nil {
				log.Printf("    -> [DB] WARN: Failed to scan existing player row: %v", err)
				continue
			}
			existingPlayers[p.Name] = p
		}
		if enableCharacterScraperDebugLogs {
			log.Printf("    -> [DB] Found %d existing player records for comparison.", len(existingPlayers))
		}
	}

	// Use a single timestamp for the entire scrape operation.
	updateTime := time.Now().Format(time.RFC3339)

	// --- MODIFIED LOOP: Loop from page 1 to the determined last page ---
	for startPage := 1; startPage <= lastPage; startPage += batchSize {
		var wg sync.WaitGroup
		playerChan := make(chan []PlayerCharacter, batchSize)

		log.Printf("🏆 [Characters] Scraping batch of up to %d pages starting from page %d (Total: %d)...", batchSize, startPage, lastPage)

		// Launch a goroutine for each page in the batch
		for i := 0; i < batchSize; i++ {
			currentPage := startPage + i

			// --- NEW: Ensure we don't try to scrape pages that don't exist ---
			if currentPage > lastPage {
				continue
			}

			wg.Add(1)
			go func(page int) {
				defer wg.Done()
				var htmlContent string
				var pageScrapedSuccessfully bool

				for attempt := 1; attempt <= maxRetries; attempt++ {
					taskCtx, cancelCtx := chromedp.NewContext(allocCtx)
					defer cancelCtx()
					taskCtx, cancelTimeout := context.WithTimeout(taskCtx, 60*time.Second)
					defer cancelTimeout()

					url := fmt.Sprintf("https://projetoyufa.com/rankings?page=%d", page)
					err := chromedp.Run(taskCtx,
						chromedp.Navigate(url),
						chromedp.WaitVisible(`tbody[data-slot="table-body"] tr[data-slot="table-row"]`),
						chromedp.Sleep(500*time.Millisecond),
						chromedp.OuterHTML("html", &htmlContent),
					)

					if err == nil {
						pageScrapedSuccessfully = true
						break
					}
					if attempt == maxRetries {
						log.Printf("    -> ❌ Error on page %d after %d attempts: %v", page, maxRetries, err)
					}
				}

				if !pageScrapedSuccessfully {
					playerChan <- nil // Signal failure for this page
					return
				}

				doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
				if err != nil {
					log.Printf("    -> ❌ Failed to parse HTML for page %d: %v", page, err)
					playerChan <- nil
					return
				}

				rows := doc.Find(`tbody[data-slot="table-body"] tr[data-slot="table-row"]`)
				if rows.Length() == 0 {
					playerChan <- []PlayerCharacter{} // Signal an empty but successful page
					return
				}

				var pagePlayers []PlayerCharacter
				rows.Each(func(i int, s *goquery.Selection) {
					var player PlayerCharacter
					var parseErr error

					cells := s.Find(`td[data-slot="table-cell"]`)
					if cells.Length() < 4 {
						return
					}
					rankStr := strings.TrimSpace(cells.Eq(0).Text())
					nameStr := strings.TrimSpace(cells.Eq(1).Text())
					levelStrRaw := cells.Eq(2).Find("div.mb-1.flex.justify-between.text-xs > span").Text()
					expStrRaw := cells.Eq(2).Find("div.absolute.inset-0.flex.items-center.justify-center > span").Text()
					classStr := cells.Eq(3).Find("span").Last().Text()

					player.Name = nameStr
					player.Class = classStr

					player.Rank, parseErr = strconv.Atoi(rankStr)
					if parseErr != nil {
						return
					}
					levelStrClean := strings.TrimSpace(strings.TrimPrefix(levelStrRaw, "Nv."))
					levelParts := strings.Split(levelStrClean, "/")
					if len(levelParts) == 2 {
						player.BaseLevel, _ = strconv.Atoi(strings.TrimSpace(levelParts[0]))
						player.JobLevel, _ = strconv.Atoi(strings.TrimSpace(levelParts[1]))
					}
					expStr := strings.TrimSuffix(strings.TrimSpace(expStrRaw), "%")
					player.Experience, _ = strconv.ParseFloat(expStr, 64)

					pagePlayers = append(pagePlayers, player)
				})
				playerChan <- pagePlayers
			}(currentPage)
		}

		wg.Wait()
		close(playerChan)

		// Consolidate results from the channel
		var batchPlayers []PlayerCharacter
		for players := range playerChan {
			if players != nil {
				batchPlayers = append(batchPlayers, players...)
			}
		}

		// If no players were found in this batch, something might be wrong, but we continue
		if len(batchPlayers) == 0 {
			log.Println("⚠️ [Characters] No players found in this batch. Continuing to next batch.")
			continue
		}

		log.Printf("🔎 [Characters] Found %d total players in batch from page %d. Processing for DB...", len(batchPlayers), startPage)

		// Open a new transaction for the entire batch
		tx, err := db.Begin()
		if err != nil {
			log.Printf("❌ [DB] Failed to begin transaction for batch from page %d: %v", startPage, err)
			break
		}

		stmt, err := tx.Prepare(`
            INSERT INTO characters (rank, name, base_level, job_level, experience, class, last_updated, last_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                rank=excluded.rank,
                base_level=excluded.base_level,
                job_level=excluded.job_level,
                experience=excluded.experience,
                class=excluded.class,
                last_updated=excluded.last_updated,
                last_active=excluded.last_active
        `)
		if err != nil {
			log.Printf("❌ [DB] Failed to prepare characters upsert statement: %v", err)
			tx.Rollback()
			break
		}

		for _, p := range batchPlayers {
			lastActiveTime := updateTime
			if oldPlayer, exists := existingPlayers[p.Name]; exists {
				if oldPlayer.Experience == p.Experience {
					lastActiveTime = oldPlayer.LastActive
				} else {
					if enableCharacterScraperDebugLogs {
						log.Printf("    -> [Activity] Player '%s' experience changed from %.2f%% to %.2f%%. Updating last_active.", p.Name, oldPlayer.Experience, p.Experience)
					}
				}
			}

			if _, err := stmt.Exec(p.Rank, p.Name, p.BaseLevel, p.JobLevel, p.Experience, p.Class, updateTime, lastActiveTime); err != nil {
				log.Printf("    -> [DB] WARN: Failed to upsert character for player %s: %v", p.Name, err)
			} else {
				if enableCharacterScraperDebugLogs {
					log.Printf("    -> [DB] Upserted: Name: %s, Rank: %d, Lvl: %d/%d, Class: %s, Exp: %.2f%%", p.Name, p.Rank, p.BaseLevel, p.JobLevel, p.Class, p.Experience)
				}
			}
		}
		stmt.Close()

		if err := tx.Commit(); err != nil {
			log.Printf("❌ [DB] Failed to commit transaction for batch from page %d: %v", startPage, err)
			break
		}
		log.Printf("✅ [Characters] Saved/updated %d records from batch starting at page %d.", len(batchPlayers), startPage)
	}

	// Cleanup logic remains the same
	log.Println("🧹 [Characters] Cleaning up old player records not found in this scrape...")
	result, err := db.Exec("DELETE FROM characters WHERE last_updated != ?", updateTime)
	if err != nil {
		log.Printf("❌ [Characters] Failed to clean up old player records: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		log.Printf("✅ [Characters] Cleanup complete. Removed %d stale player records.", rowsAffected)
	}

	log.Printf("✅ [Characters] Scrape and update process complete.")
}

// Helper structs for parsing the embedded guild JSON
type GuildMemberJSON struct {
	Name string `json:"name"`
}

type GuildJSON struct {
	Name    string            `json:"name"`
	Level   int               `json:"guild_lv"`
	Master  string            `json:"master"`
	Members []GuildMemberJSON `json:"members"`
}

// scrapeGuilds scrapes guild data, including members, and updates both the guilds and characters tables.
func scrapeGuilds() {
	log.Println("🏰 [Guilds] Starting guild and character-guild association scrape...")
	const maxRetries = 32

	allocOpts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-extensions", true), // Disable extensions
		//		chromedp.Flag("blink-settings", "imagesEnabled=false"), // Disable images
	)
	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), allocOpts...)
	defer cancel()

	// --- NEW: Find the last page number first ---
	var lastPage = 1 // Default to 1 page
	log.Println("🏰 [Guilds] Determining total number of pages...")

	// Create a new context just for this initial task
	firstPageCtx, cancelFirstPage := chromedp.NewContext(allocCtx)
	defer cancelFirstPage()
	firstPageCtx, cancelTimeout := context.WithTimeout(firstPageCtx, 60*time.Second)
	defer cancelTimeout()

	var initialHtmlContent string
	err := chromedp.Run(firstPageCtx,
		chromedp.Navigate("https://projetoyufa.com/rankings/guild?page=1"),
		chromedp.WaitVisible(`nav[aria-label="pagination"]`), // Wait for the pagination nav bar
		chromedp.OuterHTML("html", &initialHtmlContent),
	)

	if err != nil {
		log.Printf("⚠️ [Guilds] Could not find pagination on page 1. Assuming only one page. Error: %v", err)
	} else {
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(initialHtmlContent))
		pageRegex := regexp.MustCompile(`page=(\d+)`)
		// Find all links within the pagination bar
		doc.Find(`nav[aria-label="pagination"] a[href*="?page="]`).Each(func(i int, s *goquery.Selection) {
			if href, exists := s.Attr("href"); exists {
				matches := pageRegex.FindStringSubmatch(href)
				if len(matches) > 1 {
					if p, err := strconv.Atoi(matches[1]); err == nil {
						if p > lastPage {
							lastPage = p // Keep track of the highest page number found
						}
					}
				}
			}
		})
		log.Printf("✅ [Guilds] Found %d total pages to scrape.", lastPage)
	}
	// --- END of finding last page ---

	allGuilds := make(map[string]Guild)
	allMembers := make(map[string]string) // Map character name to guild name

	// --- MODIFIED LOOP: Loop from 1 to the last page found ---
	for page := 1; page <= lastPage; page++ {
		var htmlContent string
		var pageScrapedSuccessfully bool
		url := fmt.Sprintf("https://projetoyufa.com/rankings/guild?page=%d", page)

		for attempt := 1; attempt <= maxRetries; attempt++ {
			taskCtx, cancelCtx := chromedp.NewContext(allocCtx)
			defer cancelCtx()
			taskCtx, cancelTimeoutLoop := context.WithTimeout(taskCtx, 60*time.Second)
			defer cancelTimeoutLoop()

			log.Printf("🏰 [Guilds] Scraping page %d of %d (Attempt %d/%d)...", page, lastPage, attempt, maxRetries)

			err := chromedp.Run(taskCtx,
				chromedp.Navigate(url),
				chromedp.WaitVisible(`tbody[data-slot="table-body"] tr:has(td:nth-of-type(2))`),
				chromedp.WaitVisible(`div.group h3`),
				chromedp.OuterHTML("html", &htmlContent),
			)

			if err == nil {
				pageScrapedSuccessfully = true
				break
			}
			log.Printf("❌ [Guilds] Error on page %d, attempt %d/%d: %v", page, attempt, maxRetries, err)
			if attempt < maxRetries {
				time.Sleep(20 * time.Second)
			}
		}

		if !pageScrapedSuccessfully {
			log.Printf("❌ [Guilds] All %d attempts failed for page %d. Skipping this page.", maxRetries, page)
			return
			//continue // Skip to the next page in the loop
		}

		doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
		if err != nil {
			log.Printf("❌ [Guilds] Failed to parse HTML for page %d: %v", page, err)
			continue
		}

		guildRows := doc.Find(`tbody[data-slot="table-body"] > tr:not(:has(td[colspan="4"]))`)
		log.Printf("🔎 [Guilds] Found %d guild rows on page %d. Processing...", guildRows.Length(), page)

		guildRows.Each(func(i int, s *goquery.Selection) {
			var guild Guild
			var parseErr error

			cells := s.Find(`td[data-slot="table-cell"]`)
			if cells.Length() < 4 {
				log.Printf("    -> [Parser] WARN: Skipping row %d, expected at least 4 cells, got %d.", i, cells.Length())
				return
			}

			rankStr := strings.TrimSpace(cells.Eq(0).Text())
			nameStr := strings.TrimSpace(cells.Eq(1).Find("span").Text())
			levelStr := strings.TrimSpace(cells.Eq(2).Text())

			memberRow := s.Next()
			if memberRow.Length() == 0 {
				log.Printf("    -> [Parser] WARN: Could not find member detail row for guild '%s'.", nameStr)
				return
			}

			masterStr := ""
			memberRow.Find("div.group").Each(func(_ int, memberCard *goquery.Selection) {
				if strings.Contains(memberCard.Text(), "Líder") {
					masterStr = strings.TrimSpace(memberCard.Find("h3").Text())
				}
			})

			memberRow.Find("div.group h3").Each(func(_ int, memberNameTag *goquery.Selection) {
				memberName := strings.TrimSpace(memberNameTag.Text())
				if memberName != "" {
					allMembers[memberName] = nameStr
				}
			})

			guild.Name = nameStr
			guild.Master = masterStr
			guild.Rank, parseErr = strconv.Atoi(rankStr)
			if parseErr != nil {
				log.Printf("    -> [Parser] ERROR: Could not parse RANK for '%s' from value '%s'. Error: %v", nameStr, rankStr, parseErr)
				return
			}
			guild.Level, parseErr = strconv.Atoi(levelStr)
			if parseErr != nil {
				log.Printf("    -> [Parser] ERROR: Could not parse LEVEL for '%s' from value '%s'. Error: %v", nameStr, levelStr, parseErr)
				return
			}

			guild.Experience = 0
			guild.EmblemURL = ""

			log.Printf("    -> [Parser] SUCCESS: Parsed guild '%s' (Rank: %d, Level: %d, Master: '%s')", guild.Name, guild.Rank, guild.Level, guild.Master)
			allGuilds[guild.Name] = guild
		})

		time.Sleep(2 * time.Second)
	}

	if len(allGuilds) == 0 {
		log.Println("⚠️ [Guilds] Scrape finished with 0 total guilds found. Guild/character tables will not be updated.")
		return
	}

	tx, errDb := db.Begin()
	if errDb != nil {
		log.Printf("❌ [Guilds][DB] Failed to begin transaction for guilds update: %v", errDb)
		return
	}
	defer tx.Rollback()

	log.Println("    -> [DB] Clearing and repopulating 'guilds' table...")
	if _, err := tx.Exec("DELETE FROM guilds"); err != nil {
		log.Printf("❌ [Guilds][DB] Failed to clear guilds table: %v", err)
		return
	}

	guildStmt, err := tx.Prepare(`INSERT INTO guilds (rank, name, level, experience, master, emblem_url, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Printf("❌ [Guilds][DB] Failed to prepare guilds insert statement: %v", err)
		return
	}
	defer guildStmt.Close()

	updateTime := time.Now().Format(time.RFC3339)
	for _, g := range allGuilds {
		if _, err := guildStmt.Exec(g.Rank, g.Name, g.Level, g.Experience, g.Master, g.EmblemURL, updateTime); err != nil {
			log.Printf("    -> [DB] WARN: Failed to insert guild '%s': %v", g.Name, err)
		}
	}

	log.Printf("    -> [DB] Updating 'characters' table with guild associations for %d members...", len(allMembers))

	if _, err := tx.Exec("UPDATE characters SET guild_name = NULL"); err != nil {
		log.Printf("❌ [Guilds][DB] Failed to clear existing guild names from characters table: %v", err)
		return
	}

	charStmt, err := tx.Prepare("UPDATE characters SET guild_name = ? WHERE name = ?")
	if err != nil {
		log.Printf("❌ [Guilds][DB] Failed to prepare character guild update statement: %v", err)
		return
	}
	defer charStmt.Close()

	updateCount := 0
	for charName, guildName := range allMembers {
		res, err := charStmt.Exec(guildName, charName)
		if err != nil {
			log.Printf("    -> [DB] WARN: Failed to update guild for character '%s': %v", charName, err)
		} else {
			rowsAffected, _ := res.RowsAffected()
			if rowsAffected > 0 {
				updateCount++
			}
		}
	}
	log.Printf("    -> [DB] Successfully associated %d characters with their guilds.", updateCount)

	if err := tx.Commit(); err != nil {
		log.Printf("❌ [Guilds][DB] Failed to commit guilds and characters transaction: %v", err)
		return
	}

	log.Printf("✅ [Guilds] Scrape and update complete. Saved %d guild records and updated character associations.", len(allGuilds))
}

// scrapeData performs a single scrape of the market data.
func scrapeData() {
	log.Println("🚀 [Market] Starting scrape...")
	// Compile regexes once for efficiency.
	reRefineMid := regexp.MustCompile(`\s(\+\d+)`)
	reRefineStart := regexp.MustCompile(`^(\+\d+)\s`)

	// Use the optimized allocator
	allocCtx, cancel := newOptimizedAllocator()
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
		log.Printf("❌ [Market] Failed to run chromedp tasks: %v", err)
		return
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("❌ [Market] Failed to parse HTML: %v", err)
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

	log.Printf("🔎 [Market] Scrape parsed. Found %d unique item names.", len(scrapedItemsByName))
	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [Market] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("INSERT OR IGNORE INTO scrape_history (timestamp) VALUES (?)", retrievalTime)
	if err != nil {
		log.Printf("❌ [Market] Failed to log scrape history: %v", err)
		return
	}

	// --- FIX IS HERE ---
	// Changed '=' to ':=' to correctly declare the 'rows' variable.
	rows, err := tx.Query("SELECT DISTINCT name_of_the_item FROM items WHERE is_available = 1")
	if err != nil {
		log.Printf("❌ [Market] Could not get list of available items: %v", err)
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
			log.Printf("⚠️ [Market] Failed to query for existing item %s: %v", itemName, err)
			continue
		}
		for rows.Next() {
			var item Item
			err := rows.Scan(&item.Name, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &item.MapName, &item.MapCoordinates)
			if err != nil {
				log.Printf("⚠️ [Market] Failed to scan existing item: %v", err)
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
			log.Printf("❌ [Market] Failed to mark old %s as unavailable: %v", itemName, err)
			continue
		}

		insertSQL := `INSERT INTO items(name_of_the_item, item_id, quantity, price, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates, is_available) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			log.Printf("⚠️ [Market] Could not prepare insert for %s: %v", itemName, err)
			continue
		}
		for _, item := range currentScrapedItems {
			if _, err := stmt.Exec(item.Name, item.ItemID, item.Quantity, item.Price, item.StoreName, item.SellerName, retrievalTime, item.MapName, item.MapCoordinates); err != nil {
				log.Printf("⚠️ [Market] Could not execute insert for %s: %v", item.Name, err)
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
					log.Printf("❌ [Market] Failed to log ADDED event for %s: %v", itemName, err)
				}

				go scrapeAndCacheItemIfNotExists(firstItem.ItemID, itemName)
			}

			var historicalLowestPrice sql.NullInt64
			err := tx.QueryRow(`SELECT MIN(CAST(REPLACE(price, ',', '') AS INTEGER)) FROM items WHERE name_of_the_item = ?`, itemName).Scan(&historicalLowestPrice)
			if err != nil && err != sql.ErrNoRows {
				log.Printf("⚠️ [Market] Could not get historical lowest price for %s: %v", itemName, err)
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
					log.Printf("❌ [Market] Failed to log NEW_LOW event for %s: %v", itemName, err)
				}
			}
		} else {
			itemsUpdated++
		}
	}

	itemsRemoved := 0
	for name := range dbAvailableNames {
		if _, foundInScrape := scrapedItemsByName[name]; !foundInScrape {
			// --- FIX START ---
			var itemID int // Declare itemID
			err := tx.QueryRow("SELECT item_id FROM items WHERE name_of_the_item = ? AND item_id > 0 LIMIT 1", name).Scan(&itemID)
			if err != nil {
				// If no ID is found, log it but default to 0 so the event can still be created.
				log.Printf("⚠️ [Market] Could not find item_id for removed item '%s', logging event with item_id 0: %v", name, err)
				itemID = 0
			}
			// --- FIX END ---
			_, err = tx.Exec(`INSERT INTO market_events (event_timestamp, event_type, item_name, item_id, details) VALUES (?, 'REMOVED', ?, ?, '{}')`, retrievalTime, name, itemID)
			if err != nil {
				log.Printf("❌ [Market] Failed to log REMOVED event for %s: %v", name, err)
			}
			if _, err := tx.Exec("UPDATE items SET is_available = 0 WHERE name_of_the_item = ?", name); err != nil {
				log.Printf("❌ [Market] Failed to mark disappeared item %s as unavailable: %v", name, err)
			} else {
				itemsRemoved++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ [Market] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("✅ [Market] Scrape complete. Unchanged: %d groups. Updated: %d groups. Newly Added: %d groups. Removed: %d groups.", itemsUnchanged, itemsUpdated, itemsAdded, itemsRemoved)
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
				ItemID:         item.ID,
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

// startBackgroundJobs starts all recurring background tasks.
func startBackgroundJobs() {
	// --- Market Scraper ---
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		scrapeData() // Run once immediately on start
		for {
			log.Printf("🕒 [Job] Waiting for the next 5-minute market scrape schedule...")
			<-ticker.C
			scrapeData()
		}
	}()

	// --- Player Count Scraper ---
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		scrapeAndStorePlayerCount() // Run once immediately on start
		for {
			log.Printf("🕒 [Job] Waiting for the next 1-minute player count schedule...")
			<-ticker.C
			scrapeAndStorePlayerCount()
		}
	}()

	// --- Guild Scraper ---
	go func() {
		ticker := time.NewTicker(60 * time.Minute)
		defer ticker.Stop()
		scrapeGuilds()
		for {
			log.Printf("🕒 [Job] Waiting for the next 30-minute guild schedule...")
			<-ticker.C
			scrapeGuilds()
		}
	}()

	// --- Player Character Scraper ---
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		//scrapePlayerCharacters()
		for {
			log.Printf("🕒 [Job] Waiting for the next 60-minute player character schedule...")
			<-ticker.C
			scrapePlayerCharacters()
		}
	}()

}
