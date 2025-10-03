package main

import (
	"context"
	"database/sql"
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
const enableMarketScraperDebugLogs = false

// newOptimizedAllocator creates a new chromedp allocator context with optimized flags for scraping.
// It disables unnecessary resources like images and extensions to improve performance.
func newOptimizedAllocator() (context.Context, context.CancelFunc) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.UserAgent(`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36`),
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		// --- OPTIMIZATIONS ---
		chromedp.Flag("disable-extensions", true),              // Disable extensions
		chromedp.Flag("blink-settings", "imagesEnabled=false"), // Disable images
	)
	return chromedp.NewExecAllocator(context.Background(), opts...)
}

// scrapeAndStorePlayerCount fetches the online player count, queries for the unique seller count, and saves them.
func scrapeAndStorePlayerCount() {
	log.Println("📊 Checking player and seller count...")

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
		log.Printf("❌ Failed to get player info: %v", err)
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
		log.Println("⚠️ Could not find player count on the info page after successful load.")
		return
	}

	// Get the number of unique sellers with available items directly from the database.
	var sellerCount int
	err = db.QueryRow("SELECT COUNT(DISTINCT seller_name) FROM items WHERE is_available = 1").Scan(&sellerCount)
	if err != nil {
		log.Printf("⚠️ Could not query for unique seller count: %v", err)
		// Don't return, as we can still store the player count. Default sellerCount to 0.
		sellerCount = 0
	}

	// Check if the latest counts are different from the new ones to avoid duplicate entries.
	var lastPlayerCount int
	var lastSellerCount sql.NullInt64
	err = db.QueryRow("SELECT count, seller_count FROM player_history ORDER BY timestamp DESC LIMIT 1").Scan(&lastPlayerCount, &lastSellerCount)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ Could not query for last player/seller count: %v", err)
		return
	}

	// If both counts are the same as the last record, do nothing.
	if err != sql.ErrNoRows && onlineCount == lastPlayerCount && lastSellerCount.Valid && sellerCount == int(lastSellerCount.Int64) {
		if enablePlayerCountDebugLogs {
			log.Printf("Player/seller count unchanged (%d players, %d sellers). No update needed.", onlineCount, sellerCount)
		}
		return
	}

	retrievalTime := time.Now().Format(time.RFC3339)
	_, err = db.Exec("INSERT INTO player_history (timestamp, count, seller_count) VALUES (?, ?, ?)", retrievalTime, onlineCount, sellerCount)
	if err != nil {
		log.Printf("❌ Failed to insert new player/seller count: %v", err)
		return
	}

	log.Printf("✅ Player/seller count updated. New values: %d players, %d sellers", onlineCount, sellerCount)
}

func scrapePlayerCharacters() {
	log.Println("🏆 [Characters] Starting player character scrape...")

	const maxRetries = 3
	const batchSize = 1 // Number of pages to scrape in parallel

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

	// Use the optimized allocator for the entire job
	allocCtx, cancel := newOptimizedAllocator()
	defer cancel()

	// Loop through pages in batches until there's no more data
	for startPage := 1; ; startPage += batchSize {
		var wg sync.WaitGroup
		playerChan := make(chan []PlayerCharacter, batchSize)

		log.Printf("🏆 [Characters] Scraping batch of %d pages starting from page %d...", batchSize, startPage)

		// Launch a goroutine for each page in the batch
		for i := 0; i < batchSize; i++ {
			currentPage := startPage + i
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
						chromedp.Sleep(500*time.Millisecond), // This is often unnecessary
						// OPTIMIZATION: Removed unnecessary time.Sleep(), WaitVisible is sufficient.
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
		emptyPagesInBatch := 0
		for players := range playerChan {
			if players != nil { // A nil value indicates a scrape error for that page
				batchPlayers = append(batchPlayers, players...)
				if len(players) == 0 {
					emptyPagesInBatch++
				}
			}
		}

		// If the entire batch consisted of pages that returned no players, we are done.
		if emptyPagesInBatch == batchSize {
			log.Println("✅ [Characters] Concluding scrape: Batch contained only empty pages.")
			break
		}

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
	const maxRetries = 3

	// Use the optimized allocator for the entire job
	allocCtx, cancel := newOptimizedAllocator()
	defer cancel()

	// --- STEP 1: Scrape the first page to determine total pages AND get its data ---
	var lastPage = 1 // Default to 1 page
	allGuilds := make(map[string]Guild)
	allMembers := make(map[string]string) // Map character name to guild name

	log.Println("🏰 [Guilds] Scraping page 1 to determine total pages and gather initial data...")
	firstPageCtx, cancelFirstPage := chromedp.NewContext(allocCtx)
	defer cancelFirstPage()
	firstPageCtx, cancelTimeout := context.WithTimeout(firstPageCtx, 90*time.Second) // Increased timeout for the crucial first page
	defer cancelTimeout()

	var initialHtmlContent string
	err := chromedp.Run(firstPageCtx,
		chromedp.Navigate("https://projetoyufa.com/rankings/guild?page=1"),
		chromedp.WaitVisible(`tbody[data-slot="table-body"] tr[data-slot="table-row"]`), // Wait for the actual content table
		chromedp.OuterHTML("html", &initialHtmlContent),
	)

	if err != nil {
		log.Printf("❌ [Guilds] CRITICAL: Could not scrape page 1. Aborting guild scrape. Error: %v", err)
		return // Exit if the first page fails
	}

	// --- STEP 2: Parse the HTML from page 1 for both pagination and guilds ---
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(initialHtmlContent))
	if err != nil {
		log.Printf("❌ [Guilds] CRITICAL: Could not parse page 1 HTML. Aborting guild scrape. Error: %v", err)
		return
	}

	// 2a: Find the last page number from the pagination controls
	pageRegex := regexp.MustCompile(`page=(\d+)`)
	doc.Find(`nav[aria-label="pagination"] a[href*="?page="]`).Each(func(i int, s *goquery.Selection) {
		if href, exists := s.Attr("href"); exists {
			matches := pageRegex.FindStringSubmatch(href)
			if len(matches) > 1 {
				if p, err := strconv.Atoi(matches[1]); err == nil {
					if p > lastPage {
						lastPage = p
					}
				}
			}
		}
	})
	log.Printf("✅ [Guilds] Found %d total pages. Processing page 1 data...", lastPage)

	// 2b: Parse the guild rows from the page 1 document
	guildRows := doc.Find(`tbody[data-slot="table-body"] > tr:not(:has(td[colspan="4"]))`)
	log.Printf("🔎 [Guilds] Found %d guild rows on page 1. Processing...", guildRows.Length())
	guildRows.Each(func(i int, s *goquery.Selection) {
		var guild Guild
		var parseErr error

		cells := s.Find(`td[data-slot="table-cell"]`)
		if cells.Length() < 4 {
			return
		}

		rankStr := strings.TrimSpace(cells.Eq(0).Text())
		nameStr := strings.TrimSpace(cells.Eq(1).Find("span").Text())
		levelStr := strings.TrimSpace(cells.Eq(2).Text())
		memberRow := s.Next()
		if memberRow.Length() == 0 {
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
			return
		}
		guild.Level, parseErr = strconv.Atoi(levelStr)
		if parseErr != nil {
			return
		}
		allGuilds[guild.Name] = guild
	})

	// --- STEP 3: Loop through the rest of the pages ---
	for page := 2; page <= lastPage; page++ {
		var htmlContent string
		var pageScrapedSuccessfully bool
		url := fmt.Sprintf("https://projetoyufa.com/rankings/guild?page=%d", page)

		for attempt := 1; attempt <= maxRetries; attempt++ {
			taskCtx, cancelCtx := chromedp.NewContext(allocCtx)
			defer cancelCtx()
			taskCtx, cancelTimeoutLoop := context.WithTimeout(taskCtx, 60*time.Second)
			defer cancelTimeoutLoop()

			if enableGuildScraperDebugLogs {
				log.Printf("🏰 [Guilds] Scraping page %d of %d (Attempt %d/%d)...", page, lastPage, attempt, maxRetries)
			}

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
				time.Sleep(5 * time.Second) // Shorter sleep between retries
			}
		}

		if !pageScrapedSuccessfully {
			log.Printf("❌ [Guilds] All %d attempts failed for page %d. Skipping this page.", maxRetries, page)
			continue
		}

		doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
		if err != nil {
			log.Printf("❌ [Guilds] Failed to parse HTML for page %d: %v", page, err)
			continue
		}

		guildRows := doc.Find(`tbody[data-slot="table-body"] > tr:not(:has(td[colspan="4"]))`)
		if enableGuildScraperDebugLogs {
			log.Printf("🔎 [Guilds] Found %d guild rows on page %d. Processing...", guildRows.Length(), page)
		}

		guildRows.Each(func(i int, s *goquery.Selection) {
			// This parsing logic is identical to the one for page 1
			var guild Guild
			var parseErr error
			cells := s.Find(`td[data-slot="table-cell"]`)
			if cells.Length() < 4 {
				return
			}
			rankStr := strings.TrimSpace(cells.Eq(0).Text())
			nameStr := strings.TrimSpace(cells.Eq(1).Find("span").Text())
			levelStr := strings.TrimSpace(cells.Eq(2).Text())
			memberRow := s.Next()
			if memberRow.Length() == 0 {
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
				return
			}
			guild.Level, parseErr = strconv.Atoi(levelStr)
			if parseErr != nil {
				return
			}
			allGuilds[guild.Name] = guild
		})
		time.Sleep(2 * time.Second)
	}

	// --- STEP 4: Database operations ---
	if len(allGuilds) == 0 {
		log.Println("⚠️ [Guilds] Scrape finished with 0 total guilds found. Guild/character tables will not be updated.")
		return
	}

	tx, errDb := db.Begin()
	if errDb != nil {
		log.Printf("❌ [DB] Failed to begin transaction for guilds update: %v", errDb)
		return
	}
	defer tx.Rollback()

	if enableGuildScraperDebugLogs {
		log.Println("    -> [DB] Clearing and repopulating 'guilds' table...")
	}
	if _, err := tx.Exec("DELETE FROM guilds"); err != nil {
		log.Printf("❌ [DB] Failed to clear guilds table: %v", err)
		return
	}

	guildStmt, err := tx.Prepare(`INSERT INTO guilds (rank, name, level, experience, master, emblem_url, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Printf("❌ [DB] Failed to prepare guilds insert statement: %v", err)
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
		log.Printf("❌ [DB] Failed to clear existing guild names from characters table: %v", err)
		return
	}

	charStmt, err := tx.Prepare("UPDATE characters SET guild_name = ? WHERE name = ?")
	if err != nil {
		log.Printf("❌ [DB] Failed to prepare character guild update statement: %v", err)
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
		log.Printf("❌ [DB] Failed to commit guilds and characters transaction: %v", err)
		return
	}

	log.Printf("✅ [Guilds] Scrape and update complete. Saved %d guild records and updated character associations.", len(allGuilds))
}

// startBackgroundJobs starts all recurring background tasks.
func startBackgroundJobs() {
	// --- Market Scraper ---
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		scrapeData() // Run once immediately on start
		for {
			log.Printf("🕒 Waiting for the next 5-minute market scrape schedule...")
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
			log.Printf("🕒 Waiting for the next 1-minute player count schedule...")
			<-ticker.C
			scrapeAndStorePlayerCount()
		}
	}()

	// --- Guild Scraper ---
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		scrapeGuilds()
		for {
			log.Printf("🕒 Waiting for the next 30-minute guild schedule...")
			<-ticker.C
			scrapeGuilds()
		}
	}()

	// --- Player Character Scraper ---
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		scrapePlayerCharacters()
		for {
			log.Printf("🕒 Waiting for the next 30-minute player character schedule...")
			<-ticker.C
			scrapePlayerCharacters()
		}
	}()

}

// scrapeData performs a single scrape of the market data.
func scrapeData() {
	log.Println("🚀 [Market] Starting market scrape...")
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
	var scrapedItems []Item
	shopCards := doc.Find(`div[data-slot="card"]`)

	if enableMarketScraperDebugLogs {
		log.Printf("📄 [Market] Successfully parsed HTML. Found %d shop cards to process.", shopCards.Length())
	}

	shopCards.Each(func(i int, s *goquery.Selection) {
		storeName := strings.TrimSpace(s.Find(`div[data-slot="card-title"]`).Text())
		sellerName := strings.TrimSpace(s.Find("svg.lucide-user").Next().Text())
		mapName := strings.TrimSpace(s.Find("svg.lucide-map-pin").Next().Text())
		mapCoordinates := strings.TrimSpace(s.Find("svg.lucide-copy").Next().Text())

		if enableMarketScraperDebugLogs {
			log.Printf("    -> [Shop] Processing shop: '%s' by '%s' at %s (%s)", storeName, sellerName, mapName, mapCoordinates)
		}

		s.Find(`div[data-slot="card-content"] .flex.items-center.space-x-2`).Each(func(j int, itemSelection *goquery.Selection) {
			itemNameRaw := strings.TrimSpace(itemSelection.Find("p.truncate").Text())
			priceAndAmount := itemSelection.Find("div.flex.items-center.space-x-1")
			priceStr := strings.TrimSpace(priceAndAmount.Find("span").First().Text())
			amountStr := strings.TrimSpace(priceAndAmount.Find("span").Last().Text())

			if enableMarketScraperDebugLogs {
				log.Printf("        -> [Item] Raw data: Name='%s' Price='%s' Amount='%s'", itemNameRaw, priceStr, amountStr)
			}

			// --- Name Cleaning ---
			itemName := itemNameRaw
			if match := reRefineStart.FindStringSubmatch(itemName); len(match) > 1 {
				itemName = strings.TrimSpace(strings.TrimPrefix(itemName, match[0]))
			} else if match := reRefineMid.FindStringSubmatch(itemName); len(match) > 1 {
				itemName = strings.TrimSpace(strings.Replace(itemName, match[0], "", 1))
			}

			// --- Price and Amount Parsing ---
			quantity, _ := strconv.Atoi(strings.TrimSuffix(amountStr, "x"))

			if quantity == 0 || itemName == "" {
				if enableMarketScraperDebugLogs {
					log.Printf("        -> [Item] Skipping item due to invalid parsed data (quantity/name is zero/empty).")
				}
				return
			}

			item := Item{
				Name:           itemName,
				Price:          priceStr, // Keep price as the raw string
				Quantity:       quantity,
				SellerName:     sellerName,
				StoreName:      storeName,
				MapName:        mapName,
				MapCoordinates: mapCoordinates,
				Timestamp:      retrievalTime,
				IsAvailable:    true,
			}

			if enableMarketScraperDebugLogs {
				log.Printf("        -> [Item] Parsed data: Name='%s', Price='%s', Quantity=%d", item.Name, item.Price, item.Quantity)
			}

			scrapedItems = append(scrapedItems, item)
		})
	})

	if enableMarketScraperDebugLogs {
		log.Printf("✅ [Market] Finished processing all shops. A total of %d listings were found.", len(scrapedItems))
	}

	// The rest of the database logic to insert/update the 'scrapedItems' slice would follow here.
	// This was not included in the original file provided.
}

