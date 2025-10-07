package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http" // Added import
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
const enablePlayerCountDebugLogs = false
const enableCharacterScraperDebugLogs = true
const enableGuildScraperDebugLogs = false
const enableZenyScraperDebugLogs = true
const enableMarketScraperDebugLogs = true

// newOptimizedAllocator creates a new chromedp allocator context with optimized flags
// for scraping, disabling unnecessary resources like images to improve performance.
func newOptimizedAllocator() (context.Context, context.CancelFunc) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.UserAgent(`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36`),
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("blink-settings", "imagesEnabled=false"),
	)
	return chromedp.NewExecAllocator(context.Background(), opts...)
}

// scrapeAndStorePlayerCount uses net/http and goquery for a lightweight way to get the player count.
func scrapeAndStorePlayerCount() {
	log.Println("📊 [Counter] Checking player and seller count...")

	// Create a new HTTP client with a timeout.
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET", "https://projetoyufa.com/info", nil)
	if err != nil {
		log.Printf("❌ [Counter] Failed to create HTTP request: %v", err)
		return
	}

	// Set a User-Agent to mimic a real browser.
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ [Counter] Failed to fetch player info page: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("❌ [Counter] Received non-200 status code from info page: %d", resp.StatusCode)
		return
	}

	// Parse the response body with goquery.
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Printf("❌ [Counter] Failed to parse player info page HTML: %v", err)
		return
	}

	// Find the element and extract its text.
	playerCountText := doc.Find(`span[data-slot="badge"] p`).Text()

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

	var sellerCount int
	err = db.QueryRow("SELECT COUNT(DISTINCT seller_name) FROM items WHERE is_available = 1").Scan(&sellerCount)
	if err != nil {
		log.Printf("⚠️ [Counter] Could not query for unique seller count: %v", err)
		// Don't return, as we can still store the player count. Default sellerCount to 0.
		sellerCount = 0
	}

	var lastPlayerCount int
	var lastSellerCount sql.NullInt64
	err = db.QueryRow("SELECT count, seller_count FROM player_history ORDER BY timestamp DESC LIMIT 1").Scan(&lastPlayerCount, &lastSellerCount)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("⚠️ [Counter] Could not query for last player/seller count: %v", err)
		return
	}

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

	allocCtx, cancel := newOptimizedAllocator()
	defer cancel()

	var lastPage = 1
	log.Println("🏆 [Characters] Determining total number of pages...")
	firstPageCtx, cancelFirstPage := chromedp.NewContext(allocCtx)
	defer cancelFirstPage()
	firstPageCtx, cancelTimeout := context.WithTimeout(firstPageCtx, 60*time.Second)
	defer cancelTimeout()

	var initialHtmlContent string
	err := chromedp.Run(firstPageCtx,
		chromedp.Navigate("https://projetoyufa.com/rankings?page=1"),
		chromedp.WaitVisible(`nav[aria-label="pagination"]`),
		chromedp.OuterHTML("html", &initialHtmlContent),
	)

	if err != nil {
		log.Printf("⚠️ [Characters] Could not find pagination on page 1. Assuming only one page. Error: %v", err)
	} else {
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(initialHtmlContent))
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
	}
	log.Printf("✅ [Characters] Found %d total pages to scrape.", lastPage)

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

	allPlayers := make(map[string]PlayerCharacter)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Limit to 5 concurrent scrapers

	log.Printf("🏆 [Characters] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(p int) {
			defer wg.Done()
			defer func() { <-sem }()

			var htmlContent string
			var pageScrapedSuccessfully bool

			for attempt := 1; attempt <= maxRetries; attempt++ {
				taskCtx, cancelCtx := chromedp.NewContext(allocCtx)
				defer cancelCtx()
				taskCtx, cancelTimeout := context.WithTimeout(taskCtx, 60*time.Second)
				defer cancelTimeout()

				url := fmt.Sprintf("https://projetoyufa.com/rankings?page=%d", p)
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
					log.Printf("    -> ❌ Error on page %d after %d attempts: %v", p, maxRetries, err)
				}
			}

			if !pageScrapedSuccessfully {
				return
			}

			doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
			if err != nil {
				log.Printf("    -> ❌ Failed to parse HTML for page %d: %v", p, err)
				return
			}

			var pagePlayers []PlayerCharacter
			doc.Find(`tbody[data-slot="table-body"] tr[data-slot="table-row"]`).Each(func(i int, s *goquery.Selection) {
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

			if len(pagePlayers) > 0 {
				mu.Lock()
				for _, player := range pagePlayers {
					allPlayers[player.Name] = player
				}
				mu.Unlock()
			}
			if enableCharacterScraperDebugLogs {
				log.Printf("    -> Scraped page %d, found %d players.", p, len(pagePlayers))
			}
		}(page)
	}

	wg.Wait()
	log.Printf("✅ [Characters] Finished scraping all pages. Found %d unique characters.", len(allPlayers))

	if len(allPlayers) == 0 {
		log.Println("⚠️ [Characters] No players found after scrape. Skipping database update.")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [DB] Failed to begin transaction: %v", err)
		return
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
		return
	}
	defer stmt.Close()

	for _, p := range allPlayers {
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
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ [DB] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("✅ [Characters] Saved/updated %d records.", len(allPlayers))

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

// Helper structs for parsing guild data
type GuildMemberJSON struct {
	Name string `json:"name"`
}

type GuildJSON struct {
	Name    string            `json:"name"`
	Level   int               `json:"guild_lv"`
	Master  string            `json:"master"`
	Members []GuildMemberJSON `json:"members"`
}

func scrapeGuilds() {
	log.Println("🏰 [Guilds] Starting guild and character-guild association scrape...")
	const maxRetries = 60

	allocOpts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-extensions", true),
	)
	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), allocOpts...)
	defer cancel()

	var lastPage = 1
	log.Println("🏰 [Guilds] Determining total number of pages...")

	firstPageCtx, cancelFirstPage := chromedp.NewContext(allocCtx)
	defer cancelFirstPage()
	firstPageCtx, cancelTimeout := context.WithTimeout(firstPageCtx, 60*time.Second)
	defer cancelTimeout()

	var initialHtmlContent string
	err := chromedp.Run(firstPageCtx,
		chromedp.Navigate("https://projetoyufa.com/rankings/guild?page=1"),
		chromedp.WaitVisible(`nav[aria-label="pagination"]`),
		chromedp.OuterHTML("html", &initialHtmlContent),
	)

	if err != nil {
		log.Printf("⚠️ [Guilds] Could not find pagination on page 1. Assuming only one page. Error: %v", err)
	} else {
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(initialHtmlContent))
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
		log.Printf("✅ [Guilds] Found %d total pages to scrape.", lastPage)
	}

	allGuilds := make(map[string]Guild)
	allMembers := make(map[string]string) // Map character name to guild name

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
			log.Printf("❌ [Guilds] All %d attempts failed for page %d. Aborting guild scrape to prevent partial data update.", maxRetries, page)
			return
		}

		doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
		if err != nil {
			log.Printf("❌ [Guilds] Failed to parse HTML for page %d. Aborting guild scrape to prevent partial data update. Error: %v", page, err)
			return
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

// Helper struct for comparing Zeny data
type CharacterZenyInfo struct {
	Zeny       sql.NullInt64
	LastActive string
}

func scrapeZeny() {
	log.Println("💰 [Zeny] Starting Zeny ranking scrape...")
	const maxRetries = 3

	// --- PRE-FETCH EXISTING DATA ---
	log.Println("    -> [DB] Fetching existing character zeny data for activity comparison...")
	existingCharacters := make(map[string]CharacterZenyInfo)
	rows, err := db.Query("SELECT name, zeny, last_active FROM characters")
	if err != nil {
		log.Printf("❌ [Zeny][DB] Failed to query existing characters for comparison: %v", err)
		// We can continue, but activity tracking may be inaccurate.
	} else {
		defer rows.Close()
		for rows.Next() {
			var name string
			var info CharacterZenyInfo
			if err := rows.Scan(&name, &info.Zeny, &info.LastActive); err != nil {
				log.Printf("    -> [DB] WARN: Failed to scan existing zeny row: %v", err)
				continue
			}
			existingCharacters[name] = info
		}
		if enableZenyScraperDebugLogs {
			log.Printf("    -> [DB] Found %d existing character records for comparison.", len(existingCharacters))
		}
	}
	// Use a single timestamp for the entire scrape operation.
	updateTime := time.Now().Format(time.RFC3339)

	allocCtx, cancel := newOptimizedAllocator()
	defer cancel()

	var lastPage = 1
	log.Println("💰 [Zeny] Determining total number of pages...")
	firstPageCtx, cancelFirstPage := chromedp.NewContext(allocCtx)
	defer cancelFirstPage()
	firstPageCtx, cancelTimeout := context.WithTimeout(firstPageCtx, 60*time.Second)
	defer cancelTimeout()

	var initialHtmlContent string
	err = chromedp.Run(firstPageCtx,
		chromedp.Navigate("https://projetoyufa.com/rankings/zeny?page=1"),
		chromedp.WaitVisible(`nav[aria-label="pagination"]`),
		chromedp.OuterHTML("html", &initialHtmlContent),
	)

	if err != nil {
		log.Printf("⚠️ [Zeny] Could not find pagination on page 1. Assuming only one page. Error: %v", err)
	} else {
		doc, _ := goquery.NewDocumentFromReader(strings.NewReader(initialHtmlContent))
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
	}
	log.Printf("✅ [Zeny] Found %d total pages to scrape.", lastPage)

	allZenyInfo := make(map[string]int64)
	var mu sync.Mutex // Mutex to protect allZenyInfo map

	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Limit to 5 concurrent scrapers

	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{} // Acquire a semaphore slot
		go func(p int) {
			defer wg.Done()
			defer func() { <-sem }() // Release the slot

			var htmlContent string
			var pageScrapedSuccessfully bool

			for attempt := 1; attempt <= maxRetries; attempt++ {
				taskCtx, cancelCtx := chromedp.NewContext(allocCtx)
				defer cancelCtx()
				taskCtx, cancelTimeoutLoop := context.WithTimeout(taskCtx, 60*time.Second)
				defer cancelTimeoutLoop()

				url := fmt.Sprintf("https://projetoyufa.com/rankings/zeny?page=%d", p)
				err := chromedp.Run(taskCtx,
					chromedp.Navigate(url),
					chromedp.WaitVisible(`tbody[data-slot="table-body"] tr[data-slot="table-row"]`),
					chromedp.OuterHTML("html", &htmlContent),
				)

				if err == nil {
					pageScrapedSuccessfully = true
					break
				}
				if attempt == maxRetries {
					log.Printf("    -> ❌ [Zeny] Error on page %d after %d attempts: %v", p, maxRetries, err)
				}
			}

			if !pageScrapedSuccessfully {
				return
			}

			doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
			if err != nil {
				log.Printf("    -> ❌ [Zeny] Failed to parse HTML for page %d: %v", p, err)
				return
			}

			doc.Find(`tbody[data-slot="table-body"] tr[data-slot="table-row"]`).Each(func(i int, s *goquery.Selection) {
				cells := s.Find(`td[data-slot="table-cell"]`)
				if cells.Length() < 3 {
					return
				}

				nameStr := strings.TrimSpace(cells.Eq(1).Text())
				zenyStrRaw := strings.TrimSpace(cells.Eq(2).Text())
				zenyStrClean := strings.ReplaceAll(zenyStrRaw, ",", "")
				zenyStrClean = strings.TrimSuffix(zenyStrClean, "z")
				zenyStrClean = strings.TrimSpace(zenyStrClean) // Trim space left between number and 'z'

				zenyVal, err := strconv.ParseInt(zenyStrClean, 10, 64)
				if err != nil {
					log.Printf("    -> ⚠️ [Zeny] Could not parse zeny value '%s' for player '%s'", zenyStrRaw, nameStr)
					return
				}

				mu.Lock()
				allZenyInfo[nameStr] = zenyVal
				mu.Unlock()
			})
			if enableZenyScraperDebugLogs {
				log.Printf("    -> [Zeny] Scraped page %d successfully.", p)
			}
		}(page)
	}

	wg.Wait()
	log.Printf("✅ [Zeny] Finished scraping all pages. Found zeny info for %d characters.", len(allZenyInfo))

	if len(allZenyInfo) == 0 {
		log.Println("⚠️ [Zeny] No zeny information was scraped. Skipping database update.")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [Zeny][DB] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("UPDATE characters SET zeny = ?, last_active = ? WHERE name = ?")
	if err != nil {
		log.Printf("❌ [Zeny][DB] Failed to prepare update statement: %v", err)
		return
	}
	defer stmt.Close()

	updatedCount := 0
	unchangedCount := 0
	for name, newZeny := range allZenyInfo {
		oldInfo, exists := existingCharacters[name]

		// Condition for update: character is new, their old zeny value was NULL, or the zeny value has changed.
		if !exists || !oldInfo.Zeny.Valid || oldInfo.Zeny.Int64 != newZeny {
			res, err := stmt.Exec(newZeny, updateTime, name)
			if err != nil {
				log.Printf("    -> ⚠️ [Zeny][DB] Failed to update zeny for '%s': %v", name, err)
				continue
			}
			rowsAffected, _ := res.RowsAffected()
			if rowsAffected > 0 {
				if enableZenyScraperDebugLogs {
					if !exists || !oldInfo.Zeny.Valid {
						log.Printf("    -> [Activity] Player '%s' zeny recorded for the first time: %d. Updating last_active.", name, newZeny)
					} else {
						log.Printf("    -> [Activity] Player '%s' zeny changed from %d to %d. Updating last_active.", name, oldInfo.Zeny.Int64, newZeny)
					}
				}
				updatedCount++
			}
		} else {
			// Zeny value is the same, no update needed.
			unchangedCount++
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ [Zeny][DB] Failed to commit transaction: %v", err)
		return
	}

	log.Printf("✅ [Zeny] Database update complete. Updated activity for %d characters. %d characters were unchanged.", updatedCount, unchangedCount)
}

func scrapeData() {
	log.Println("🚀 [Market] Starting scrape...")
	// Compile regexes once for efficiency.
	reRefineMid := regexp.MustCompile(`\s(\+\d+)`)
	reRefineStart := regexp.MustCompile(`^(\+\d+)\s`)

	const maxRetries = 3
	const retryDelay = 5 * time.Second

	allocCtx, cancel := newOptimizedAllocator()
	defer cancel()

	var htmlContent string
	var err error
	var scrapeSuccessful bool

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🚀 [Market] Scraping market page (Attempt %d/%d)...", attempt, maxRetries)
		taskCtx, cancel := chromedp.NewContext(allocCtx)
		defer cancel()

		taskCtxWithTimeout, cancelTimeout := context.WithTimeout(taskCtx, 45*time.Second)
		defer cancelTimeout()

		err = chromedp.Run(taskCtxWithTimeout,
			chromedp.Navigate("https://projetoyufa.com/market"),
			chromedp.WaitVisible(`div[data-slot="card-header"]`),
			chromedp.OuterHTML("html", &htmlContent),
		)

		if err == nil {
			scrapeSuccessful = true
			break
		}

		log.Printf("⚠️ [Market] Attempt %d/%d failed: %v", attempt, maxRetries, err)
		if attempt < maxRetries {
			log.Printf("    -> Retrying in %v...", retryDelay)
			time.Sleep(retryDelay)
		}
	}

	if !scrapeSuccessful {
		log.Printf("❌ [Market] Failed to scrape market page after %d attempts. Aborting update.", maxRetries)
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
					itemName = cleanedName + " " + match[1]
				}
			}

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
			var itemID int
			err := tx.QueryRow("SELECT item_id FROM items WHERE name_of_the_item = ? AND item_id > 0 LIMIT 1", name).Scan(&itemID)
			if err != nil {
				log.Printf("⚠️ [Market] Could not find item_id for removed item '%s', logging event with item_id 0: %v", name, err)
				itemID = 0
			}
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

func startBackgroundJobs() {
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

	go func() {
		ticker := time.NewTicker(2 * time.Hour)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 30-minute player character schedule...")
			<-ticker.C
			scrapePlayerCharacters()
		}
	}()

	go func() {
		scrapeGuilds()
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 60-minute guild schedule...")
			<-ticker.C
			scrapeGuilds()
		}
	}()

	go func() {
		ticker := time.NewTicker(3 * time.Hour)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 6-hour Zeny ranking schedule...")
			<-ticker.C
			scrapeZeny()
		}
	}()
}

