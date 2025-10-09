package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
const enableCharacterScraperDebugLogs = false
const enableGuildScraperDebugLogs = true
const enableMvpScraperDebugLogs = true // Added constant
const enableZenyScraperDebugLogs = false
const enableMarketScraperDebugLogs = false

// Added a shared slice of MVP IDs for the scraper and database logic to use.
var mvpMobIDs = []string{
	"1038", "1039", "1046", "1059", "1086", "1087", "1112", "1115", "1147",
	"1150", "1157", "1159", "1190", "1251", "1252", "1272", "1312", "1373",
	"1389", "1418", "1492", "1511",
}

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

// **MODIFIED FUNCTION**
// scrapePlayerCharacters uses multiple individual regexes to find data points and assembles
// characters based on the order the data was found in the raw server response.
func scrapePlayerCharacters() {
	log.Println("🏆 [Characters] Starting player character scrape...")
	const maxRetries = 3
	const retryDelay = 3 * time.Second

	// Define individual regexes for each piece of character data.
	rankRegex := regexp.MustCompile(`p\-1 text\-center font\-medium\\",\\"children\\":(\d+)\}\]`)
	nameRegex := regexp.MustCompile(`max-w-10 truncate p-1 font-semibold">([^<]+)</td>`)
	baseLevelRegex := regexp.MustCompile(`\\"level\\":(\d+),`)
	jobLevelRegex := regexp.MustCompile(`\\"job_level\\":(\d+),\\"exp`)
	expRegex := regexp.MustCompile(`\\"exp\\":(\d+)`)
	classRegex := regexp.MustCompile(`"hidden text\-sm sm:inline\\",\\"children\\":\\"([^"]+)\\"`)

	client := &http.Client{Timeout: 45 * time.Second}

	// --- Determine total number of pages using regex ---
	var lastPage = 1
	log.Println("🏆 [Characters] Determining total number of pages...")
	firstPageURL := "https://projetoyufa.com/rankings?page=1"

	req, err := http.NewRequest("GET", firstPageURL, nil)
	if err != nil {
		log.Printf("❌ [Characters] Failed to create request for page 1: %v", err)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("⚠️ [Characters] Could not fetch page 1 to determine page count. Assuming one page. Error: %v", err)
	} else {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		// ✅ FIX: Close the body immediately after reading, before checking for errors.
		resp.Body.Close()
		if readErr != nil {
			log.Printf("⚠️ [Characters] Failed to read page 1 body. Assuming one page. Error: %v", readErr)
		} else {
			pageRegex := regexp.MustCompile(`page=(\d+)`)
			matches := pageRegex.FindAllStringSubmatch(string(bodyBytes), -1)
			for _, match := range matches {
				if len(match) > 1 {
					if p, pErr := strconv.Atoi(match[1]); pErr == nil {
						if p > lastPage {
							lastPage = p
						}
					}
				}
			}
		}
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

	updateTime := time.Now().Format(time.RFC3339)
	allPlayers := make(map[string]PlayerCharacter)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	log.Printf("🏆 [Characters] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(p int) {
			defer wg.Done()
			defer func() { <-sem }()

			var bodyContent string
			pageScrapedSuccessfully := false

			for attempt := 1; attempt <= maxRetries; attempt++ {
				url := fmt.Sprintf("https://projetoyufa.com/rankings?page=%d", p)
				req, reqErr := http.NewRequest("GET", url, nil)
				if reqErr != nil {
					log.Printf("    -> ❌ Error creating request for page %d: %v", p, reqErr)
					time.Sleep(retryDelay)
					continue
				}
				req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

				resp, doErr := client.Do(req)
				if doErr != nil {
					log.Printf("    -> ❌ Error on page %d (attempt %d/%d): %v", p, attempt, maxRetries, doErr)
					time.Sleep(retryDelay)
					continue
				}

				if resp.StatusCode != http.StatusOK {
					log.Printf("    -> ❌ Non-200 status code for page %d (attempt %d/%d): %d", p, attempt, maxRetries, resp.StatusCode)
					resp.Body.Close()
					time.Sleep(retryDelay)
					continue
				}

				bodyBytes, readErr := io.ReadAll(resp.Body)
				// ✅ FIX: Close the body immediately after reading, before checking for errors.
				resp.Body.Close()
				if readErr != nil {
					log.Printf("    -> ❌ Failed to read body for page %d: %v", p, readErr)
					time.Sleep(retryDelay)
					continue
				}

				bodyContent = string(bodyBytes)
				pageScrapedSuccessfully = true
				break
			}

			if !pageScrapedSuccessfully {
				log.Printf("    -> ❌ All retries failed for page %d.", p)
				return
			}

			// Find all matches for each attribute individually.
			rankMatches := rankRegex.FindAllStringSubmatch(bodyContent, -1)
			nameMatches := nameRegex.FindAllStringSubmatch(bodyContent, -1)
			baseLevelMatches := baseLevelRegex.FindAllStringSubmatch(bodyContent, -1)
			jobLevelMatches := jobLevelRegex.FindAllStringSubmatch(bodyContent, -1)
			expMatches := expRegex.FindAllStringSubmatch(bodyContent, -1)
			classMatches := classRegex.FindAllStringSubmatch(bodyContent, -1)

			if enableCharacterScraperDebugLogs {
				log.Printf("    -> matches: rank: %s, name: %s, base: %s, job %s, exp %s, class %s", rankMatches, nameMatches, baseLevelMatches, jobLevelMatches, expMatches, classMatches)
				log.Printf("    -> matches: rank: %d, name: %d, base: %d, job %d, exp %d, class %d", len(rankMatches), len(nameMatches), len(baseLevelMatches), len(jobLevelMatches), len(expMatches), len(classMatches))
			}

			// Validate that we found the same number of matches for each attribute.
			numChars := len(nameMatches)
			if enableCharacterScraperDebugLogs {
				log.Printf("    -> chars: %d", numChars)
			}

			if numChars == 0 || len(rankMatches) != numChars || len(baseLevelMatches) != numChars || len(jobLevelMatches) != numChars || len(expMatches) != numChars || len(classMatches) != numChars {
				log.Printf("    -> matches: rank: %s, name: %s, base: %s, job %s, exp %s, class %s", rankMatches, nameMatches, baseLevelMatches, jobLevelMatches, expMatches, classMatches)
				log.Printf("    -> matches: rank: %d, name: %d, base: %d, job %d, exp %d, class %d", len(rankMatches), len(nameMatches), len(baseLevelMatches), len(jobLevelMatches), len(expMatches), len(classMatches))
				log.Printf("    -> ⚠️ [Characters] Mismatch in regex match counts on page %d. Skipping page. (Ranks: %d, Names: %d, Classes: %d)", p, len(rankMatches), len(nameMatches), len(classMatches))
				return
			}

			var pagePlayers []PlayerCharacter
			for i := 0; i < numChars; i++ {
				rank, _ := strconv.Atoi(rankMatches[i][1])
				name := nameMatches[i][1]
				baseLevel, _ := strconv.Atoi(baseLevelMatches[i][1])
				jobLevel, _ := strconv.Atoi(jobLevelMatches[i][1])
				rawExp, _ := strconv.ParseFloat(expMatches[i][1], 64)
				class := classMatches[i][1]

				player := PlayerCharacter{
					Rank:       rank,
					Name:       name,
					BaseLevel:  baseLevel,
					JobLevel:   jobLevel,
					Experience: rawExp / 1000000.0, // Calculate percentage
					Class:      class,
				}

				if enableCharacterScraperDebugLogs {
					log.Printf("    -> char rank: %d, name: %s, level: %d/%d, exp: %.2f%%, class: %s", player.Rank, player.Name, player.BaseLevel, player.JobLevel, player.Experience, player.Class)
				}
				pagePlayers = append(pagePlayers, player)
			}

			if len(pagePlayers) > 0 {
				mu.Lock()
				for _, player := range pagePlayers {
					allPlayers[player.Name] = player
				}
				mu.Unlock()
			}
			log.Printf("    -> Scraped page %d/%d, found %d chars.", p, lastPage, len(pagePlayers))
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
	defer tx.Rollback() // Rollback is a no-op if Commit succeeds

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
		return
	}
	defer stmt.Close()

	for _, p := range allPlayers {
		lastActiveTime := updateTime
		if oldPlayer, exists := existingPlayers[p.Name]; exists {
			// Compare floats with a small tolerance to avoid issues with precision.
			if (p.Experience-oldPlayer.Experience) < 0.001 && (p.Experience-oldPlayer.Experience) > -0.001 {
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

// **MODIFIED FUNCTION**
func scrapeGuilds() {
	log.Println("🏰 [Guilds] Starting guild and character-guild association scrape...")
	const maxRetries = 5
	const retryDelay = 5 * time.Second
	client := &http.Client{Timeout: 60 * time.Second}

	// Define individual regexes for each piece of guild data.
	// These regexes assume a certain structure in the HTML source.
	nameRegex := regexp.MustCompile(`<span class="font-medium">([^<]+)</span>`)
	levelRegex := regexp.MustCompile(`\\"guild_lv\\":(\d+),\\"connect_member\\"`)
	masterRegex := regexp.MustCompile(`\\"master\\":\\"([^"]+)\\",\\"members\\"`)
	membersRegex := regexp.MustCompile(`\\"members\\":\[(.*?)\]\}`)
	memberNameRegex := regexp.MustCompile(`\\"name\\":\\"([^"]+)\\",\\"base_level\\"`)

	// --- Determine total number of pages ---
	var lastPage = 1
	log.Println("🏰 [Guilds] Determining total number of pages...")
	firstPageURL := "https://projetoyufa.com/rankings/guild?page=1"
	req, err := http.NewRequest("GET", firstPageURL, nil)
	if err != nil {
		log.Printf("❌ [Guilds] Failed to create request for page 1: %v", err)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("⚠️ [Guilds] Could not fetch page 1 to determine page count. Assuming one page. Error: %v", err)
	} else {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		// ✅ FIX: Close the body immediately after reading, before checking for errors.
		resp.Body.Close()
		if readErr != nil {
			log.Printf("⚠️ [Guilds] Failed to read page 1 body. Assuming one page. Error: %v", readErr)
		} else {
			pageRegex := regexp.MustCompile(`page=(\d+)`)
			matches := pageRegex.FindAllStringSubmatch(string(bodyBytes), -1)
			for _, match := range matches {
				if len(match) > 1 {
					if p, pErr := strconv.Atoi(match[1]); pErr == nil {
						if p > lastPage {
							lastPage = p
						}
					}
				}
			}
		}
	}
	log.Printf("✅ [Guilds] Found %d total pages to scrape.", lastPage)

	allGuilds := make(map[string]Guild)
	allMembers := make(map[string]string) // Map character name to guild name
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Limit to 5 concurrent scrapers

	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(p int) {
			defer wg.Done()
			defer func() { <-sem }()

			var bodyContent string
			pageScrapedSuccessfully := false

			for attempt := 1; attempt <= maxRetries; attempt++ {
				url := fmt.Sprintf("https://projetoyufa.com/rankings/guild?page=%d", p)
				req, reqErr := http.NewRequest("GET", url, nil)
				if reqErr != nil {
					log.Printf("    -> ❌ Error creating request for page %d: %v", p, reqErr)
					time.Sleep(retryDelay)
					continue
				}
				req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

				resp, doErr := client.Do(req)
				if doErr != nil {
					log.Printf("    -> ❌ Error on page %d (attempt %d/%d): %v", p, attempt, maxRetries, doErr)
					time.Sleep(retryDelay)
					continue
				}
				if resp.StatusCode != http.StatusOK {
					log.Printf("    -> ❌ Non-200 status on page %d (attempt %d/%d): %d", p, attempt, maxRetries, resp.StatusCode)
					resp.Body.Close()
					time.Sleep(retryDelay)
					continue
				}
				bodyBytes, readErr := io.ReadAll(resp.Body)
				// ✅ FIX: Close the body immediately after reading, before checking for errors.
				resp.Body.Close()
				if readErr != nil {
					log.Printf("    -> ❌ Failed to read body for page %d: %v", p, readErr)
					time.Sleep(retryDelay)
					continue
				}
				bodyContent = string(bodyBytes)
				pageScrapedSuccessfully = true
				break
			}

			if !pageScrapedSuccessfully {
				log.Printf("    -> ❌ All retries failed for page %d. Skipping.", p)
				return
			}

			// Find all matches for each attribute individually.
			nameMatches := nameRegex.FindAllStringSubmatch(bodyContent, -1)
			levelMatches := levelRegex.FindAllStringSubmatch(bodyContent, -1)
			masterMatches := masterRegex.FindAllStringSubmatch(bodyContent, -1)
			membersMatches := membersRegex.FindAllStringSubmatch(bodyContent, -1)
			log.Printf("    -> ⚠️ [Guilds] membersMatches %d", len(membersMatches))
			//log.Printf("    -> ⚠️ [Guilds] membersMatches text %s", membersMatches)

			numGuilds := len(nameMatches)
			if numGuilds == 0 || len(levelMatches) != numGuilds || len(masterMatches) != numGuilds {
				log.Printf("    -> ⚠️ [Guilds] Mismatch in regex match counts on page %d. Skipping page. (Names: %d, Levels: %d, Masters: %d)",
					p, len(nameMatches), len(levelMatches), len(masterMatches))
				return
			}

			var pageGuilds []Guild
			var pageMembers = make(map[string]string)
			for i := 0; i < numGuilds; i++ {
				name := nameMatches[i][1]
				level, _ := strconv.Atoi(levelMatches[i][1])
				master := masterMatches[i][1]

				guild := Guild{
					Name:       name,
					Level:      level,
					Master:     master,
					EmblemURL:  "",
					Experience: 0,
				}
				pageGuilds = append(pageGuilds, guild)
				// This strategy can only reliably associate the master.
				if master != "" {
					pageMembers[master] = name
				}

				members := memberNameRegex.FindAllStringSubmatch(membersMatches[i][1], -1)
				for _, member := range members {
					pageMembers[member[1]] = name
				}
			}

			if len(pageGuilds) > 0 {
				mu.Lock()
				for _, guild := range pageGuilds {
					allGuilds[guild.Name] = guild
				}
				for memberName, guildName := range pageMembers {
					allMembers[memberName] = guildName
				}
				mu.Unlock()
			}
			log.Printf("    -> Scraped page %d/%d, found %d guilds.", p, lastPage, len(pageGuilds))
		}(page)
	}
	wg.Wait()

	log.Printf("✅ [Guilds] Finished scraping all pages. Found %d unique guilds.", len(allGuilds))

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
		if _, err := guildStmt.Exec(0, g.Name, g.Level, g.Experience, g.Master, g.EmblemURL, updateTime); err != nil {
			log.Printf("    -> [DB] WARN: Failed to insert guild '%s': %v", g.Name, err)
		}
	}

	log.Printf("    -> [DB] Updating 'characters' table with guild associations for %d masters...", len(allMembers))

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
	log.Printf("    -> [DB] Successfully associated %d characters (masters) with their guilds.", updateCount)

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
	const retryDelay = 3 * time.Second
	client := &http.Client{Timeout: 45 * time.Second}

	// --- PRE-FETCH EXISTING DATA ---
	log.Println("    -> [DB] Fetching existing character zeny data for activity comparison...")
	existingCharacters := make(map[string]CharacterZenyInfo)
	rows, err := db.Query("SELECT name, zeny, last_active FROM characters")
	if err != nil {
		log.Printf("❌ [Zeny][DB] Failed to query existing characters for comparison: %v", err)
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

	// --- DETERMINE TOTAL PAGES ---
	var lastPage = 1
	log.Println("💰 [Zeny] Determining total number of pages...")
	firstPageURL := "https://projetoyufa.com/rankings/zeny?page=1"
	req, err := http.NewRequest("GET", firstPageURL, nil)
	if err != nil {
		log.Printf("❌ [Zeny] Failed to create request for page 1: %v", err)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("⚠️ [Zeny] Could not fetch page 1 to determine page count. Assuming one page. Error: %v", err)
	} else {
		// defer is safe here as it's not inside a retry loop.
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			doc, docErr := goquery.NewDocumentFromReader(resp.Body)
			if docErr != nil {
				log.Printf("⚠️ [Zeny] Failed to parse page 1 body. Assuming one page. Error: %v", docErr)
			} else {
				pageRegex := regexp.MustCompile(`page=(\d+)`)
				doc.Find(`nav[aria-label="pagination"] a[href*="?page="]`).Each(func(i int, s *goquery.Selection) {
					if href, exists := s.Attr("href"); exists {
						matches := pageRegex.FindStringSubmatch(href)
						if len(matches) > 1 {
							if p, pErr := strconv.Atoi(matches[1]); pErr == nil {
								if p > lastPage {
									lastPage = p
								}
							}
						}
					}
				})
			}
		}
	}
	log.Printf("✅ [Zeny] Found %d total pages to scrape.", lastPage)

	// --- SCRAPE ALL PAGES CONCURRENTLY ---
	updateTime := time.Now().Format(time.RFC3339)
	allZenyInfo := make(map[string]int64)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Limit to 5 concurrent scrapers

	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(p int) {
			defer wg.Done()
			defer func() { <-sem }()

			var doc *goquery.Document
			pageScrapedSuccessfully := false

			for attempt := 1; attempt <= maxRetries; attempt++ {
				url := fmt.Sprintf("https://projetoyufa.com/rankings/zeny?page=%d", p)
				req, reqErr := http.NewRequest("GET", url, nil)
				if reqErr != nil {
					log.Printf("    -> ❌ Error creating request for page %d: %v", p, reqErr)
					time.Sleep(retryDelay)
					continue
				}
				req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

				resp, doErr := client.Do(req)
				if doErr != nil {
					log.Printf("    -> ❌ Error on page %d (attempt %d/%d): %v", p, attempt, maxRetries, doErr)
					time.Sleep(retryDelay)
					continue
				}

				if resp.StatusCode != http.StatusOK {
					log.Printf("    -> ❌ Non-200 status on page %d (attempt %d/%d): %d", p, attempt, maxRetries, resp.StatusCode)
					resp.Body.Close()
					time.Sleep(retryDelay)
					continue
				}

				var parseErr error
				doc, parseErr = goquery.NewDocumentFromReader(resp.Body)
				// ✅ FIX: Close the body immediately after goquery has read it.
				resp.Body.Close()
				if parseErr != nil {
					log.Printf("    -> ❌ Failed to parse body for page %d: %v", p, parseErr)
					time.Sleep(retryDelay)
					continue
				}

				pageScrapedSuccessfully = true
				break
			}

			if !pageScrapedSuccessfully {
				log.Printf("    -> ❌ All retries failed for page %d.", p)
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
				zenyStrClean = strings.TrimSpace(zenyStrClean)

				zenyVal, err := strconv.ParseInt(zenyStrClean, 10, 64)

				if enableZenyScraperDebugLogs {
					log.Printf("    -> [Zeny] name: %s, zeny: %s.", nameStr, zenyStrClean)
				}

				if err != nil {
					log.Printf("    -> ⚠️ [Zeny] Could not parse zeny value '%s' for player '%s'", zenyStrRaw, nameStr)
					return
				}

				mu.Lock()
				allZenyInfo[nameStr] = zenyVal
				mu.Unlock()
			})
			log.Printf("    -> [Zeny] Scraped page %d/%d successfully.", p, lastPage)
		}(page)
	}

	wg.Wait()
	log.Printf("✅ [Zeny] Finished scraping all pages. Found zeny info for %d characters.", len(allZenyInfo))

	if len(allZenyInfo) == 0 {
		log.Println("⚠️ [Zeny] No zeny information was scraped. Skipping database update.")
		return
	}

	// --- UPDATE DATABASE ---
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
	const requestURL = "https://projetoyufa.com/market"

	// Create a new HTTP client with a timeout.
	client := &http.Client{Timeout: 45 * time.Second}

	var htmlContent string
	var scrapeSuccessful bool

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🚀 [Market] Scraping market page (Attempt %d/%d)...", attempt, maxRetries)

		req, err := http.NewRequest("GET", requestURL, nil)
		if err != nil {
			log.Printf("⚠️ [Market] Attempt %d/%d failed: could not create request: %v", attempt, maxRetries, err)
			time.Sleep(retryDelay) // Wait before the next attempt
			continue
		}

		// Set a User-Agent to mimic a real browser to avoid being blocked.
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("⚠️ [Market] Attempt %d/%d failed during request: %v", attempt, maxRetries, err)
			if attempt < maxRetries {
				log.Printf("    -> Retrying in %v...", retryDelay)
				time.Sleep(retryDelay)
			}
			continue
		}

		bodyBytes, readErr := io.ReadAll(resp.Body)
		// ✅ FIX: Close the body immediately after reading, before checking for errors.
		resp.Body.Close()

		if readErr != nil {
			log.Printf("⚠️ [Market] Attempt %d/%d failed reading response body: %v", attempt, maxRetries, readErr)
			if attempt < maxRetries {
				log.Printf("    -> Retrying in %v...", retryDelay)
				time.Sleep(retryDelay)
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("⚠️ [Market] Attempt %d/%d failed: received non-200 status code: %d", attempt, maxRetries, resp.StatusCode)
			if attempt < maxRetries {
				log.Printf("    -> Retrying in %v...", retryDelay)
				time.Sleep(retryDelay)
			}
			continue
		}

		htmlContent = string(bodyBytes)
		scrapeSuccessful = true
		break // Success, exit the loop.
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

		if enableMarketScraperDebugLogs == true {
			log.Printf("[Market] shop name: %s, seller name: %s, map_name: %s, mapcoord: %s", shopName, sellerName, mapName, mapCoordinates)
		}

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
			if enableMarketScraperDebugLogs == true {
				log.Printf("🔎 [Market] name: %s, id: %d, qtd: %d price %s store: %s seller: %s map: %s coord %s", itemName, itemID, quantity, priceStr, shopName, sellerName, mapName, mapCoordinates)
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

// scrapeMvpKills scrapes the MVP kill rankings using regex against the raw HTTP response.
func scrapeMvpKills() {
	log.Println("☠️  [MVP] Starting MVP kill count scrape...")
	const maxRetries = 3
	const retryDelay = 3 * time.Second

	// This regex targets the JSON-like data structure found in the page's source,
	// capturing the character name and the array of their MVP kills.
	//	playerBlockRegex := regexp.MustCompile(`{"id":\d+,"char_id":\d+,"name":"([^"]+)","class":\d+,"level":\d+,"job_level":\d+,"mvps":(\[.*?\])}`)
	playerBlockRegex := regexp.MustCompile(`{\\"rank\\":\d+,\\"total_kills\\":\d+,\\"char_id\\":\d+,\\"name\\":\\"([^"]+)\\",\\"base_level\\":\d+,\\"job_level\\":\d+,\\"class\\":\d+,\\"hair\\":\d+,\\"hair_color\\":\d+,\\"clothes_color\\":\d+,\\"head_top\\":\d+,\\"head_mid\\":\d+,\\"head_bottom\\":\d+,\\"robe\\":\d+,\\"weapon\\":\d+,\\"sex\\":\\"([^"]+)\\",\\"guild\\":{\\"guild_id\\":\d+,\\"name\\":\\"([^"])+\\"},\\"mvp_kills\\":(\[.*?\])}`)
	// This regex parses individual MVP entries within the captured array.
	mvpKillsRegex := regexp.MustCompile(`{\\"mob_id\\":(\d+),\\"kills\\":(\d+)}`)

	client := &http.Client{Timeout: 45 * time.Second}

	// --- Determine total number of pages ---
	var lastPage = 1
	log.Println("☠️  [MVP] Determining total number of pages...")
	firstPageURL := "https://projetoyufa.com/rankings/mvp?page=1"

	req, err := http.NewRequest("GET", firstPageURL, nil)
	if err != nil {
		log.Printf("❌ [MVP] Failed to create request for page 1: %v", err)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5.0 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("⚠️ [MVP] Could not fetch page 1 to determine page count. Assuming one page. Error: %v", err)
	} else {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Printf("⚠️ [MVP] Failed to read page 1 body. Assuming one page. Error: %v", readErr)
		} else {
			pageRegex := regexp.MustCompile(`page=(\d+)`)
			matches := pageRegex.FindAllStringSubmatch(string(bodyBytes), -1)
			for _, match := range matches {
				if len(match) > 1 {
					if p, pErr := strconv.Atoi(match[1]); pErr == nil {
						if p > lastPage {
							lastPage = p
						}
					}
				}
			}
		}
	}
	log.Printf("✅ [MVP] Found %d total pages to scrape.", lastPage)

	// map[characterName]map[mobID]killCount
	allMvpKills := make(map[string]map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	log.Printf("☠️  [MVP] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(p int) {
			defer wg.Done()
			defer func() { <-sem }()

			var bodyContent string
			pageScrapedSuccessfully := false

			for attempt := 1; attempt <= maxRetries; attempt++ {
				url := fmt.Sprintf("https://projetoyufa.com/rankings/mvp?page=%d", p)
				req, reqErr := http.NewRequest("GET", url, nil)
				if reqErr != nil {
					log.Printf("    -> ❌ [MVP] Error creating request for page %d: %v", p, reqErr)
					time.Sleep(retryDelay)
					continue
				}
				req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/5.0 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

				resp, doErr := client.Do(req)
				if doErr != nil {
					log.Printf("    -> ❌ [MVP] Error on page %d (attempt %d/%d): %v", p, attempt, maxRetries, doErr)
					time.Sleep(retryDelay)
					continue
				}

				bodyBytes, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if readErr != nil {
					log.Printf("    -> ❌ [MVP] Failed to read body for page %d: %v", p, readErr)
					time.Sleep(retryDelay)
					continue
				}

				bodyContent = string(bodyBytes)
				pageScrapedSuccessfully = true
				break
			}

			if !pageScrapedSuccessfully {
				log.Printf("    -> ❌ [MVP] All retries failed for page %d.", p)
				return
			}

			playerBlocks := playerBlockRegex.FindAllStringSubmatch(bodyContent, -1)
			if len(playerBlocks) == 0 {
				log.Printf("    -> ⚠️ [MVP] No player data blocks found on page %d.", p)
				return
			}

			if enableMvpScraperDebugLogs {
				log.Printf("    -> [MVP] playerBlocks: %s", playerBlocks)
			}

			pageKills := make(map[string]map[string]int)
			for _, block := range playerBlocks {
				charName := block[1]
				mvpsJSON := block[4]

				if enableMvpScraperDebugLogs {
					for i := 0; i < len(block); i++ {
						log.Printf("    -> [MVP] block[%d]: %s", i, block[i])
					}
				}

				if enableMvpScraperDebugLogs {
					log.Printf("    -> [MVP] Found player: %s", charName)
				}

				playerKills := make(map[string]int)
				killMatches := mvpKillsRegex.FindAllStringSubmatch(mvpsJSON, -1)

				if enableMvpScraperDebugLogs {
					log.Printf("    -> [MVP] killMatches: %s", killMatches)
				}

				for _, killMatch := range killMatches {
					mobID := killMatch[1]
					killCount, _ := strconv.Atoi(killMatch[2])
					playerKills[mobID] = killCount
				}
				pageKills[charName] = playerKills
			}

			if len(pageKills) > 0 {
				mu.Lock()
				for charName, kills := range pageKills {
					allMvpKills[charName] = kills
				}
				mu.Unlock()
			}
			log.Printf("    -> [MVP] Scraped page %d/%d, found %d characters with MVP kills.", p, lastPage, len(pageKills))
		}(page)
	}

	wg.Wait()
	log.Printf("✅ [MVP] Finished scraping all pages. Found %d unique characters with MVP kills.", len(allMvpKills))

	if len(allMvpKills) == 0 {
		log.Println("⚠️ [MVP] No MVP kills found after scrape. Skipping database update.")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [MVP][DB] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// Prepare the dynamic UPSERT statement
	columnNames := []string{"character_name"}
	valuePlaceholders := []string{"?"}
	updateSetters := []string{}
	for _, mobID := range mvpMobIDs {
		colName := fmt.Sprintf("mvp_%s", mobID)
		columnNames = append(columnNames, colName)
		valuePlaceholders = append(valuePlaceholders, "?")
		updateSetters = append(updateSetters, fmt.Sprintf("%s=excluded.%s", colName, colName))
	}

	sql := fmt.Sprintf(`
		INSERT INTO character_mvp_kills (%s)
		VALUES (%s)
		ON CONFLICT(character_name) DO UPDATE SET %s
	`, strings.Join(columnNames, ", "), strings.Join(valuePlaceholders, ", "), strings.Join(updateSetters, ", "))

	stmt, err := tx.Prepare(sql)
	if err != nil {
		log.Printf("❌ [MVP][DB] Failed to prepare MVP kills upsert statement: %v", err)
		return
	}
	defer stmt.Close()

	for charName, kills := range allMvpKills {
		// Ensure the character exists in the main 'characters' table first to satisfy the FOREIGN KEY constraint.
		var exists int
		err := tx.QueryRow("SELECT COUNT(*) FROM characters WHERE name = ?", charName).Scan(&exists)
		if err != nil {
			log.Printf("    -> [MVP][DB] WARN: Could not check for existence of character '%s': %v. Skipping.", charName, err)
			continue
		}
		if exists == 0 {
			if enableMvpScraperDebugLogs {
				log.Printf("    -> [MVP][DB] Character '%s' not found in main table. Skipping MVP data insert.", charName)
			}
			continue
		}

		params := []interface{}{charName}
		for _, mobID := range mvpMobIDs {
			if count, ok := kills[mobID]; ok {
				params = append(params, count)
			} else {
				params = append(params, 0) // Default to 0 if not found in scraped data
			}
		}

		if _, err := stmt.Exec(params...); err != nil {
			log.Printf("    -> [MVP][DB] WARN: Failed to upsert MVP kills for player %s: %v", charName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ [MVP][DB] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("✅ [MVP] Saved/updated MVP kill records for %d characters.", len(allMvpKills))
	log.Printf("✅ [MVP] Scrape and update process complete.")
}

func startBackgroundJobs() {
	go func() {
		ticker := time.NewTicker(3 * time.Minute)
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
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 30-minute player character schedule...")
			<-ticker.C
			scrapePlayerCharacters()
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 60-minute guild schedule...")
			<-ticker.C
			scrapeGuilds()
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 6-hour Zeny ranking schedule...")
			<-ticker.C
			scrapeZeny()
		}
	}()

	go func() {
		scrapeMvpKills()
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 60-minute MVP kill count schedule...")
			<-ticker.C
			scrapeMvpKills()
		}
	}()
}
