package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
)

const enablePlayerCountDebugLogs = false
const enableCharacterScraperDebugLogs = false
const enableGuildScraperDebugLogs = false
const enableMvpScraperDebugLogs = false
const enableZenyScraperDebugLogs = false
const enableMarketScraperDebugLogs = false

var (
	marketMutex      sync.Mutex
	characterMutex   sync.Mutex
	playerCountMutex sync.Mutex
)

const (
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
	defaultTimeout   = 45 * time.Second
	maxScrapeRetries = 3
	retryScrapeDelay = 3 * time.Second
)

// ScraperClient holds a shared HTTP client and user agent for all scrapers.
type ScraperClient struct {
	Client    *http.Client
	UserAgent string
}

// NewScraperClient creates a new client optimized for scraping.
func NewScraperClient() *ScraperClient {
	return &ScraperClient{
		Client:    &http.Client{Timeout: defaultTimeout},
		UserAgent: defaultUserAgent,
	}
}

var scraperClient = NewScraperClient()

// getPage performs a GET request with the shared client, user agent, and retry logic.
// It returns the response body as a string.
func (sc *ScraperClient) getPage(url, logPrefix string) (string, error) {
	var bodyContent string
	var err error

	for attempt := 1; attempt <= maxScrapeRetries; attempt++ {
		req, reqErr := http.NewRequest("GET", url, nil)
		if reqErr != nil {
			return "", fmt.Errorf("failed to create request: %w", reqErr)
		}
		req.Header.Set("User-Agent", sc.UserAgent)

		resp, doErr := sc.Client.Do(req)
		if doErr != nil {
			err = doErr
			log.Printf("    -> ❌ %s Error on page (attempt %d/%d): %v", logPrefix, attempt, maxScrapeRetries, doErr)
			time.Sleep(retryScrapeDelay)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("received non-200 status: %d", resp.StatusCode)
			log.Printf("    -> ❌ %s Non-200 status (attempt %d/%d): %d", logPrefix, attempt, maxScrapeRetries, resp.StatusCode)
			resp.Body.Close()
			time.Sleep(retryScrapeDelay)
			continue
		}

		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			err = readErr
			log.Printf("    -> ❌ %s Failed to read body (attempt %d/%d): %v", logPrefix, attempt, maxScrapeRetries, readErr)
			time.Sleep(retryScrapeDelay)
			continue
		}

		// Success
		bodyContent = string(bodyBytes)
		return bodyContent, nil
	}

	// All retries failed
	return "", fmt.Errorf("all retries failed for %s: %w", url, err)
}

// findLastPage determines the total number of pages for a paginated ranking.
func (sc *ScraperClient) findLastPage(firstPageURL, logPrefix string) int {
	log.Printf(" %s Determining total number of pages...", logPrefix)

	bodyContent, err := sc.getPage(firstPageURL, logPrefix)
	if err != nil {
		log.Printf("⚠️ %s Could not fetch page 1 to determine page count. Assuming 1 page. Error: %v", logPrefix, err)
		return 1
	}

	lastPage := 1
	// This regex is simple and works for all ranking pages on the target site
	pageRegex := regexp.MustCompile(`page=(\d+)`)
	matches := pageRegex.FindAllStringSubmatch(bodyContent, -1)

	for _, match := range matches {
		if len(match) > 1 {
			if p, pErr := strconv.Atoi(match[1]); pErr == nil {
				if p > lastPage {
					lastPage = p
				}
			}
		}
	}

	log.Printf("✅ %s Found %d total pages to scrape.", logPrefix, lastPage)
	return lastPage
}

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

func logCharacterActivity(changelogStmt *sql.Stmt, charName string, description string) error {
	if description == "" {
		return nil
	}

	_, err := changelogStmt.Exec(charName, time.Now().Format(time.RFC3339), description)
	if err != nil {
		log.Printf("    -> ❌ [Changelog] Failed to log activity for %s: %v", charName, err)
	}
	return err
}

func scrapeAndStorePlayerCount() {
	log.Println("📊 [Counter] Checking player and seller count...")
	const url = "https://projetoyufa.com/info"

	// Use the shared client's getPage method.
	// This encapsulates the user-agent, timeout, and status check.
	bodyContent, err := scraperClient.getPage(url, "[Counter]")
	if err != nil {
		log.Printf("❌ [Counter] Failed to fetch player info page: %v", err)
		return
	}

	// Parse the HTML content
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(bodyContent))
	if err != nil {
		log.Printf("❌ [Counter] Failed to parse player info page HTML: %v", err)
		return
	}

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

	playerCountMutex.Lock()
	defer playerCountMutex.Unlock()

	var sellerCount int
	err = db.QueryRow("SELECT COUNT(DISTINCT seller_name) FROM items WHERE is_available = 1").Scan(&sellerCount)
	if err != nil {
		log.Printf("⚠️ [Counter] Could not query for unique seller count: %v", err)

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

// checkAndLogCharacterActivity contains the logic for detecting and logging player changes.
func checkAndLogCharacterActivity(changelogStmt *sql.Stmt, p PlayerCharacter, oldPlayer PlayerCharacter) string {
	lastActiveTime := oldPlayer.LastActive // Assume inactive unless changed

	// --- Activity Logging Logic ---
	baseLeveledUp := false
	if p.BaseLevel > oldPlayer.BaseLevel {
		logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Leveled up to Base Level %d!", p.BaseLevel))
		baseLeveledUp = true
		lastActiveTime = p.LastUpdated // Active
	}
	if p.JobLevel > oldPlayer.JobLevel {
		logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Leveled up to Job Level %d!", p.JobLevel))
		lastActiveTime = p.LastUpdated // Active
	}

	expDelta := p.Experience - oldPlayer.Experience
	if !baseLeveledUp {
		if expDelta > 0.001 {
			logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Gained %.2f%% experience (now at %.2f%%).", expDelta, p.Experience))
			lastActiveTime = p.LastUpdated // Active
		} else if expDelta < -0.001 {
			logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Lost %.2f%% experience (now at %.2f%%).", -expDelta, p.Experience))
			lastActiveTime = p.LastUpdated // Active
		}
	} else if expDelta > 0.001 {
		// Log experience gain even on level up, but only if it's positive
		logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Gained %.2f%% experience (now at %.2f%%).", p.Experience, p.Experience))
	}

	if p.Class != oldPlayer.Class {
		logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Changed class from '%s' to '%s'.", oldPlayer.Class, p.Class))
		lastActiveTime = p.LastUpdated // Active
	}

	if lastActiveTime != p.LastUpdated && enableCharacterScraperDebugLogs {
		log.Printf("    -> [Activity] Player '%s' showed no change. last_active remains %s.", p.Name, lastActiveTime)
	}

	return lastActiveTime
}

// fetchExistingPlayers queries the DB for all player data needed for comparison.
func fetchExistingPlayers() (map[string]PlayerCharacter, error) {
	if enableCharacterScraperDebugLogs {
		log.Println("    -> [DB] Fetching existing player data for activity comparison...")
	}
	existingPlayers := make(map[string]PlayerCharacter)
	rowsPre, err := db.Query("SELECT name, base_level, job_level, experience, class, last_active FROM characters")
	if err != nil {
		return nil, fmt.Errorf("failed to query existing characters for comparison: %w", err)
	}
	defer rowsPre.Close()

	for rowsPre.Next() {
		var p PlayerCharacter
		if err := rowsPre.Scan(&p.Name, &p.BaseLevel, &p.JobLevel, &p.Experience, &p.Class, &p.LastActive); err != nil {
			log.Printf("    -> [DB] WARN: Failed to scan existing player row: %v", err)
			continue
		}
		existingPlayers[p.Name] = p
	}

	if enableCharacterScraperDebugLogs {
		log.Printf("    -> [DB] Found %d existing player records for comparison.", len(existingPlayers))
	}
	return existingPlayers, nil
}

// cleanupStalePlayers removes players from the DB who were not in the latest scrape.
func cleanupStalePlayers(scrapedPlayerNames map[string]bool, existingPlayers map[string]PlayerCharacter) {
	log.Println("🧹 [Characters] Cleaning up old player records not found in this scrape...")
	var stalePlayers []interface{}
	for existingName := range existingPlayers {
		if !scrapedPlayerNames[existingName] {
			stalePlayers = append(stalePlayers, existingName)
		}
	}

	if len(stalePlayers) == 0 {
		log.Println("✅ [Characters] Cleanup complete. No stale records found.")
		return
	}

	// Delete in batches to avoid "too many SQL variables" error
	const batchSize = 100
	for i := 0; i < len(stalePlayers); i += batchSize {
		end := i + batchSize
		if end > len(stalePlayers) {
			end = len(stalePlayers)
		}
		batch := stalePlayers[i:end]

		placeholders := "WHERE name IN (?" + strings.Repeat(",?", len(batch)-1) + ")"
		query := "DELETE FROM characters " + placeholders

		result, err := db.Exec(query, batch...)
		if err != nil {
			log.Printf("❌ [Characters] Failed to clean up batch of old player records: %v", err)
			continue // Continue to next batch
		}
		rowsAffected, _ := result.RowsAffected()
		if enableCharacterScraperDebugLogs {
			log.Printf("    -> [DB] Cleaned %d stale records in batch.", rowsAffected)
		}
	}
	log.Printf("✅ [Characters] Cleanup complete. Removed %d stale player records in total.", len(stalePlayers))
}

// processPlayerData is now a cleaner orchestrator that uses helper functions.
func processPlayerData(playerChan <-chan PlayerCharacter, updateTime string) {
	characterMutex.Lock()
	defer characterMutex.Unlock()

	log.Println("🏆 [Characters] DB worker started. Processing scraped data...")

	// 1. Fetch existing player data
	existingPlayers, err := fetchExistingPlayers()
	if err != nil {
		log.Printf("❌ [DB] %v", err)
		// Continue with an empty map, logging will just be incomplete
	}

	// 2. Prepare database transaction and statements
	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [DB] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback() // Rollback on error

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

	changelogStmt, err := tx.Prepare(`
		INSERT INTO character_changelog (character_name, change_time, activity_description)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		log.Printf("❌ [DB] Failed to prepare changelog statement: %v", err)
		return
	}
	defer changelogStmt.Close()

	scrapedPlayerNames := make(map[string]bool)
	totalProcessed := 0

	// 3. Process all players from the channel
	for p := range playerChan {
		p.LastUpdated = updateTime
		scrapedPlayerNames[p.Name] = true
		totalProcessed++

		lastActiveTime := updateTime // Assume active for new players
		if oldPlayer, exists := existingPlayers[p.Name]; exists {
			// Check for activity changes and get the correct lastActiveTime
			lastActiveTime = checkAndLogCharacterActivity(changelogStmt, p, oldPlayer)
		} else {
			// This is a new player
			logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("New character '%s' detected (Class: %s, Level: %d).", p.Name, p.Class, p.BaseLevel))
		}

		// Upsert the player
		if _, err := stmt.Exec(p.Rank, p.Name, p.BaseLevel, p.JobLevel, p.Experience, p.Class, p.LastUpdated, lastActiveTime); err != nil {
			log.Printf("    -> [DB] WARN: Failed to upsert character for player %s: %v", p.Name, err)
		}
	}

	// 4. Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Printf("❌ [DB] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("✅ [Characters] Saved/updated %d records.", totalProcessed)

	// 5. Clean up stale records (outside the transaction)
	cleanupStalePlayers(scrapedPlayerNames, existingPlayers)

	log.Printf("✅ [Characters] Scrape and update process complete.")
}

func scrapePlayerCharacters() {
	log.Println("🏆 [Characters] Starting player character scrape...")

	// Regex definitions (unchanged)
	rankRegex := regexp.MustCompile(`p\-1 text\-center font\-medium\\",\\"children\\":(\d+)\}\]`)
	nameRegex := regexp.MustCompile(`max-w-10 truncate p-1 font-semibold">([^<]+)</td>`)
	baseLevelRegex := regexp.MustCompile(`\\"level\\":(\d+),`)
	jobLevelRegex := regexp.MustCompile(`\\"job_level\\":(\d+),\\"exp`)
	expRegex := regexp.MustCompile(`\\"exp\\":(\d+)`)
	classRegex := regexp.MustCompile(`"hidden text\-sm sm:inline\\",\\"children\\":\\"([^"]+)\\"`)

	// Use the helper to find the last page
	const firstPageURL = "https://projetoyufa.com/rankings?page=1"
	lastPage := scraperClient.findLastPage(firstPageURL, "[Characters]")
	updateTime := time.Now().Format(time.RFC3339)

	// --- Producer-Consumer setup ---
	playerChan := make(chan PlayerCharacter, 100) // Buffered channel
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Concurrency semaphore for scraping

	// Start the single DB consumer goroutine
	// It will wait until playerChan is filled and closed.
	go processPlayerData(playerChan, updateTime)
	// --------------------------------

	log.Printf("🏆 [Characters] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }()

			url := fmt.Sprintf("https://projetoyufa.com/rankings?page=%d", pageIndex)

			bodyContent, err := scraperClient.getPage(url, "[Characters]")
			if err != nil {
				log.Printf("    -> ❌ All retries failed for page %d: %v", pageIndex, err)
				return
			}

			// --- Parsing logic (unchanged) ---
			rankMatches := rankRegex.FindAllStringSubmatch(bodyContent, -1)
			nameMatches := nameRegex.FindAllStringSubmatch(bodyContent, -1)
			baseLevelMatches := baseLevelRegex.FindAllStringSubmatch(bodyContent, -1)
			jobLevelMatches := jobLevelRegex.FindAllStringSubmatch(bodyContent, -1)
			expMatches := expRegex.FindAllStringSubmatch(bodyContent, -1)
			classMatches := classRegex.FindAllStringSubmatch(bodyContent, -1)

			numChars := len(nameMatches)

			if numChars == 0 || len(rankMatches) != numChars || len(baseLevelMatches) != numChars || len(jobLevelMatches) != numChars || len(expMatches) != numChars || len(classMatches) != numChars {
				log.Printf("    -> ⚠️ [Characters] Mismatch in regex match counts on page %d. Skipping page. (Ranks: %d, Names: %d, Classes: %d)", pageIndex, len(rankMatches), len(nameMatches), len(classMatches))
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
					BaseLevel:  baseLevel,
					JobLevel:   jobLevel,
					Experience: rawExp / 1000000.0,
					Class:      class,
					Name:       name,
				}
				pagePlayers = append(pagePlayers, player)
			}
			// --- End parsing logic ---

			// Send found players to the consumer
			if len(pagePlayers) > 0 {
				for _, player := range pagePlayers {
					playerChan <- player
				}
			}
			log.Printf("    -> Scraped page %d/%d, sent %d chars to DB worker.", pageIndex, lastPage, len(pagePlayers))
		}(page)
	}

	// Wait for all *scraping* goroutines to finish
	wg.Wait()
	// Close the channel to signal the *consumer* that no more data is coming
	close(playerChan)
	log.Printf("✅ [Characters] Finished scraping all pages. DB worker is now processing data...")
}

type GuildMemberJSON struct {
	Name string `json:"name"`
}

type GuildJSON struct {
	Name    string            `json:"name"`
	Level   int               `json:"guild_lv"`
	Master  string            `json:"master"`
	Members []GuildMemberJSON `json:"members"`
}

// processGuildData handles all database transactions for updating guilds and member associations.
func processGuildData(allGuilds map[string]Guild, allMembers map[string]string) {
	characterMutex.Lock()
	defer characterMutex.Unlock()

	// 1. Fetch old associations for comparison
	oldAssociations := make(map[string]string)
	oldGuildRows, err := db.Query("SELECT name, guild_name FROM characters WHERE guild_name IS NOT NULL")
	if err != nil {
		log.Printf("⚠️ [Guilds] Could not fetch old guild associations for comparison: %v", err)
	} else {
		for oldGuildRows.Next() {
			var charName, guildName string
			if err := oldGuildRows.Scan(&charName, &guildName); err == nil {
				oldAssociations[charName] = guildName
			}
		}
		oldGuildRows.Close()
	}

	// 2. Start transaction
	tx, errDb := db.Begin()
	if errDb != nil {
		log.Printf("❌ [Guilds][DB] Failed to begin transaction for guilds update: %v", errDb)
		return
	}
	defer tx.Rollback()

	// 3. Upsert guild information
	log.Println("    -> [DB] Upserting guild information into 'guilds' table...")
	guildStmt, err := tx.Prepare(`
		INSERT INTO guilds (rank, name, level, experience, master, emblem_url, last_updated)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			level=excluded.level,
			master=excluded.master,
			last_updated=excluded.last_updated
	`)
	if err != nil {
		log.Printf("❌ [Guilds][DB] Failed to prepare guilds upsert statement: %v", err)
		return
	}
	defer guildStmt.Close()

	updateTime := time.Now().Format(time.RFC3339)
	for _, g := range allGuilds {
		if _, err := guildStmt.Exec(0, g.Name, g.Level, g.Experience, g.Master, g.EmblemURL, updateTime); err != nil {
			log.Printf("    -> [DB] WARN: Failed to upsert guild '%s': %v", g.Name, err)
		}
	}

	// 4. Update character associations
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
		} else if n, _ := res.RowsAffected(); n > 0 {
			updateCount++
		}
	}
	log.Printf("    -> [DB] Successfully associated %d characters with their guilds.", updateCount)

	// 5. Log guild changes
	changelogStmt, err := tx.Prepare(`
		INSERT INTO character_changelog (character_name, change_time, activity_description)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		log.Printf("❌ [DB] Failed to prepare changelog statement for guilds: %v", err)
	} else {
		defer changelogStmt.Close()

		allInvolvedChars := make(map[string]bool)
		for charName := range oldAssociations {
			allInvolvedChars[charName] = true
		}
		for charName := range allMembers {
			allInvolvedChars[charName] = true
		}

		for charName := range allInvolvedChars {
			// Check if character still exists
			var exists int
			err := tx.QueryRow("SELECT COUNT(*) FROM characters WHERE name = ?", charName).Scan(&exists)
			if err != nil || exists == 0 {
				continue // Skip logging for non-existent char
			}

			oldGuild, hadOld := oldAssociations[charName]
			newGuild, hasNew := allMembers[charName]

			if hadOld && !hasNew {
				logCharacterActivity(changelogStmt, charName, fmt.Sprintf("Left guild '%s'.", oldGuild))
			} else if !hadOld && hasNew {
				logCharacterActivity(changelogStmt, charName, fmt.Sprintf("Joined guild '%s'.", newGuild))
			} else if hadOld && hasNew && oldGuild != newGuild {
				logCharacterActivity(changelogStmt, charName, fmt.Sprintf("Moved from guild '%s' to '%s'.", oldGuild, newGuild))
			}
		}
	}

	// 6. Commit
	if err := tx.Commit(); err != nil {
		log.Printf("❌ [Guilds][DB] Failed to commit guilds and characters transaction: %v", err)
		return
	}

	log.Printf("✅ [Guilds] Scrape and update complete. Saved %d guild records and updated character associations.", len(allGuilds))
}

// scrapeGuilds is now only responsible for concurrent scraping.
func scrapeGuilds() {
	log.Println("🏰 [Guilds] Starting guild and character-guild association scrape...")

	// Regex definitions
	nameRegex := regexp.MustCompile(`<span class="font-medium">([^<]+)</span>`)
	levelRegex := regexp.MustCompile(`\\"guild_lv\\":(\d+),\\"connect_member\\"`)
	masterRegex := regexp.MustCompile(`\\"master\\":\\"([^"]+)\\",\\"members\\"`)
	membersRegex := regexp.MustCompile(`\\"members\\":\[(.*?)\]\}`)
	memberNameRegex := regexp.MustCompile(`\\"name\\":\\"([^"]+)\\",\\"base_level\\"`)

	const firstPageURL = "https://projetoyufa.com/rankings/guild?page=1"
	lastPage := scraperClient.findLastPage(firstPageURL, "[Guilds]")

	allGuilds := make(map[string]Guild)
	allMembers := make(map[string]string) // Map[characterName]guildName
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Concurrency semaphore

	log.Printf("🏰 [Guilds] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }()

			url := fmt.Sprintf("https://projetoyufa.com/rankings/guild?page=%d", pageIndex)

			bodyContent, err := scraperClient.getPage(url, "[Guilds]")
			if err != nil {
				log.Printf("    -> ❌ All retries failed for page %d: %v", pageIndex, err)
				return
			}

			// --- Parsing logic ---
			nameMatches := nameRegex.FindAllStringSubmatch(bodyContent, -1)
			levelMatches := levelRegex.FindAllStringSubmatch(bodyContent, -1)
			masterMatches := masterRegex.FindAllStringSubmatch(bodyContent, -1)
			membersMatches := membersRegex.FindAllStringSubmatch(bodyContent, -1)

			numGuilds := len(nameMatches)
			if numGuilds == 0 || len(levelMatches) != numGuilds || len(masterMatches) != numGuilds || len(membersMatches) != numGuilds {
				log.Printf("    -> ⚠️ [Guilds] Mismatch in regex match counts on page %d. Skipping page. (Names: %d, Levels: %d, Masters: %d, Members: %d)",
					pageIndex, len(nameMatches), len(levelMatches), len(masterMatches), len(membersMatches))
				return
			}

			var pageGuilds []Guild
			var pageMembers = make(map[string]string)
			for i := 0; i < numGuilds; i++ {
				name := nameMatches[i][1]
				level, _ := strconv.Atoi(levelMatches[i][1])
				master := masterMatches[i][1]

				guild := Guild{Name: name, Level: level, Master: master}
				pageGuilds = append(pageGuilds, guild)

				members := memberNameRegex.FindAllStringSubmatch(membersMatches[i][1], -1)
				for _, member := range members {
					pageMembers[member[1]] = name // charName -> guildName
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
			log.Printf("    -> Scraped page %d/%d, found %d guilds.", pageIndex, lastPage, len(pageGuilds))
			// --- End of parsing logic ---
		}(page)
	}
	wg.Wait()

	log.Printf("✅ [Guilds] Finished scraping all pages. Found %d unique guilds.", len(allGuilds))

	if len(allGuilds) == 0 {
		log.Println("⚠️ [Guilds] Scrape finished with 0 total guilds found. Guild/character tables will not be updated.")
		return
	}

	// Call the dedicated database function
	processGuildData(allGuilds, allMembers)
}

type CharacterZenyInfo struct {
	Zeny       sql.NullInt64
	LastActive string
}

// formatWithCommas is a small helper for formatting zeny values in logs.
func formatWithCommas(n int64) string {
	s := strconv.FormatInt(n, 10)
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
	return strings.Join(result, ",")
}

// processZenyData handles fetching old zeny data, comparing, and updating the database.
func processZenyData(allZenyInfo map[string]int64, updateTime string) {
	characterMutex.Lock()
	defer characterMutex.Unlock()

	log.Println("    -> [DB] Fetching existing character zeny data for activity comparison...")
	existingCharacters := make(map[string]CharacterZenyInfo)
	rows, err := db.Query("SELECT name, zeny, last_active FROM characters")
	if err != nil {
		log.Printf("❌ [Zeny][DB] Failed to query existing characters for comparison: %v", err)
		// Continue with an empty map
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

	changelogStmt, err := tx.Prepare(`
		INSERT INTO character_changelog (character_name, change_time, activity_description)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		log.Printf("❌ [DB] Failed to prepare changelog statement for zeny: %v", err)
		// Continue without logging if this fails
	} else {
		defer changelogStmt.Close()
	}

	updatedCount := 0
	unchangedCount := 0
	for name, newZeny := range allZenyInfo {
		oldInfo, exists := existingCharacters[name]

		// Skip update if zeny is unchanged
		if exists && oldInfo.Zeny.Valid && oldInfo.Zeny.Int64 == newZeny {
			unchangedCount++
			continue
		}

		// Log the change
		if changelogStmt != nil {
			var oldZeny int64
			if exists && oldInfo.Zeny.Valid {
				oldZeny = oldInfo.Zeny.Int64
			}
			delta := newZeny - oldZeny
			formattedNewZeny := formatWithCommas(newZeny)
			var description string

			if delta > 0 {
				description = fmt.Sprintf("Zeny increased by %sz (New total: %sz).", formatWithCommas(delta), formattedNewZeny)
			} else if delta < 0 {
				description = fmt.Sprintf("Zeny decreased by %sz (New total: %sz).", formatWithCommas(-delta), formattedNewZeny)
			}
			logCharacterActivity(changelogStmt, name, description)
		}

		// Update the database
		res, err := stmt.Exec(newZeny, updateTime, name)
		if err != nil {
			log.Printf("    -> ⚠️ [Zeny][DB] Failed to update zeny for '%s': %v", name, err)
			continue
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			updatedCount++
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ [Zeny][DB] Failed to commit transaction: %v", err)
		return
	}

	log.Printf("✅ [Zeny] Database update complete. Updated activity for %d characters. %d characters were unchanged.", updatedCount, unchangedCount)
}

// scrapeZeny is now only responsible for concurrent scraping.
func scrapeZeny() {
	log.Println("💰 [Zeny] Starting Zeny ranking scrape...")

	const firstPageURL = "https://projetoyufa.com/rankings/zeny?page=1"
	lastPage := scraperClient.findLastPage(firstPageURL, "[Zeny]")

	updateTime := time.Now().Format(time.RFC3339)
	allZenyInfo := make(map[string]int64) // Map[characterName]zeny
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Concurrency semaphore

	log.Printf("💰 [Zeny] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }()

			url := fmt.Sprintf("https://projetoyufa.com/rankings/zeny?page=%d", pageIndex)
			bodyContent, err := scraperClient.getPage(url, "[Zeny]")
			if err != nil {
				log.Printf("    -> ❌ All retries failed for page %d: %v", pageIndex, err)
				return
			}

			doc, err := goquery.NewDocumentFromReader(strings.NewReader(bodyContent))
			if err != nil {
				log.Printf("    -> ❌ Failed to parse body for page %d: %v", pageIndex, err)
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
				if err != nil {
					log.Printf("    -> ⚠️ [Zeny] Could not parse zeny value '%s' (from raw '%s') for player '%s'", zenyStrClean, zenyStrRaw, nameStr)
					return
				}

				mu.Lock()
				allZenyInfo[nameStr] = zenyVal
				mu.Unlock()
			})
			log.Printf("    -> [Zeny] Scraped page %d/%d successfully.", pageIndex, lastPage)
		}(page)
	}

	wg.Wait()
	log.Printf("✅ [Zeny] Finished scraping all pages. Found zeny info for %d characters.", len(allZenyInfo))

	if len(allZenyInfo) == 0 {
		log.Println("⚠️ [Zeny] No zeny information was scraped. Skipping database update.")
		return
	}

	// Call the dedicated database function
	processZenyData(allZenyInfo, updateTime)
}

// parseMarketItem extracts all item details from a goquery selection.
// This helper function isolates the complex name-parsing logic from the main scraper loop.
func parseMarketItem(itemSelection *goquery.Selection) (Item, bool) {
	// --- Regex for +Refine levels ---
	// These are kept locally as they are only used here.
	reRefineMid := regexp.MustCompile(`\s(\+\d+)`)    // e.g., "Item +7 Name"
	reRefineStart := regexp.MustCompile(`^(\+\d+)\s`) // e.g., "+7 Item Name"

	// --- 1. Get Base Name ---
	baseItemName := strings.TrimSpace(itemSelection.Find("p.truncate").Text())
	if baseItemName == "" {
		return Item{}, false // No name, invalid item
	}

	// --- 2. Normalize Refinements ---
	// Move mid-string refinements (e.g., "Slotted +7 Tsurugi") to the end ("Slotted Tsurugi +7")
	if match := reRefineMid.FindStringSubmatch(baseItemName); len(match) > 1 && !strings.HasSuffix(baseItemName, match[0]) {
		cleanedName := strings.Replace(baseItemName, match[0], "", 1)
		cleanedName = strings.Join(strings.Fields(cleanedName), " ") // Remove extra spaces
		baseItemName = cleanedName + match[0]
		// Move start-string refinements (e.g., "+7 Tsurugi") to the end ("Tsurugi +7")
	} else if match := reRefineStart.FindStringSubmatch(baseItemName); len(match) > 1 {
		cleanedName := strings.Replace(baseItemName, match[0], "", 1)
		cleanedName = strings.Join(strings.Fields(cleanedName), " ") // Remove extra spaces
		baseItemName = cleanedName + " " + match[1]
	}

	// --- 3. Extract and Append Card Names ---
	var cardNames []string
	itemSelection.Find("div.mt-1.flex.flex-wrap.gap-1 span[data-slot='badge']").Each(func(k int, cardSelection *goquery.Selection) {
		// The first badge is the ID, so we skip it.
		// Card badges don't have the "ID: " prefix.
		cardText := cardSelection.Text()
		if !strings.HasPrefix(cardText, "ID: ") {
			cardName := strings.TrimSpace(strings.TrimSuffix(cardText, " Card"))
			if cardName != "" {
				cardNames = append(cardNames, cardName)
			}
		}
	})

	// Finalize the name, e.g., "Tsurugi +7 [Hydra] [Skeleton Worker]"
	finalItemName := baseItemName
	if len(cardNames) > 0 {
		wrapped := make([]string, len(cardNames))
		for i, c := range cardNames {
			wrapped[i] = fmt.Sprintf(" [%s]", c)
		}
		finalItemName = fmt.Sprintf("%s%s", baseItemName, strings.Join(wrapped, ""))
	}

	// --- 4. Get Other Details ---
	quantityStr := strings.TrimSuffix(strings.TrimSpace(itemSelection.Find("span.text-xs.text-muted-foreground").Text()), "x")
	priceStr := strings.TrimSpace(itemSelection.Find("span.text-xs.font-medium.text-green-600").Text())

	// Find the ID badge specifically
	idStr := ""
	itemSelection.Find("span[data-slot='badge']").Each(func(_ int, idSelection *goquery.Selection) {
		if strings.HasPrefix(idSelection.Text(), "ID: ") {
			idStr = strings.TrimPrefix(strings.TrimSpace(idSelection.Text()), "ID: ")
		}
	})

	if priceStr == "" {
		return Item{}, false // No price, invalid item
	}

	quantity, _ := strconv.Atoi(quantityStr)
	if quantity == 0 {
		quantity = 1 // Default to 1 if parsing fails or 0
	}
	itemID, _ := strconv.Atoi(idStr)

	return Item{
		Name:     finalItemName,
		ItemID:   itemID,
		Quantity: quantity,
		Price:    priceStr,
	}, true
}

func scrapeData() {
	log.Println("🚀 [Market] Starting scrape...")

	// --- REMOVED regex definitions here, they are now in parseMarketItem ---

	const requestURL = "https://projetoyufa.com/market"

	// Use the new helper function. All the retry logic is now encapsulated.
	htmlContent, err := scraperClient.getPage(requestURL, "[Market]")
	if err != nil {
		log.Printf("❌ [Market] Failed to scrape market page after retries: %v. Aborting update.", err)
		return
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("❌ [Market] Failed to parse HTML: %v", err)
		return
	}

	retrievalTime := time.Now().Format(time.RFC3339)
	scrapedItemsByName := make(map[string][]Item)
	activeSellers := make(map[string]bool)

	// --- Main Scraper Loop ---
	// This loop is now much simpler. It just finds shops and items.
	doc.Find(`div[data-slot="card"]`).Each(func(i int, s *goquery.Selection) {
		// Get shop-level details
		shopName := strings.TrimSpace(s.Find(`div[data-slot="card-title"]`).Text())
		sellerName := strings.TrimSpace(s.Find("svg.lucide-user").Next().Text())
		mapName := strings.TrimSpace(s.Find("svg.lucide-map-pin").Next().Text())
		mapCoordinates := strings.TrimSpace(s.Find("svg.lucide-copy").Next().Text())
		activeSellers[sellerName] = true

		if enableMarketScraperDebugLogs == true {
			log.Printf("[Market] shop name: %s, seller name: %s, map_name: %s, mapcoord: %s", shopName, sellerName, mapName, mapCoordinates)
		}

		if shopName == "" || sellerName == "" {
			return // Skip shops with missing critical info
		}

		// Iterate over items in this shop
		s.Find(`div[data-slot="card-content"] .flex.items-center.space-x-2`).Each(func(j int, itemSelection *goquery.Selection) {

			// Use the helper function to do all the hard parsing
			item, ok := parseMarketItem(itemSelection)
			if !ok {
				// Item was invalid (e.g., missing name or price), skip it
				return
			}

			// Add the shop-level details to the item
			item.StoreName = shopName
			item.SellerName = sellerName
			item.MapName = mapName
			item.MapCoordinates = mapCoordinates

			if enableMarketScraperDebugLogs == true {
				log.Printf("🔎 [Market] name: %s, id: %d, qtd: %d price %s store: %s seller: %s map: %s coord %s", item.Name, item.ItemID, item.Quantity, item.Price, shopName, sellerName, mapName, mapCoordinates)
			}

			// Add the fully-formed item to our map
			scrapedItemsByName[item.Name] = append(scrapedItemsByName[item.Name], item)
		})
	})

	log.Printf("🔎 [Market] Scrape parsed. Found %d unique item names.", len(scrapedItemsByName))

	marketMutex.Lock()
	defer marketMutex.Unlock()

	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [Market] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// --- Database logic from here down is unchanged ---

	_, err = tx.Exec("INSERT OR IGNORE INTO scrape_history (timestamp) VALUES (?)", retrievalTime)
	if err != nil {
		log.Printf("❌ [Market] Failed to log scrape history: %v", err)
		return
	}

	dbStoreSizes := make(map[string]int)
	sellerItems := make(map[string]map[string]bool)
	rows, err := tx.Query("SELECT seller_name, name_of_the_item FROM items WHERE is_available = 1")
	if err != nil {
		log.Printf("❌ [Market] Could not pre-query seller item counts: %v", err)
	} else {
		for rows.Next() {
			var sellerName, itemName string
			if err := rows.Scan(&sellerName, &itemName); err != nil {
				continue
			}
			if _, ok := sellerItems[sellerName]; !ok {
				sellerItems[sellerName] = make(map[string]bool)
			}
			sellerItems[sellerName][itemName] = true
		}
		rows.Close()
		for seller, items := range sellerItems {
			dbStoreSizes[seller] = len(items)
		}
	}

	dbAvailableItemsMap := make(map[string][]Item)
	dbAvailableNames := make(map[string]bool)

	rows, err = tx.Query("SELECT name_of_the_item, item_id, quantity, price, store_name, seller_name, map_name, map_coordinates FROM items WHERE is_available = 1")
	if err != nil {
		log.Printf("❌ [Market] Could not get list of all available items: %v", err)
		return
	}
	for rows.Next() {
		var item Item
		err := rows.Scan(&item.Name, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &item.MapName, &item.MapCoordinates)
		if err != nil {
			log.Printf("⚠️ [Market] Failed to scan existing item: %v", err)
			continue
		}
		dbAvailableItemsMap[item.Name] = append(dbAvailableItemsMap[item.Name], item)
		dbAvailableNames[item.Name] = true
	}
	rows.Close()

	itemsUpdated := 0
	itemsUnchanged := 0
	itemsAdded := 0

	for itemName, currentScrapedItems := range scrapedItemsByName {

		lastAvailableItems := dbAvailableItemsMap[itemName]

		if !areItemSetsIdentical(currentScrapedItems, lastAvailableItems) {
			currentSet := make(map[comparableItem]bool)
			for _, item := range currentScrapedItems {
				currentSet[toComparable(item)] = true
			}

			for _, lastItem := range lastAvailableItems {
				if _, found := currentSet[toComparable(lastItem)]; !found {

					eventType := "REMOVED"
					if _, sellerIsActive := activeSellers[lastItem.SellerName]; sellerIsActive {
						eventType = "SOLD"
					} else if dbStoreSizes[lastItem.SellerName] == 1 {
						eventType = "REMOVED_SINGLE"
					}

					details, _ := json.Marshal(map[string]interface{}{
						"price":      lastItem.Price,
						"quantity":   lastItem.Quantity,
						"seller":     lastItem.SellerName,
						"store_name": lastItem.StoreName,
					})
					_, err := tx.Exec(`INSERT INTO market_events (event_timestamp, event_type, item_name, item_id, details) VALUES (?, ?, ?, ?, ?)`, retrievalTime, eventType, lastItem.Name, lastItem.ItemID, string(details))
					if err != nil {
						log.Printf("❌ [Market] Failed to log %s event for %s: %v", eventType, lastItem.Name, err)
					}
				}
			}
		}

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
				details, _ := json.Marshal(map[string]interface{}{
					"price":      firstItem.Price,
					"quantity":   firstItem.Quantity,
					"seller":     firstItem.SellerName,
					"store_name": firstItem.StoreName,
				})
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
				details, _ := json.Marshal(map[string]interface{}{
					"price":      lowestPriceListingInBatch.Price,
					"quantity":   lowestPriceListingInBatch.Quantity,
					"seller":     lowestPriceListingInBatch.SellerName,
					"store_name": lowestPriceListingInBatch.StoreName,
				})
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

			removedListings := dbAvailableItemsMap[name]

			for _, listing := range removedListings {

				eventType := "REMOVED"
				if _, sellerIsActive := activeSellers[listing.SellerName]; sellerIsActive {
					eventType = "SOLD"
				} else if dbStoreSizes[listing.SellerName] == 1 {
					eventType = "REMOVED_SINGLE"
				}

				details, _ := json.Marshal(map[string]interface{}{
					"price":      listing.Price,
					"quantity":   listing.Quantity,
					"seller":     listing.SellerName,
					"store_name": listing.StoreName,
				})
				_, err = tx.Exec(`INSERT INTO market_events (event_timestamp, event_type, item_name, item_id, details) VALUES (?, ?, ?, ?, ?)`, retrievalTime, eventType, name, listing.ItemID, string(details))
				if err != nil {
					log.Printf("❌ [Market] Failed to log %s event for removed item %s: %v", eventType, name, err)
				}
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

func toComparable(item Item) comparableItem {
	return comparableItem{
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

func areItemSetsIdentical(setA, setB []Item) bool {
	if len(setA) != len(setB) {
		return false
	}
	counts := make(map[comparableItem]int)
	for _, item := range setA {
		counts[toComparable(item)]++
	}
	for _, item := range setB {
		if counts[toComparable(item)] == 0 {
			return false
		}
		counts[toComparable(item)]--
	}
	return true
}

// fetchAllCharacterNames provides a set of all valid character names.
func fetchAllCharacterNames() (map[string]bool, error) {
	allCharacterNames := make(map[string]bool)
	charRows, err := db.Query("SELECT name FROM characters")
	if err != nil {
		return nil, fmt.Errorf("failed to pre-fetch character names: %w", err)
	}
	defer charRows.Close()
	for charRows.Next() {
		var name string
		if err := charRows.Scan(&name); err == nil {
			allCharacterNames[name] = true
		}
	}
	return allCharacterNames, nil
}

// fetchExistingMvpKills retrieves the current kill counts from the database.
func fetchExistingMvpKills() (map[string]map[string]int, error) {
	allExistingKills := make(map[string]map[string]int)

	// Build the SELECT query dynamically
	selectCols := make([]string, 0, len(mvpMobIDs)+1)
	selectCols = append(selectCols, "character_name")
	scanDest := make([]interface{}, len(mvpMobIDs)+1)
	scanDest[0] = new(string)
	columnValues := make([]sql.NullInt64, len(mvpMobIDs))

	for i, mobID := range mvpMobIDs {
		selectCols = append(selectCols, fmt.Sprintf("mvp_%s", mobID))
		scanDest[i+1] = &columnValues[i]
	}

	mvpQuery := fmt.Sprintf("SELECT %s FROM character_mvp_kills", strings.Join(selectCols, ", "))
	mvpRows, err := db.Query(mvpQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to pre-fetch MVP kills: %w", err)
	}
	defer mvpRows.Close()

	for mvpRows.Next() {
		if err := mvpRows.Scan(scanDest...); err != nil {
			log.Printf("    -> [MVP][DB] WARN: Failed to scan existing MVP row: %v", err)
			continue
		}
		charName := *(scanDest[0].(*string))
		playerKills := make(map[string]int)
		for i, mobID := range mvpMobIDs {
			if columnValues[i].Valid {
				playerKills[mobID] = int(columnValues[i].Int64)
			}
		}
		allExistingKills[charName] = playerKills
	}
	return allExistingKills, nil
}

// processMvpKills handles all database logic for the MVP scraper.
func processMvpKills(allMvpKills map[string]map[string]int) {
	characterMutex.Lock()
	defer characterMutex.Unlock()

	// 1. Fetch prerequisite data
	allCharacterNames, err := fetchAllCharacterNames()
	if err != nil {
		log.Printf("❌ [MVP][DB] %v. Aborting update.", err)
		return
	}

	allExistingKills, err := fetchExistingMvpKills()
	if err != nil {
		log.Printf("❌ [MVP][DB] %v. Aborting update.", err)
		return
	}

	// 2. Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [MVP][DB] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// 3. Build and prepare the dynamic UPSERT statement
	columnNames := []string{"character_name"}
	valuePlaceholders := []string{"?"}
	updateSetters := []string{}
	for _, mobID := range mvpMobIDs {
		colName := fmt.Sprintf("mvp_%s", mobID)
		columnNames = append(columnNames, colName)
		valuePlaceholders = append(valuePlaceholders, "?")
		updateSetters = append(updateSetters, fmt.Sprintf("%s=excluded.%s", colName, colName))
	}

	queryStr := fmt.Sprintf(`
		INSERT INTO character_mvp_kills (%s)
		VALUES (%s)
		ON CONFLICT(character_name) DO UPDATE SET %s
	`, strings.Join(columnNames, ", "), strings.Join(valuePlaceholders, ", "), strings.Join(updateSetters, ", "))

	stmt, err := tx.Prepare(queryStr)
	if err != nil {
		log.Printf("❌ [MVP][DB] Failed to prepare MVP kills upsert statement: %v", err)
		return
	}
	defer stmt.Close()

	// 4. Iterate and execute upserts
	updateCount := 0
	for charName, newKills := range allMvpKills {
		if !allCharacterNames[charName] {
			continue // Skip characters not in the main 'characters' table
		}
		updateCount++

		existingKills := allExistingKills[charName]
		if existingKills == nil {
			existingKills = make(map[string]int)
		}

		params := []interface{}{charName}
		for _, mobID := range mvpMobIDs {
			newKillCount := newKills[mobID]           // 0 if not in map
			existingKillCount := existingKills[mobID] // 0 if not in map

			// This is your stale data protection logic:
			// Only update if the new count is >= the existing one.
			finalKillCount := newKillCount
			if existingKillCount > newKillCount {
				finalKillCount = existingKillCount
			}
			params = append(params, finalKillCount)
		}

		if _, err := stmt.Exec(params...); err != nil {
			log.Printf("    -> [MVP][DB] WARN: Failed to upsert MVP kills for player %s: %v", charName, err)
		}
	}

	// 5. Commit
	if err := tx.Commit(); err != nil {
		log.Printf("❌ [MVP][DB] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("✅ [MVP] Saved/updated MVP kill records for %d characters.", updateCount)
	log.Printf("✅ [MVP] Scrape and update process complete.")
}

// scrapeMvpKills is now only responsible for concurrent scraping.
func scrapeMvpKills() {
	log.Println("☠️  [MVP] Starting MVP kill count scrape...")

	playerBlockRegex := regexp.MustCompile(`\\"name\\":\\"([^"]+)\\",\\"base_level\\".*?\\"mvp_kills\\":\[(.*?)]`)
	mvpKillsRegex := regexp.MustCompile(`{\\"mob_id\\":(\d+),\\"kills\\":(\d+)}`)

	const firstPageURL = "https://projetoyufa.com/rankings/mvp?page=1"
	lastPage := scraperClient.findLastPage(firstPageURL, "[MVP]")

	allMvpKills := make(map[string]map[string]int) // Map[characterName]Map[mobID]killCount
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	log.Printf("☠️  [MVP] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }()

			url := fmt.Sprintf("https://projetoyufa.com/rankings/mvp?page=%d", pageIndex)
			bodyContent, err := scraperClient.getPage(url, "[MVP]")
			if err != nil {
				log.Printf("    -> ❌ All retries failed for page %d: %v", pageIndex, err)
				return
			}

			playerBlocks := playerBlockRegex.FindAllStringSubmatch(bodyContent, -1)
			if len(playerBlocks) == 0 {
				log.Printf("    -> ⚠️ [MVP] No player data blocks found on page %d.", pageIndex)
				return
			}

			pageKills := make(map[string]map[string]int)
			for _, block := range playerBlocks {
				charName := block[1]
				mvpsJSON := block[2]
				playerKills := make(map[string]int)
				killMatches := mvpKillsRegex.FindAllStringSubmatch(mvpsJSON, -1)
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
			log.Printf("    -> [MVP] Scraped page %d/%d, found %d characters with MVP kills.", pageIndex, lastPage, len(pageKills))
		}(page)
	}

	wg.Wait()
	log.Printf("✅ [MVP] Finished scraping all pages. Found %d unique characters with MVP kills.", len(allMvpKills))

	if len(allMvpKills) == 0 {
		log.Println("⚠️ [MVP] No MVP kills found after scrape. Skipping database update.")
		return
	}

	// Call the dedicated database function
	processMvpKills(allMvpKills)
}

// Job defines a background task with its function and schedule.
type Job struct {
	Name     string
	Func     func()
	Interval time.Duration
}

// runJobOnTicker executes a job immediately and then on its scheduled interval.
// It stops when the provided context is canceled.
func runJobOnTicker(ctx context.Context, job Job) {
	ticker := time.NewTicker(job.Interval)
	defer ticker.Stop()

	log.Printf("🕒 [Job] Starting initial run for %s job...", job.Name)
	//job.Func() // Run immediately on start

	for {
		select {
		case <-ctx.Done():
			log.Printf("🔌 [Job] Stopping %s job due to shutdown.", job.Name)
			return
		case <-ticker.C:
			log.Printf("🕒 [Job] Starting scheduled %s scrape...", job.Name)
			job.Func()
		}
	}
}

func startBackgroundJobs(ctx context.Context) {
	// Define all scheduled jobs
	jobs := []Job{
		{Name: "Market", Func: scrapeData, Interval: 3 * time.Minute},
		{Name: "Player Count", Func: scrapeAndStorePlayerCount, Interval: 1 * time.Minute},
		{Name: "Player Character", Func: scrapePlayerCharacters, Interval: 30 * time.Minute},
		{Name: "Guild", Func: scrapeGuilds, Interval: 25 * time.Minute},
		{Name: "Zeny", Func: scrapeZeny, Interval: 1 * time.Hour},
		{Name: "MVP Kill", Func: scrapeMvpKills, Interval: 5 * time.Minute},
	}

	// Start all standard jobs
	for _, job := range jobs {
		go runJobOnTicker(ctx, job)
	}

	// --- Special RMS Cache Refresh Job (runs once daily) ---
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		log.Printf("🕒 [Job] Starting initial run for RMS Cache Refresh job...")
		runFullRMSCacheJob() // Run immediately on start
		log.Printf("🕒 [Job] RMS Cache Refresh job scheduled. Will run once every 24 hours.")

		for {
			select {
			case <-ctx.Done():
				log.Printf("🔌 [Job] Stopping RMS Cache Refresh job due to shutdown.")
				return
			case <-ticker.C:
				log.Printf("🕒 [Job] Starting scheduled 24-hour full RMS cache refresh...")
				// Run in its own goroutine so it doesn't block the ticker
				// if the job takes a long time.
				go runFullRMSCacheJob()
			}
		}
	}()
}
