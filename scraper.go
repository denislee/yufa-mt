package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

const (
	enablePlayerCountDebugLogs      = false
	enableCharacterScraperDebugLogs = false
	enableGuildScraperDebugLogs     = false
	enableMvpScraperDebugLogs       = false
	enableZenyScraperDebugLogs      = false
	enableMarketScraperDebugLogs    = false
	enableWoeScraperDebugLogs       = true
	enableChatScraperDebugLogs      = true
)

var jobIDToClassName = map[string]string{
	"0":    "Aprendiz",
	"4001": "Super Aprendiz",
	"3":    "Arqueiro",
	"1":    "Espadachim",
	"6":    "Gatuno",
	"2":    "Mago",
	"5":    "Mercador",
	"4":    "Noviço",
	"18":   "Alquimista",
	"17":   "Arruaceiro",
	"19":   "Bardo",
	"9":    "Bruxo",
	"7":    "Cavaleiro",
	"11":   "Caçador",
	"10":   "Ferreiro",
	"12":   "Mercenário",
	"15":   "Monge",
	"20":   "Odalisca",
	"8":    "Sacerdote",
	"16":   "Sábio",
	"14":   "Templário",
}

var jobIconRegex = regexp.MustCompile(`icon_jobs_(\d+)\.png`) // Regex to extract ID from URL

var (
	marketMutex        sync.Mutex
	characterMutex     sync.Mutex
	playerCountMutex   sync.Mutex
	ptNameMutex        sync.Mutex
	ptNameRegex        = regexp.MustCompile(`<h1 class="item-title-db">([^<]+)</h1>`)
	slotRemoverRegex   = regexp.MustCompile(`\s*\[\d+\]\s*`)
	lastChatPacketTime atomic.Int64 // Stores Unix timestamp
	lastActivityLog    time.Time
	activityLogMutex   sync.Mutex
)

type chatPacketDefinition struct {
	prefix        []byte // The packet's byte prefix
	messageOffset int    // How many bytes from prefix start to the message text
	headerLength  int    // The length of the header to subtract from the packet length field
}

// in scraper.go

var (
	knownChatPackets = []chatPacketDefinition{
		// Standard chat
		{prefix: []byte{0xf3, 0x00}, messageOffset: 4, headerLength: 4},
		// Guild/Party chat?
		{prefix: []byte{0x8e, 0x00}, messageOffset: 4, headerLength: 4},
		// Channel chat (e.g., [Trade], [Global])
		{prefix: []byte{0xc1, 0x02}, messageOffset: 12, headerLength: 12},
		// Drop notification
		{prefix: []byte{0x9a, 0x00}, messageOffset: 4, headerLength: 4},
		// System/Event Announcement (Invasion, WoE, etc.)
		{prefix: []byte{0xc3, 0x01}, messageOffset: 16, headerLength: 16},
	}
)

const (
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
	defaultTimeout   = 45 * time.Second
	maxScrapeRetries = 3
	retryScrapeDelay = 3 * time.Second
)

const (
	maxParseRetries = 3 // Max attempts to parse a page if it returns 0 items
	parseRetryDelay = 2 * time.Second
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
			log.Printf("[W] %s Error on page (attempt %d/%d): %v", logPrefix, attempt, maxScrapeRetries, doErr)
			time.Sleep(retryScrapeDelay)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("received non-200 status: %d", resp.StatusCode)
			log.Printf("[W] %s Non-200 status (attempt %d/%d): %d", logPrefix, attempt, maxScrapeRetries, resp.StatusCode)
			resp.Body.Close()
			time.Sleep(retryScrapeDelay)
			continue
		}

		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			err = readErr
			log.Printf("[E] %s Failed to read body (attempt %d/%d): %v", logPrefix, attempt, maxScrapeRetries, readErr)
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

// in scraper.go

// findLastPage determines the total number of pages for a paginated ranking.
func (sc *ScraperClient) findLastPage(firstPageURL, logPrefix string) int {
	log.Printf("[I] %s Determining total number of pages...", logPrefix)

	bodyContent, err := sc.getPage(firstPageURL, logPrefix)
	if err != nil {
		log.Printf("[W] %s Could not fetch page 1 to determine page count. Assuming 1 page. Error: %v", logPrefix, err)
		return 1
	}

	// 1. Use goquery to parse the document first.
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(bodyContent))
	if err != nil {
		log.Printf("[W] %s Could not parse page 1 HTML to determine page count. Assuming 1 page. Error: %v", logPrefix, err)
		return 1
	}

	// --- START MODIFICATION ---
	// 2. Try to find the specific "Page/Página X of/de Y" div text.
	// This regex handles "Page 1 of 15" and "Página 1 de 15", ignoring comments.
	pageOfRegex := regexp.MustCompile(`(?:Page|Página)\s+\d+\s+(?:of|de)\s+(\d+)`)
	var foundLastPageFromText bool
	var lastPageFromText int

	doc.Find("div.flex.w-fit.items-center.justify-center.text-sm.font-medium").Each(func(i int, s *goquery.Selection) {
		if foundLastPageFromText { // Stop searching once found
			return
		}
		t := s.Text() // .Text() strips comments, giving "Página 1 de 15"
		matches := pageOfRegex.FindStringSubmatch(t)

		// matches[1] will be the last page number (e.g., "15")
		if len(matches) > 1 {
			if p, pErr := strconv.Atoi(matches[1]); pErr == nil {
				log.Printf("[I] %s Found 'Page/Página X of/de Y' div text. Total pages: %d", logPrefix, p)
				lastPageFromText = p
				foundLastPageFromText = true
			}
		}
	})

	// If we found the page number from the text, return it.
	if foundLastPageFromText {
		return lastPageFromText
	}

	// 3. If "Page X of Y" div fails, fall back to parsing the links.
	log.Printf("[W] %s Could not find 'Page X of Y' div. Falling back to link parsing...", logPrefix)
	// --- END MODIFICATION ---

	lastPage := 1
	pageRegex := regexp.MustCompile(`page=(\d+)`)
	var foundLastPageLink bool

	// 4. Try to find the "Last Page" link (e.g., '>>')
	lastPageSelection := doc.Find("svg.lucide-chevrons-right")
	if lastPageSelection.Length() > 0 {
		// Find the parent <a> tag and get its href
		href, exists := lastPageSelection.Parent().Attr("href")
		if exists {
			matches := pageRegex.FindStringSubmatch(href)
			if len(matches) > 1 {
				if p, pErr := strconv.Atoi(matches[1]); pErr == nil {
					lastPage = p
					foundLastPageLink = true
					log.Printf("[I] %s Found 'Last Page' (>>) link. Total pages: %d", logPrefix, lastPage)
				}
			}
		}
	}

	// 5. If no "Last Page" link is found (e.g., only a few pages exist),
	// fall back to: find the max page number from all visible links.
	if !foundLastPageLink {
		doc.Find("a[href*='page=']").Each(func(i int, s *goquery.Selection) {
			href, exists := s.Attr("href")
			if !exists {
				return
			}
			matches := pageRegex.FindStringSubmatch(href)
			if len(matches) > 1 {
				if p, pErr := strconv.Atoi(matches[1]); pErr == nil {
					if p > lastPage {
						lastPage = p
					}
				}
			}
		})
		log.Printf("[I] %s No 'Last Page' (>>) link found. Using max of visible links. Total pages: %d", logPrefix, lastPage)
	}

	return lastPage
}

func logCharacterActivity(changelogStmt *sql.Stmt, charName string, description string) error {
	if description == "" {
		return nil
	}

	_, err := changelogStmt.Exec(charName, time.Now().Format(time.RFC3339), description)
	if err != nil {
		log.Printf("[E] [Changelog] Failed to log activity for %s: %v", charName, err)
	}
	return err
}

func scrapeAndStorePlayerCount() {
	log.Println("[I] [Scraper/PlayerCount] Checking player and seller count...")
	const url = "https://projetoyufa.com/info"

	// Use the shared client's getPage method.
	bodyContent, err := scraperClient.getPage(url, "[Counter]")
	if err != nil {
		log.Printf("[E] [Scraper/PlayerCount] Failed to fetch player info page: %v", err)
		return
	}

	if enablePlayerCountDebugLogs {
		log.Printf("[D] [Scraper/PlayerCount] Page fetched successfully. Body size: %d bytes", len(bodyContent))
	}

	// Parse the HTML content
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(bodyContent))
	if err != nil {
		log.Printf("[E] [Scraper/PlayerCount] Failed to parse player info page HTML: %v", err)
		return
	}

	// --- THIS IS THE UPDATED SECTION ---
	// Define a regex to match the exact pattern "Online 100" and capture the number
	onlinePlayerRegex := regexp.MustCompile(`Online(?:\s|<!--.*?-->)*?(\d+)`)

	var onlineCount int
	var found bool

	// Iterate over all <p> tags
	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		if found { // Stop searching once we have a valid match
			return
		}

		text := s.Text()
		matches := onlinePlayerRegex.FindStringSubmatch(text)

		// matches[0] will be "Online 100"
		// matches[1] will be "100"
		if len(matches) > 1 {
			if enablePlayerCountDebugLogs {
				log.Printf("[D] [Scraper/PlayerCount] Found <p> tag text matching regex: '%s'", text)
				log.Printf("[D] [Scraper/PlayerCount] Regex captured number string: '%s'", matches[1])
			}

			if num, err := strconv.Atoi(matches[1]); err == nil {
				onlineCount = num
				found = true // Set found to true to stop the loop
				if enablePlayerCountDebugLogs {
					log.Printf("[D] [Scraper/PlayerCount] Parsed onlineCount: %d", onlineCount)
				}
			}
		} else if enablePlayerCountDebugLogs && strings.HasPrefix(text, "Online") {
			// This debug log helps if the structure changes slightly
			log.Printf("[D] [Scraper/PlayerCount] Found <p> tag starting with 'Online' but it did NOT match regex: '%s'", text)
		}
	})
	// --- END OF UPDATE ---

	if !found {
		log.Println("[W] [Scraper/PlayerCount] Could not find player count on the info page after successful load. The selector `p` with text matching regex 'Online\\s+(\\d+)' may need updating.")
		return
	}

	playerCountMutex.Lock()
	defer playerCountMutex.Unlock()

	var sellerCount int
	err = db.QueryRow("SELECT COUNT(DISTINCT seller_name) FROM items WHERE is_available = 1").Scan(&sellerCount)
	if err != nil {
		log.Printf("[W] [Scraper/PlayerCount] Could not query for unique seller count: %v", err)
		sellerCount = 0
	}
	if enablePlayerCountDebugLogs {
		log.Printf("[D] [Scraper/PlayerCount] Found %d unique sellers in DB.", sellerCount)
	}

	var lastPlayerCount int
	var lastSellerCount sql.NullInt64
	err = db.QueryRow("SELECT count, seller_count FROM player_history ORDER BY timestamp DESC LIMIT 1").Scan(&lastPlayerCount, &lastSellerCount)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("[W] [Scraper/PlayerCount] Could not query for last player/seller count: %v", err)
		return
	}

	if enablePlayerCountDebugLogs {
		if err == sql.ErrNoRows {
			log.Println("[D] [Scraper/PlayerCount] No previous player count found in DB.")
		} else {
			log.Printf("[D] [Scraper/PlayerCount] Last DB values - Players: %d, Sellers: %d (Valid: %v)", lastPlayerCount, lastSellerCount.Int64, lastSellerCount.Valid)
		}
	}

	if err != sql.ErrNoRows && onlineCount == lastPlayerCount && lastSellerCount.Valid && sellerCount == int(lastSellerCount.Int64) {
		if enablePlayerCountDebugLogs {
			log.Printf("[D] [Scraper/PlayerCount] Player/seller count unchanged (%d players, %d sellers). No update needed.", onlineCount, sellerCount)
		}
		return
	}

	if enablePlayerCountDebugLogs {
		log.Printf("[D] [Scraper/PlayerCount] Values changed. Old: (P: %d, S: %d), New: (P: %d, S: %d). Proceeding with insert.", lastPlayerCount, lastSellerCount.Int64, onlineCount, sellerCount)
	}

	retrievalTime := time.Now().Format(time.RFC3339)
	_, err = db.Exec("INSERT INTO player_history (timestamp, count, seller_count) VALUES (?, ?, ?)", retrievalTime, onlineCount, sellerCount)
	if err != nil {
		log.Printf("[E] [Scraper/PlayerCount] Failed to insert new player/seller count: %v", err)
		return
	}

	log.Printf("[I] [Scraper/PlayerCount] Player/seller count updated. New values: %d players, %d sellers", onlineCount, sellerCount)
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
		log.Printf("[D] [Scraper/Char] Player '%s' showed no change. last_active remains %s.", p.Name, lastActiveTime)

	}

	return lastActiveTime
}

// fetchExistingPlayers queries the DB for all player data needed for comparison.
func fetchExistingPlayers() (map[string]PlayerCharacter, error) {
	if enableCharacterScraperDebugLogs {
		log.Println("[D] [Scraper/Char] Fetching existing player data for activity comparison...")
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
			log.Printf("[W] [Scraper/Char] Failed to scan existing player row: %v", err)
			continue
		}
		existingPlayers[p.Name] = p
	}

	if enableCharacterScraperDebugLogs {
		log.Printf("[D] [Scraper/Char] Found %d existing player records for comparison.", len(existingPlayers))
	}
	return existingPlayers, nil
}

// cleanupStalePlayers removes players from the DB who were not in the latest scrape.
func cleanupStalePlayers(scrapedPlayerNames map[string]bool, existingPlayers map[string]PlayerCharacter) {
	log.Println("[I] [Scraper/Char] Cleaning up old player records not found in this scrape...")
	var stalePlayers []interface{}
	for existingName := range existingPlayers {
		if !scrapedPlayerNames[existingName] {
			stalePlayers = append(stalePlayers, existingName)
		}
	}

	if len(stalePlayers) == 0 {
		log.Println("[I] [Scraper/Char] Cleanup complete. No stale records found.")
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
			log.Printf("[E] [Scraper/Char] Failed to clean up batch of old player records: %v", err)
			continue // Continue to next batch
		}
		rowsAffected, _ := result.RowsAffected()
		if enableCharacterScraperDebugLogs {
			log.Printf("[D] [Scraper/Char] Cleaned %d stale records in batch.", rowsAffected)
		}
	}
	log.Printf("[I] [Scraper/Char] Cleanup complete. Removed %d stale player records in total.", len(stalePlayers))
}

// processPlayerData is the modified "consumer" function.
// --- MODIFICATION ---
func processPlayerData(playerChan <-chan PlayerCharacter) {
	// --- END MODIFICATION ---
	characterMutex.Lock()
	defer characterMutex.Unlock()

	log.Println("[I] [Scraper/Char] DB worker started. Processing scraped data...")

	// 1. Fetch existing player data
	existingPlayers, err := fetchExistingPlayers()
	if err != nil {
		log.Printf("[E] [Scraper/Char] %v", err)
		// Continue with an empty map, logging will just be incomplete
	}

	// --- MODIFICATION ---
	// Generate the timestamp *after* data is confirmed to be ready for processing
	// and *before* the transaction begins.
	updateTime := time.Now().Format(time.RFC3339)
	// --- END MODIFICATION ---

	// 2. Prepare database transaction and statements
	tx, err := db.Begin()
	if err != nil {
		log.Printf("[E] [Scraper/Char] Failed to begin transaction: %v", err)
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
		log.Printf("[E] [Scraper/Char] Failed to prepare characters upsert statement: %v", err)
		return
	}
	defer stmt.Close()

	changelogStmt, err := tx.Prepare(`
		INSERT INTO character_changelog (character_name, change_time, activity_description)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		log.Printf("[E] [Scraper/Char] Failed to prepare changelog statement: %v", err)
		return
	}
	defer changelogStmt.Close()

	scrapedPlayerNames := make(map[string]bool)
	totalProcessed := 0

	// 3. Process all players from the channel
	for p := range playerChan {
		p.LastUpdated = updateTime // Use the new timestamp
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
			log.Printf("[W] [Scraper/Char] Failed to upsert character for player %s: %v", p.Name, err)
		}
	}

	// 4. Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Printf("[E] [Scraper/Char] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("[I] [Scraper/Char] Saved/updated %d records.", totalProcessed)

	if totalProcessed == 0 {
		log.Println("[W] [Scraper/Char] Scraper processed 0 total characters. This might be a parsing error. Skipping stale player cleanup to avoid wiping data.")
		return // Abort before cleanup
	}

	// 5. Clean up stale records (outside the transaction)
	cleanupStalePlayers(scrapedPlayerNames, existingPlayers)

	log.Printf("[I] [Scraper/Char] Scrape and update process complete.")
}

// scrapePlayerCharacters is the concurrent "producer" for character data.
func scrapePlayerCharacters() {
	log.Println("[I] [Scraper/Char] Starting player character scrape...")

	const firstPageURL = "https://projetoyufa.com/rankings?page=1"
	lastPage := scraperClient.findLastPage(firstPageURL, "[Characters]")

	playerChan := make(chan PlayerCharacter, 100)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Concurrency semaphore

	// Start the single DB consumer goroutine
	go processPlayerData(playerChan)

	log.Printf("[I] [Scraper/Char] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }()

			url := fmt.Sprintf("https://projetoyufa.com/rankings?page=%d", pageIndex)
			var pagePlayers []PlayerCharacter

			for attempt := 1; attempt <= maxParseRetries; attempt++ {
				bodyContent, err := scraperClient.getPage(url, "[Characters]")
				if err != nil {
					log.Printf("[E] [Scraper/Char] Network/HTTP error for page %d (attempt %d/%d): %v. Retrying...", pageIndex, attempt, maxParseRetries, err)
					time.Sleep(parseRetryDelay)
					continue
				}

				pagePlayers, err = parseCharacterPage(bodyContent, pageIndex, attempt)
				if err != nil {
					// Error was a parsing failure (e.g., 0 items)
					log.Printf("[W] [Scraper/Char] %v. Retrying...", err)
					time.Sleep(parseRetryDelay)
					continue // Try fetching and parsing again
				}

				// Success
				break
			}

			// Send found players (if any) to the consumer
			if len(pagePlayers) > 0 {
				for _, player := range pagePlayers {
					playerChan <- player
				}
				log.Printf("[D] [Scraper/Char] Scraped page %d/%d, sent %d chars to DB worker.", pageIndex, lastPage, len(pagePlayers))
			} else {
				log.Printf("[E] [Scraper/Char] Failed to scrape page %d/%d after all retries.", pageIndex, lastPage)
			}
		}(page)
	}

	wg.Wait()
	close(playerChan) // Signal consumer that all scraping is done
	log.Printf("[I] [Scraper/Char] Finished scraping all pages. DB worker is now processing data...")
}

// parseCharacterPage contains all the parsing logic for a character page.
func parseCharacterPage(bodyContent string, pageIndex, attempt int) ([]PlayerCharacter, error) {
	rankRegex := regexp.MustCompile(`p\-1 text\-center font\-medium\\",\\"children\\":(\d+)\}\]`)
	nameRegex := regexp.MustCompile(`max-w-10 truncate p-1 font-semibold">([^<]+)</td>`)
	baseLevelRegex := regexp.MustCompile(`\\"level\\":(\d+),`)
	jobLevelRegex := regexp.MustCompile(`\\"job_level\\":(\d+),\\"exp`)
	expRegex := regexp.MustCompile(`\\"exp\\":(\d+)`)
	classRegex := regexp.MustCompile(`"hidden text\-sm sm:inline\\",\\"children\\":\\"([^"]+)\\"`)

	rankMatches := rankRegex.FindAllStringSubmatch(bodyContent, -1)
	nameMatches := nameRegex.FindAllStringSubmatch(bodyContent, -1)
	baseLevelMatches := baseLevelRegex.FindAllStringSubmatch(bodyContent, -1)
	jobLevelMatches := jobLevelRegex.FindAllStringSubmatch(bodyContent, -1)
	expMatches := expRegex.FindAllStringSubmatch(bodyContent, -1)
	classMatches := classRegex.FindAllStringSubmatch(bodyContent, -1)

	numChars := len(nameMatches)
	if numChars == 0 {
		return nil, fmt.Errorf("page %d returned 0 characters on parse attempt %d/%d", pageIndex, attempt, maxParseRetries)
	}

	if len(rankMatches) != numChars || len(baseLevelMatches) != numChars || len(jobLevelMatches) != numChars || len(expMatches) != numChars || len(classMatches) != numChars {
		log.Printf("[W] [Scraper/Char] Mismatch in regex match counts on page %d. Skipping page. (Ranks: %d, Names: %d, Classes: %d)", pageIndex, len(rankMatches), len(nameMatches), len(classMatches))
		return nil, nil // Data integrity issue, don't retry, just return no players
	}

	pagePlayers := make([]PlayerCharacter, 0, numChars)
	for i := 0; i < numChars; i++ {
		rank, _ := strconv.Atoi(rankMatches[i][1])
		name := nameMatches[i][1]
		baseLevel, _ := strconv.Atoi(baseLevelMatches[i][1])
		jobLevel, _ := strconv.Atoi(jobLevelMatches[i][1])
		rawExp, _ := strconv.ParseFloat(expMatches[i][1], 64)
		class := classMatches[i][1]

		pagePlayers = append(pagePlayers, PlayerCharacter{
			Rank:       rank,
			BaseLevel:  baseLevel,
			JobLevel:   jobLevel,
			Experience: rawExp / 1000000.0,
			Class:      class,
			Name:       name,
		})
	}
	return pagePlayers, nil
}

// processGuildData handles all database transactions for updating guilds and member associations.
func processGuildData(allGuilds map[string]Guild, allMembers map[string]string) {
	characterMutex.Lock()
	defer characterMutex.Unlock()

	// 1. Fetch old associations for comparison
	oldAssociations := make(map[string]string)
	oldGuildRows, err := db.Query("SELECT name, guild_name FROM characters WHERE guild_name IS NOT NULL")
	if err != nil {
		log.Printf("[W] [Scraper/Guild] Could not fetch old guild associations for comparison: %v", err)
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
		log.Printf("[E] [Scraper/Guild] Failed to begin transaction for guilds update: %v", errDb)
		return
	}
	defer tx.Rollback()

	// 3. Upsert guild information
	log.Println("[D] [Scraper/Guild] Upserting guild information into 'guilds' table...")
	guildStmt, err := tx.Prepare(`
		INSERT INTO guilds (rank, name, level, experience, master, emblem_url, last_updated)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			level=excluded.level,
			master=excluded.master,
			last_updated=excluded.last_updated
	`)
	if err != nil {
		log.Printf("[E] [Scraper/Guild] Failed to prepare guilds upsert statement: %v", err)
		return
	}
	defer guildStmt.Close()

	updateTime := time.Now().Format(time.RFC3339)
	for _, g := range allGuilds {
		if _, err := guildStmt.Exec(0, g.Name, g.Level, g.Experience, g.Master, g.EmblemURL, updateTime); err != nil {
			log.Printf("[W] [Scraper/Guild] Failed to upsert guild '%s': %v", g.Name, err)
		}
	}

	// 4. Update character associations
	log.Printf("[D] [Scraper/Guild] Updating 'characters' table with guild associations for %d members...", len(allMembers))
	if _, err := tx.Exec("UPDATE characters SET guild_name = NULL"); err != nil {
		log.Printf("[E] [Scraper/Guild] Failed to clear existing guild names from characters table: %v", err)
		return
	}

	charStmt, err := tx.Prepare("UPDATE characters SET guild_name = ? WHERE name = ?")
	if err != nil {
		log.Printf("[E] [Scraper/Guild] Failed to prepare character guild update statement: %v", err)
		return
	}
	defer charStmt.Close()

	updateCount := 0
	for charName, guildName := range allMembers {
		res, err := charStmt.Exec(guildName, charName)
		if err != nil {
			log.Printf("[W] [Scraper/Guild] Failed to update guild for character '%s': %v", charName, err)
		} else if n, _ := res.RowsAffected(); n > 0 {
			updateCount++
		}
	}
	log.Printf("[D] [Scraper/Guild] Successfully associated %d characters with their guilds.", updateCount)

	// 5. Log guild changes
	changelogStmt, err := tx.Prepare(`
		INSERT INTO character_changelog (character_name, change_time, activity_description)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		log.Printf("[E] [Scraper/Guild] Failed to prepare changelog statement for guilds: %v", err)
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
		log.Printf("[E] [Scraper/Guild] Failed to commit guilds and characters transaction: %v", err)
		return
	}

	log.Printf("[I] [Scraper/Guild] Scrape and update complete. Saved %d guild records and updated character associations.", len(allGuilds))
}

// scrapeGuilds is the concurrent "producer" for guild data.
func scrapeGuilds() {
	log.Println("[I] [Scraper/Guild] Starting guild and character-guild association scrape...")

	const firstPageURL = "https://projetoyufa.com/rankings/guild?page=1"
	lastPage := scraperClient.findLastPage(firstPageURL, "[Guilds]")

	// These maps are safe for concurrent writes because each goroutine
	// writes to a *different* key (guild name / member name).
	// For this specific use case, a mutex is sufficient.
	allGuilds := make(map[string]Guild)
	allMembers := make(map[string]string) // Map[characterName]guildName
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	log.Printf("[I] [Scraper/Guild] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }()

			var pageGuilds []Guild
			var pageMembers map[string]string

			for attempt := 1; attempt <= maxParseRetries; attempt++ {
				url := fmt.Sprintf("https://projetoyufa.com/rankings/guild?page=%d", pageIndex)
				bodyContent, err := scraperClient.getPage(url, "[Guilds]")
				if err != nil {
					log.Printf("[E] [Scraper/Guild] Network/HTTP error for page %d (attempt %d/%d): %v. Retrying...", pageIndex, attempt, maxParseRetries, err)
					time.Sleep(parseRetryDelay)
					continue
				}

				pageGuilds, pageMembers, err = parseGuildPage(bodyContent, pageIndex, attempt)
				if err != nil {
					log.Printf("[W] [Scraper/Guild] %v. Retrying...", err)
					time.Sleep(parseRetryDelay)
					continue
				}

				// Success
				break
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
				log.Printf("[D] [Scraper/Guild] Scraped page %d/%d, found %d guilds.", pageIndex, lastPage, len(pageGuilds))
			} else {
				log.Printf("[E] [Scraper/Guild] Failed to scrape page %d/%d after all retries.", pageIndex, lastPage)
			}
		}(page)
	}
	wg.Wait()

	log.Printf("[I] [Scraper/Guild] Finished scraping all pages. Found %d unique guilds.", len(allGuilds))
	if len(allGuilds) == 0 {
		log.Println("[W] [Scraper/Guild] Scrape finished with 0 total guilds found. Guild/character tables will not be updated.")
		return
	}

	// Call the dedicated database function
	processGuildData(allGuilds, allMembers)
}

// parseGuildPage contains all the parsing logic for a guild page.
func parseGuildPage(bodyContent string, pageIndex, attempt int) ([]Guild, map[string]string, error) {
	nameRegex := regexp.MustCompile(`<span class="font-medium">([^<]+)</span>`)
	levelRegex := regexp.MustCompile(`\\"guild_lv\\":(\d+),\\"connect_member\\"`)
	masterRegex := regexp.MustCompile(`\\"master\\":\\"([^"]+)\\",\\"members\\"`)
	membersRegex := regexp.MustCompile(`\\"members\\":\[(.*?)\]\}`)
	memberNameRegex := regexp.MustCompile(`\\"name\\":\\"([^"]+)\\",\\"base_level\\"`)

	nameMatches := nameRegex.FindAllStringSubmatch(bodyContent, -1)
	levelMatches := levelRegex.FindAllStringSubmatch(bodyContent, -1)
	masterMatches := masterRegex.FindAllStringSubmatch(bodyContent, -1)
	membersMatches := membersRegex.FindAllStringSubmatch(bodyContent, -1)

	numGuilds := len(nameMatches)
	if numGuilds == 0 {
		return nil, nil, fmt.Errorf("page %d returned 0 guilds on parse attempt %d/%d", pageIndex, attempt, maxParseRetries)
	}

	if len(levelMatches) != numGuilds || len(masterMatches) != numGuilds || len(membersMatches) != numGuilds {
		log.Printf("[W] [Scraper/Guild] Mismatch in regex match counts on page %d. Skipping page. (Names: %d, Levels: %d, Masters: %d, Members: %d)",
			pageIndex, len(nameMatches), len(levelMatches), len(masterMatches), len(membersMatches))
		return nil, nil, nil // Data integrity issue, don't retry
	}

	pageGuilds := make([]Guild, 0, numGuilds)
	pageMembers := make(map[string]string)

	for i := 0; i < numGuilds; i++ {
		name := nameMatches[i][1]
		level, _ := strconv.Atoi(levelMatches[i][1])
		master := masterMatches[i][1]

		pageGuilds = append(pageGuilds, Guild{Name: name, Level: level, Master: master})

		members := memberNameRegex.FindAllStringSubmatch(membersMatches[i][1], -1)
		for _, member := range members {
			pageMembers[member[1]] = name // charName -> guildName
		}
	}
	return pageGuilds, pageMembers, nil
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
// --- MODIFICATION ---
func processZenyData(allZenyInfo map[string]int64) {
	// --- END MODIFICATION ---
	characterMutex.Lock()
	defer characterMutex.Unlock()

	// --- MODIFICATION ---
	// Timestamp is generated *after* scraping is complete.
	updateTime := time.Now().Format(time.RFC3339)
	// --- END MODIFICATION ---

	log.Println("[D] [Scraper/Zeny] Fetching existing character zeny data for activity comparison...")
	existingCharacters := make(map[string]CharacterZenyInfo)
	rows, err := db.Query("SELECT name, zeny, last_active FROM characters")
	if err != nil {
		log.Printf("[E] [Scraper/Zeny] Failed to query existing characters for comparison: %v", err)
		// Continue with an empty map
	} else {
		defer rows.Close()
		for rows.Next() {
			var name string
			var info CharacterZenyInfo
			if err := rows.Scan(&name, &info.Zeny, &info.LastActive); err != nil {
				log.Printf("[W] [Scraper/Zeny] Failed to scan existing zeny row: %v", err)
				continue
			}
			existingCharacters[name] = info
		}
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("[E] [Scraper/Zeny] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("UPDATE characters SET zeny = ?, last_active = ? WHERE name = ?")
	if err != nil {
		log.Printf("[E... ] [Scraper/Zeny] Failed to prepare update statement: %v", err)
		return
	}
	defer stmt.Close()

	changelogStmt, err := tx.Prepare(`
		INSERT INTO character_changelog (character_name, change_time, activity_description)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		log.Printf("[E] [Scraper/Zeny] Failed to prepare changelog statement for zeny: %v", err)
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
			log.Printf("[W] [Scraper/Zeny] Failed to update zeny for '%s': %v", name, err)
			continue
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			updatedCount++
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[E] [Scraper/Zeny] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("[I] [Scraper/Zeny] Database update complete. Updated activity for %d characters. %d characters were unchanged.", updatedCount, unchangedCount)
}

// scrapeZeny is now only responsible for concurrent scraping.
func scrapeZeny() {
	log.Println("[I] [Scraper/Zeny] Starting Zeny ranking scrape...")

	const firstPageURL = "https://projetoyufa.com/rankings/zeny?page=1"
	lastPage := scraperClient.findLastPage(firstPageURL, "[Scraper/Zeny]")

	allZenyInfo := make(map[string]int64) // Map[characterName]zeny
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // Concurrency semaphore

	log.Printf("[I] [Scraper/Zeny] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }()

			var bodyContent string
			var err error
			var numRows int

			// --- MODIFICATION: Added retry loop for parsing ---
			for attempt := 1; attempt <= maxParseRetries; attempt++ {
				url := fmt.Sprintf("https://projetoyufa.com/rankings/zeny?page=%d", pageIndex)
				bodyContent, err = scraperClient.getPage(url, "[Scraper/Zeny]")
				if err != nil {
					log.Printf("[E] [Scraper/Zeny] Network/HTTP error for page %d (attempt %d/%d): %v. Retrying...", pageIndex, attempt, maxParseRetries, err)
					time.Sleep(parseRetryDelay)
					continue
				}

				doc, err := goquery.NewDocumentFromReader(strings.NewReader(bodyContent))
				if err != nil {
					log.Printf("[E] [Scraper/Zeny] Failed to parse body for page %d: %v", pageIndex, err)
					// Don't retry on parse error, just fail this page
					numRows = -1 // Mark as failed
					break
				}

				rows := doc.Find("table tbody tr")
				numRows = rows.Length()

				if numRows == 0 {
					// --- THIS IS THE NEW LOGIC ---
					log.Printf("[W] [Scraper/Zeny] Page %d returned 0 zeny rows on parse attempt %d/%d. Retrying...", pageIndex, attempt, maxParseRetries)
					time.Sleep(parseRetryDelay)
					continue // Try fetching and parsing again
				}

				rows.Each(func(i int, s *goquery.Selection) {
					cells := s.Find("td")
					if cells.Length() < 3 {
						if enableZenyScraperDebugLogs {
							log.Printf("[D] [Scraper/Zeny] Skipping row on page %d, expected >= 3 cells, got %d", pageIndex, cells.Length())
						}
						return
					}

					nameStr := strings.TrimSpace(cells.Eq(1).Text())
					zenyStrRaw := strings.TrimSpace(cells.Eq(2).Text())

					zenyStrClean := strings.ReplaceAll(zenyStrRaw, ",", "")
					zenyStrClean = strings.TrimSuffix(zenyStrClean, "z")
					zenyStrClean = strings.TrimSpace(zenyStrClean)

					if nameStr == "" {
						if enableZenyScraperDebugLogs {
							log.Printf("[D] [Scraper/Zeny] Skipping row on page %d, extracted name is empty.", pageIndex)
						}
						return
					}

					zenyVal, err := strconv.ParseInt(zenyStrClean, 10, 64)
					if err != nil {
						log.Printf("[W] [Scraper/Zeny] Could not parse zeny value '%s' (from raw '%s') for player '%s'", zenyStrClean, zenyStrRaw, nameStr)
						return
					}

					mu.Lock()
					allZenyInfo[nameStr] = zenyVal
					mu.Unlock()
				})
				// --- End parsing logic ---

				// Success, break from retry loop
				break
			}
			// --- END MODIFICATION ---

			// Log final status for this page
			if numRows > 0 {
				log.Printf("[D] [Scraper/Zeny] Scraped page %d/%d successfully.", pageIndex, lastPage)
			} else if numRows == 0 {
				log.Printf("[E] [Scraper/Zeny] Failed to scrape page %d/%d after all retries.", pageIndex, lastPage)
			} // (numRows == -1 was logged already)
		}(page)
	}

	wg.Wait()
	log.Printf("[I] [Scraper/Zeny] Finished scraping all pages. Found zeny info for %d characters.", len(allZenyInfo))

	if len(allZenyInfo) == 0 {
		log.Println("[W] [Scraper/Zeny] No zeny information was scraped. Skipping database update.")
		return
	}

	// Call the dedicated database function
	processZenyData(allZenyInfo)
}

// in scraper.go

// parseMarketItem extracts all item details from a goquery selection.
// This helper function isolates the complex name-parsing logic from the main scraper loop.
func parseMarketItem(itemSelection *goquery.Selection) (Item, bool) {
	// --- Regex for +Refine levels ---
	reRefineMid := regexp.MustCompile(`\s(\+\d+)`)    // e.g., "Item +7 Name"
	reRefineStart := regexp.MustCompile(`^(\+\d+)\s`) // e.g., "+7 Item Name"

	// --- 1. Get Base Name ---
	baseItemName := strings.TrimSpace(itemSelection.Find("p.font-medium.truncate").Text()) // Guess: added font-medium
	if baseItemName == "" {
		baseItemName = strings.TrimSpace(itemSelection.Find("p.truncate").Text()) // Fallback to old
	}
	if baseItemName == "" {
		return Item{}, false // No name, invalid item
	}

	// --- 2. Normalize Refinements (unchanged) ---
	if match := reRefineMid.FindStringSubmatch(baseItemName); len(match) > 1 && !strings.HasSuffix(baseItemName, match[0]) {
		cleanedName := strings.Replace(baseItemName, match[0], "", 1)
		cleanedName = strings.Join(strings.Fields(cleanedName), " ") // Remove extra spaces
		baseItemName = cleanedName + match[0]
	} else if match := reRefineStart.FindStringSubmatch(baseItemName); len(match) > 1 {
		cleanedName := strings.Replace(baseItemName, match[0], "", 1)
		cleanedName = strings.Join(strings.Fields(cleanedName), " ") // Remove extra spaces
		baseItemName = cleanedName + " " + match[1]
	}

	// --- 3. Extract ID and Card Names (NEW FIX) ---
	var cardNames []string
	idStr := ""

	// Find all elements matching the "badge" structure you provided.
	itemSelection.Find("div.inline-flex.items-center.rounded-full.border.font-semibold").Each(func(k int, sel *goquery.Selection) {
		badgeText := strings.TrimSpace(sel.Text())
		if badgeText == "" {
			return
		}

		if strings.HasPrefix(badgeText, "ID: ") {
			// It's the ID.
			// Text might be "ID: 1095"
			idStrWithJunk := strings.TrimPrefix(badgeText, "ID: ")
			idStr = strings.Fields(idStrWithJunk)[0] // Get just the first part
		} else {
			// It's not an ID, so it must be a card or refinement.
			// We'll append it to the name.
			cardName := strings.TrimSpace(strings.TrimSuffix(badgeText, " Card"))
			if cardName != "" {
				cardNames = append(cardNames, cardName)
			}
		}
	})
	// --- END NEW FIX ---

	// --- 4. Finalize Name ---
	finalItemName := baseItemName
	if len(cardNames) > 0 {
		wrapped := make([]string, len(cardNames))
		for i, c := range cardNames {
			wrapped[i] = fmt.Sprintf(" [%s]", c)
		}
		// We join and then clean up, just in case the base name
		// already had a refinement that was *also* picked up as a badge.
		// e.g., "Manteau [1] +7" + " [+7]" becomes "Manteau [1] +7 [+7]"
		// This is hard to de-duplicate, but appending is safer.
		finalItemName = fmt.Sprintf("%s%s", baseItemName, strings.Join(wrapped, ""))
	}

	// --- 5. Get Other Details ---
	quantityStr := strings.TrimSuffix(strings.TrimSpace(itemSelection.Find("span[class*='text-muted-foreground']").Text()), "x")
	priceStr := strings.TrimSpace(itemSelection.Find("span[class*='text-green']").Text())

	if priceStr == "" {
		priceStr = strings.TrimSpace(itemSelection.Find("span.text-xs.font-medium.text-green-600").Text())
	}
	if priceStr == "" {
		return Item{}, false // No price, invalid item
	}

	quantity, _ := strconv.Atoi(quantityStr)
	if quantity == 0 {
		quantity = 1 // Default to 1 if parsing fails or 0
	}
	itemID, _ := strconv.Atoi(idStr) // idStr is now correctly populated

	return Item{
		Name:     finalItemName,
		ItemID:   itemID,
		Quantity: quantity,
		Price:    priceStr,
	}, true
}

// in scraper.go

// determineRemovalType encapsulates the logic for deciding if an item was sold or just removed.
func determineRemovalType(listing Item, activeStores map[string]bool, dbStoreSizes map[string]int) string {
	// --- END MODIFICATION ---

	// --- MODIFICATION: Check if the specific store is still active ---
	// Create a unique key for the store
	storeKey := listing.SellerName + "::" + listing.StoreName
	if _, storeIsActive := activeStores[storeKey]; storeIsActive {
		// If the specific store is still online, assume the item was sold.
		return "SOLD"
	}
	// --- END MODIFICATION ---

	// If the store is offline, check if it was the only item.
	// This logic remains the same, but now only triggers if the store itself is gone.
	if dbStoreSizes[listing.SellerName] == 1 {
		return "REMOVED_SINGLE"
	}

	// Otherwise, the seller is offline (or this specific store is closed)
	// and they had other items. We assume they just removed this item or closed this one store.
	return "REMOVED"
}

func scrapeData() {
	log.Println("[I] [Scraper/Market] Starting scrape...")

	const requestURL = "https://projetoyufa.com/market"

	var htmlContent string
	var err error
	var doc *goquery.Document
	scrapedItemsByName := make(map[string][]Item)

	// --- MODIFICATION: This is now a map of active *stores* ---
	// The key will be "SellerName::StoreName"
	activeStores := make(map[string]bool)
	// --- END MODIFICATION ---

	// --- MODIFICATION: Added retry loop for parsing ---
	for attempt := 1; attempt <= maxParseRetries; attempt++ {
		// Use the new helper function. All the retry logic is now encapsulated.
		htmlContent, err = scraperClient.getPage(requestURL, "[Scraper/Market]")
		if err != nil {
			log.Printf("[E] [Scraper/Market] Network/HTTP error (attempt %d/%d): %v. Retrying...", attempt, maxParseRetries, err)
			time.Sleep(parseRetryDelay)
			continue
		}

		doc, err = goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
		if err != nil {
			log.Printf("[E] [Scraper/Market] Failed to parse HTML: %v", err)
			// This is a critical error, don't retry
			return
		}

		// Reset maps for this attempt
		scrapedItemsByName = make(map[string][]Item)
		// --- MODIFICATION: Reset activeStores map ---
		activeStores = make(map[string]bool)
		// --- END MODIFICATION ---

		// --- Main Scraper Loop ---
		doc.Find("svg.lucide-user").Each(func(i int, s *goquery.Selection) {
			card := s.Closest("div.border")
			if card.Length() == 0 {
				card = s.Closest("article")
				if card.Length() == 0 {
					log.Printf("[W] [Scraper/Market] Could not find parent 'card' for a lucide-user icon. Skipping shop.")
					return
				}
			}

			shopName := strings.TrimSpace(card.Find("div.font-semibold.tracking-tight.line-clamp-1.text-lg").First().Text())
			if shopName == "" {
				shopName = strings.TrimSpace(card.Find("div.font-semibold").First().Text())
			}
			if shopName == "" {
				shopName = strings.TrimSpace(card.Find("h3").First().Text())
			}
			if shopName == "" {
				if enableMarketScraperDebugLogs {
					log.Printf("[D] [Scraper/Market] Shop name is empty. Renaming to '(empty)'.")
				}
				shopName = "(empty)"
			}

			sellerName := strings.TrimSpace(s.Next().Text())
			mapName := strings.TrimSpace(card.Find("svg.lucide-map-pin").Next().Text())
			mapCoordinates := strings.TrimSpace(card.Find("svg.lucide-copy").Next().Text())

			// --- MODIFICATION: Use the new storeKey to track active stores ---
			if sellerName != "" {
				storeKey := sellerName + "::" + shopName
				activeStores[storeKey] = true
			}
			// --- END MODIFICATION ---

			if enableMarketScraperDebugLogs == true {
				log.Printf("[D] [Scraper/Market] shop name: %s, seller name: %s, map_name: %s, mapcoord: %s", shopName, sellerName, mapName, mapCoordinates)
			}

			if sellerName == "" {
				log.Printf("[W] [Scraper/Market] Skipping shop with missing seller name (Shop: '%s').", shopName)
				return
			}

			card.Find(".flex.items-center.space-x-2").Each(func(j int, itemSelection *goquery.Selection) {
				item, ok := parseMarketItem(itemSelection)
				if !ok {
					return
				}

				item.StoreName = shopName
				item.SellerName = sellerName
				item.MapName = mapName
				item.MapCoordinates = mapCoordinates

				if enableMarketScraperDebugLogs == true {
					log.Printf("[D] [Scraper/Market] name: %s, id: %d, qtd: %d price %s store: %s seller: %s map: %s coord %s", item.Name, item.ItemID, item.Quantity, item.Price, shopName, sellerName, mapName, mapCoordinates)
				}

				scrapedItemsByName[item.Name] = append(scrapedItemsByName[item.Name], item)
			})
		})

		if len(scrapedItemsByName) == 0 {
			// --- THIS IS THE NEW LOGIC ---
			log.Printf("[W] [Scraper/Market] Market page returned 0 items on parse attempt %d/%d. Retrying...", attempt, maxParseRetries)
			time.Sleep(parseRetryDelay)
			continue // Try fetching and parsing again
		}

		// Success, break from retry loop
		break
	}
	// --- END MODIFICATION ---

	log.Printf("[I] [Scraper/Market] Scrape parsed. Found %d unique item names.", len(scrapedItemsByName))

	if len(scrapedItemsByName) == 0 {
		log.Println("[W] [Scraper/Market] Scraper found 0 items on the market page after all retries. This might be a parsing error or an empty market. Skipping this update cycle to avoid wiping data.")
		return // Abort the function here
	}

	retrievalTime := time.Now().Format(time.RFC3339) // Get time *after* successful parse

	marketMutex.Lock()
	defer marketMutex.Unlock()

	tx, err := db.Begin()
	if err != nil {
		log.Printf("[E] [Scraper/Market] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// --- Database logic from here down ---

	_, err = tx.Exec("INSERT OR IGNORE INTO scrape_history (timestamp) VALUES (?)", retrievalTime)
	if err != nil {
		log.Printf("[E] [Scraper/Market] Failed to log scrape history: %v", err)
		return
	}

	// --- MODIFICATION: This query now pre-loads store sizes by *seller* ---
	// We still need this for the REMOVED_SINGLE check.
	dbStoreSizes := make(map[string]int)
	sellerItems := make(map[string]map[string]bool)
	rows, err := tx.Query("SELECT seller_name, name_of_the_item FROM items WHERE is_available = 1")
	if err != nil {
		log.Printf("[E] [Scraper/Market] Could not pre-query seller item counts: %v", err)
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
	// --- END MODIFICATION ---

	dbAvailableItemsMap := make(map[string][]Item)
	dbAvailableNames := make(map[string]bool)

	rows, err = tx.Query("SELECT name_of_the_item, item_id, quantity, price, store_name, seller_name, map_name, map_coordinates FROM items WHERE is_available = 1")
	if err != nil {
		log.Printf("[E] [Scraper/Market] Could not get list of all available items: %v", err)
		return
	}
	for rows.Next() {
		var item Item
		err := rows.Scan(&item.Name, &item.ItemID, &item.Quantity, &item.Price, &item.StoreName, &item.SellerName, &item.MapName, &item.MapCoordinates)
		if err != nil {
			log.Printf("[W] [Scraper/Market] Failed to scan existing item: %v", err)
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

					// --- REFACTOR ---
					// Use the new helper function, passing the new activeStores map
					eventType := determineRemovalType(lastItem, activeStores, dbStoreSizes)
					// --- END REFACTOR ---

					details, _ := json.Marshal(map[string]interface{}{
						"price":      lastItem.Price,
						"quantity":   lastItem.Quantity,
						"seller":     lastItem.SellerName,
						"store_name": lastItem.StoreName,
					})
					_, err := tx.Exec(`INSERT INTO market_events (event_timestamp, event_type, item_name, item_id, details) VALUES (?, ?, ?, ?, ?)`, retrievalTime, eventType, lastItem.Name, lastItem.ItemID, string(details))
					if err != nil {
						log.Printf("[E] [Scraper/Market] Failed to log %s event for %s: %v", eventType, lastItem.Name, err)
					}
				}
			}
		}

		if areItemSetsIdentical(currentScrapedItems, lastAvailableItems) {
			itemsUnchanged++
			continue
		}

		if _, err := tx.Exec("UPDATE items SET is_available = 0 WHERE name_of_the_item = ?", itemName); err != nil {
			log.Printf("[E] [Scraper/Market] Failed to mark old %s as unavailable: %v", itemName, err)
			continue
		}

		insertSQL := `INSERT INTO items(name_of_the_item, item_id, quantity, price, store_name, seller_name, date_and_time_retrieved, map_name, map_coordinates, is_available) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			log.Printf("[W] [Scraper/Market] Could not prepare insert for %s: %v", itemName, err)
			continue
		}
		for _, item := range currentScrapedItems {
			if _, err := stmt.Exec(item.Name, item.ItemID, item.Quantity, item.Price, item.StoreName, item.SellerName, retrievalTime, item.MapName, item.MapCoordinates); err != nil {
				log.Printf("[WF] [Scraper/Market] Could not execute insert for %s: %v", item.Name, err)
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
					log.Printf("[E] [Scraper/Market] Failed to log ADDED event for %s: %v", itemName, err)
				}
			}

			var historicalLowestPrice sql.NullInt64
			err := tx.QueryRow(`SELECT MIN(CAST(REPLACE(price, ',', '') AS INTEGER)) FROM items WHERE name_of_the_item = ?`, itemName).Scan(&historicalLowestPrice)
			if err != nil && err != sql.ErrNoRows {
				log.Printf("[W] [Scraper/Market] Could not get historical lowest price for %s: %v", itemName, err)
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
					log.Printf("[E] [Scraper/Market] Failed to log NEW_LOW event for %s: %v", itemName, err)
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

				// --- REFACTOR ---
				// Use the new helper function, passing the new activeStores map
				eventType := determineRemovalType(listing, activeStores, dbStoreSizes)
				// --- END REFACTOR ---

				details, _ := json.Marshal(map[string]interface{}{
					"price":      listing.Price,
					"quantity":   listing.Quantity,
					"seller":     listing.SellerName,
					"store_name": listing.StoreName,
				})
				_, err = tx.Exec(`INSERT INTO market_events (event_timestamp, event_type, item_name, item_id, details) VALUES (?, ?, ?, ?, ?)`, retrievalTime, eventType, name, listing.ItemID, string(details))
				if err != nil {
					log.Printf("[E] [Scraper/Market] Failed to log %s event for removed item %s: %v", eventType, name, err)
				}
			}

			if _, err := tx.Exec("UPDATE items SET is_available = 0 WHERE name_of_the_item = ?", name); err != nil {
				log.Printf("[E] [Scraper/Market] Failed to mark disappeared item %s as unavailable: %v", name, err)
			} else {
				itemsRemoved++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[E] [Scraper/Market] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("[I] [Scraper/Market] Scrape complete. Unchanged: %d groups. Updated: %d groups. Newly Added: %d groups. Removed: %d groups.", itemsUnchanged, itemsUpdated, itemsAdded, itemsRemoved)
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
			log.Printf("[W] [Scraper/MVP] Failed to scan existing MVP row: %v", err)
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
		log.Printf("[E] [Scraper/MVP] %v. Aborting update.", err)
		return
	}

	allExistingKills, err := fetchExistingMvpKills()
	if err != nil {
		log.Printf("[E] [Scraper/MVP] %v. Aborting update.", err)
		return
	}

	// 2. Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Printf("[E] [Scraper/MVP] Failed to begin transaction: %v", err)
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
		log.Printf("[E] [Scraper/MVP] Failed to prepare MVP kills upsert statement: %v", err)
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
			log.Printf("[W] [Scraper/MVP] Failed to upsert MVP kills for player %s: %v", charName, err)
		}
	}

	// 5. Commit
	if err := tx.Commit(); err != nil {
		log.Printf("[E] [Scraper/MVP] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("[I] [Scraper/MVP] Saved/updated MVP kill records for %d characters.", updateCount)
	log.Printf("[I] [Scraper/MVP] Scrape and update process complete.")
}

// WoEEventStats holds summary info about a WoE event for season detection.
type WoEEventStats struct {
	EventID        int64
	SeasonID       int64
	TotalKills     int64
	TotalDamage    int64
	CharacterCount int
}

// getLastWoeEventStats retrieves the stats for the most recent WoE event from the DB.
func getLastWoeEventStats(tx *sql.Tx) (WoEEventStats, error) {
	var stats WoEEventStats
	var latestEventID sql.NullInt64
	var latestSeasonID sql.NullInt64

	// Find the most recent event
	err := tx.QueryRow(`
		SELECT event_id, season_id
		FROM woe_events
		ORDER BY event_date DESC
		LIMIT 1
	`).Scan(&latestEventID, &latestSeasonID)

	if err != nil {
		if err == sql.ErrNoRows {
			// No events ever recorded
			return stats, nil
		}
		return stats, fmt.Errorf("failed to query latest event: %w", err)
	}

	if !latestEventID.Valid {
		// Should not happen if a row was found, but good to check
		return stats, nil
	}

	stats.EventID = latestEventID.Int64
	stats.SeasonID = latestSeasonID.Int64 // Will be 0 if not set, which is fine

	// Get the summary stats for that event
	err = tx.QueryRow(`
		SELECT SUM(kill_count), SUM(damage_done), COUNT(character_name)
		FROM woe_event_rankings
		WHERE event_id = ?
	`, stats.EventID).Scan(&stats.TotalKills, &stats.TotalDamage, &stats.CharacterCount)

	if err != nil {
		return stats, fmt.Errorf("failed to query stats for event %d: %w", stats.EventID, err)
	}

	return stats, nil
}

// --- END NEW HELPER ---

// processWoeCharacterData handles saving the scraped WoE rankings to the database,
// now with season detection logic.
func processWoeCharacterData(allWoeChars map[string]WoeCharacterRank) {
	characterMutex.Lock() // Using characterMutex as WoE data relates to characters
	defer characterMutex.Unlock()

	eventTime := time.Now().Format(time.RFC3339)

	log.Println("[D] [Scraper/WoE] Starting database update for new WoE Event...")

	if len(allWoeChars) == 0 {
		log.Println("[W] [Scraper/WoE] Scraped 0 WoE characters. Aborting event save.")
		return
	}

	// Calculate stats for the *newly scraped* data
	var currentTotalKills, currentTotalDamage int64
	var currentCharacterCount int
	for _, char := range allWoeChars {
		currentTotalKills += int64(char.KillCount)
		currentTotalDamage += char.DamageDone
		currentCharacterCount++
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("[E] [Scraper/WoE] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	// 1. Get stats from the *last* saved event
	lastEventStats, err := getLastWoeEventStats(tx)
	if err != nil {
		log.Printf("[E] [Scraper/WoE] Could not get last event stats: %v", err)
		return // Don't proceed if we can't check
	}

	// 2. Season Detection Logic
	currentSeasonID := lastEventStats.SeasonID
	isNewSeason := false

	if lastEventStats.EventID == 0 {
		// This is the first WoE event ever recorded. It must be a new season.
		log.Println("[I] [Scraper/WoE] No previous WoE event found. Starting new season.")
		isNewSeason = true
	} else {
		// Compare current stats to last saved stats to detect a reset
		// A "reset" means significantly fewer kills OR damage than the previous summary.
		// We add a character count check to avoid false positives if only 1 player joined.
		if (currentTotalKills < lastEventStats.TotalKills || currentTotalDamage < lastEventStats.TotalDamage) &&
			(currentCharacterCount > 10 && lastEventStats.CharacterCount > 10) {

			log.Printf("[I] [Scraper/WoE] WoE data reset detected! (Last Kills: %d, New Kills: %d). Starting new season.", lastEventStats.TotalKills, currentTotalKills)
			isNewSeason = true
		} else {
			if enableWoeScraperDebugLogs {
				log.Printf("[D] [Scraper/WoE] No data reset detected. (Last Kills: %d, New Kills: %d). Continuing season %d.", lastEventStats.TotalKills, currentTotalKills, currentSeasonID)
			}
		}
	}

	// 3. Create new Season if needed
	if isNewSeason || currentSeasonID == 0 {
		res, err := tx.Exec(`INSERT INTO woe_seasons (start_date) VALUES (?)`, eventTime)
		if err != nil {
			log.Printf("[E] [Scraper/WoE] Failed to create new woe_season entry: %v", err)
			return
		}
		newSeasonID, err := res.LastInsertId()
		if err != nil {
			log.Printf("[E] [Scraper/WoE] Failed to get new season_id: %v", err)
			return
		}
		currentSeasonID = newSeasonID
		log.Printf("[I] [Scraper/WoE] Created new WoE Season with ID: %d", currentSeasonID)
	}

	// 4. Create the new WoE Event entry, linked to the season
	res, err := tx.Exec(`INSERT INTO woe_events (season_id, event_date, is_season_summary) VALUES (?, ?, 0)`, currentSeasonID, eventTime)
	if err != nil {
		log.Printf("[E] [Scraper/WoE] Failed to create new woe_event entry: %v", err)
		return
	}

	newEventID, err := res.LastInsertId()
	if err != nil {
		log.Printf("[E] [Scraper/WoE] Failed to get new event_id: %v", err)
		return
	}

	if enableWoeScraperDebugLogs {
		log.Printf("[D] [Scraper/WoE] Created new WoE event (ID: %d) for season %d", newEventID, currentSeasonID)
	}

	// 5. Prepare statement for inserting the rankings for this event
	stmt, err := tx.Prepare(`
		INSERT INTO woe_event_rankings (
			event_id, character_name, class, guild_id, guild_name,
			kill_count, death_count, damage_done, emperium_kill,
			healing_done, score, points
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		log.Printf("[E] [Scraper/WoE] Failed to prepare event rankings insert statement: %v", err)
		return
	}
	defer stmt.Close()

	updateCount := 0
	for charName, char := range allWoeChars {
		// 6. Insert each character's stats for this new event
		_, err := stmt.Exec(
			newEventID, charName, char.Class, char.GuildID, char.GuildName,
			char.KillCount, char.DeathCount, char.DamageDone, char.EmperiumKill,
			char.HealingDone, char.Score, char.Points,
		)
		if err != nil {
			log.Printf("[W] [Scraper/WoE] Failed to insert WoE data for char '%s' into event %d: %v", charName, newEventID, err)
			continue
		}
		updateCount++
	}

	// 7. Commit
	if err := tx.Commit(); err != nil {
		log.Printf("[E] [Scraper/WoE] Failed to commit transaction: %v", err)
		return
	}
	log.Printf("[I] [Scraper/WoE] Database update complete. Saved %d WoE character records for new event ID %d in season %d.", updateCount, newEventID, currentSeasonID)
}

// scrapeWoeCharacterRankings scrapes the WoE character rankings from the website using goquery, handling pagination.
// This function's logic remains the same, as it's the "producer".
// The "consumer" (processWoeCharacterData) has been changed.
func scrapeWoeCharacterRankings() {
	log.Println("[I] [Scraper/WoE] Starting WoE character ranking scrape...")

	const firstPageURL = "https://projetoyufa.com/rankings/woe?page=1" // Base URL for finding last page
	allWoeChars := make(map[string]WoeCharacterRank)                   // Map[characterName]WoeCharacterRank

	// --- Step 1: Find the last page ---
	lastPage := scraperClient.findLastPage(firstPageURL, "[Scraper/WoE]")
	log.Printf("[I] [Scraper/WoE] Determined total pages: %d", lastPage)
	// --- End Step 1 ---

	// --- Step 2: Fetch Character NAME mapping from the main 'characters' table ---
	log.Println("[D] [Scraper/WoE] Fetching existing Character Names from 'characters' table...")
	existingCharNames := make(map[string]bool) // Set of names
	rows, err := db.Query("SELECT name FROM characters")
	if err != nil {
		log.Printf("[E] [Scraper/WoE] Could not query characters table for Names: %v. Aborting WoE scrape.", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			existingCharNames[name] = true
		}
	}
	log.Printf("[D] [Scraper/WoE] Found %d characters in the main table.", len(existingCharNames))
	if len(existingCharNames) == 0 {
		log.Printf("[W] [Scraper/WoE] Main 'characters' table appears empty. Cannot link WoE stats. Aborting.")
		return
	}
	// --- End Step 2 ---

	// --- Step 3: Scrape all pages concurrently ---
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)                 // Concurrency semaphore
	var totalParsedCount, totalMatchedCount int32 // Use atomic or mutex for shared counters if needed, simple int32 for now
	var emptyPageEncountered atomic.Bool          // Flag for an empty page
	emptyPageEncountered.Store(false)             // Initialize to false

	log.Printf("[I] [Scraper/WoE] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore when done

			var bodyContent string
			var err error
			var numRows int

			// --- MODIFICATION: Added retry loop for parsing ---
			for attempt := 1; attempt <= maxParseRetries; attempt++ {
				pageURL := fmt.Sprintf("https://projetoyufa.com/rankings/woe?page=%d", pageIndex)
				if enableWoeScraperDebugLogs {
					log.Printf("[D] [Scraper/WoE] Fetching page: %s (Attempt %d/%d)", pageURL, attempt, maxParseRetries)
				}
				bodyContent, err = scraperClient.getPage(pageURL, "[Scraper/WoE]")
				if err != nil {
					log.Printf("[E] [Scraper/WoE] Network/HTTP error for page %d (attempt %d/%d): %v. Retrying...", pageIndex, attempt, maxParseRetries, err)
					time.Sleep(parseRetryDelay)
					continue
				}

				doc, err := goquery.NewDocumentFromReader(strings.NewReader(bodyContent))
				if err != nil {
					log.Printf("[E] [Scraper/WoE] Failed to parse WoE page %d HTML: %v", pageIndex, err)
					numRows = -1 // Mark as failed
					break        // Don't retry on parse error
				}

				rows := doc.Find("table tbody tr")
				numRows = rows.Length()

				if numRows == 0 {
					// --- THIS IS THE NEW LOGIC ---
					log.Printf("[W] [Scraper/WoE] Page %d returned 0 WoE rows on parse attempt %d/%d. Retrying...", pageIndex, attempt, maxParseRetries)
					emptyPageEncountered.Store(true) // Set flag in case *all* pages are empty
					time.Sleep(parseRetryDelay)
					continue // Try fetching and parsing again
				}

				// --- Found rows, clear the flag for this page and proceed ---
				emptyPageEncountered.Store(false)
				var pageParsedCount, pageMatchedCount int
				rows.Each(func(i int, s *goquery.Selection) { // Use the 'rows' variable
					cells := s.Find("td")
					if cells.Length() < 8 {
						if enableWoeScraperDebugLogs {
							log.Printf("[D] [Scraper/WoE] Skipping row %d on page %d, expected >= 8 cells, got %d", i, pageIndex, cells.Length())
						}
						return
					}

					var c WoeCharacterRank
					charCell := cells.Eq(1)
					scrapedName := strings.TrimSpace(charCell.Find("span.font-medium").Text())
					if scrapedName == "" {
						if enableWoeScraperDebugLogs {
							log.Printf("[D] [Scraper/WoE] Skipped row %d on page %d due to missing Name.", i, pageIndex)
						}
						return
					}

					_, foundInDB := existingCharNames[scrapedName]
					if !foundInDB {
						if enableWoeScraperDebugLogs {
							log.Printf("[D] [Scraper/WoE] Scraped char '%s' (page %d) not found in DB. Skipping.", scrapedName, pageIndex)
						}
						return
					}

					c.Name = scrapedName
					pageMatchedCount++

					imgSrc, _ := charCell.Find("img").Attr("src")
					classIDMatch := jobIconRegex.FindStringSubmatch(imgSrc)
					if len(classIDMatch) > 1 {
						classID := classIDMatch[1]
						if className, ok := jobIDToClassName[classID]; ok {
							c.Class = className
						} else {
							c.Class = "Unknown (" + classID + ")"
							log.Printf("[W] [Scraper/WoE] Unknown class ID '%s' found for char '%s' on page %d", classID, c.Name, pageIndex)
						}
					} else {
						c.Class = "Unknown"
					}

					guildCell := cells.Eq(2)
					guildName := strings.TrimSpace(guildCell.Find("div.inline-flex").Text())
					if guildName != "" && guildName != "N/A" {
						c.GuildName = sql.NullString{String: guildName, Valid: true}
						guildImgSrc, _ := guildCell.Find("img").Attr("src")
						guildIDStr := regexp.MustCompile(`/(\d+)$`).FindStringSubmatch(guildImgSrc)
						if len(guildIDStr) > 1 {
							if guildID, err := strconv.ParseInt(guildIDStr[1], 10, 64); err == nil {
								c.GuildID = sql.NullInt64{Int64: guildID, Valid: true}
							}
						}
					}

					kdCellText := cells.Eq(3).Text()
					kdParts := strings.Split(strings.Split(kdCellText, "(")[0], "/")
					if len(kdParts) == 2 {
						c.KillCount, _ = strconv.Atoi(strings.TrimSpace(kdParts[0]))
						c.DeathCount, _ = strconv.Atoi(strings.TrimSpace(kdParts[1]))
					}

					damageStr := strings.ReplaceAll(strings.TrimSpace(cells.Eq(4).Text()), ",", "")
					c.DamageDone, _ = strconv.ParseInt(damageStr, 10, 64)
					c.EmperiumKill, _ = strconv.Atoi(strings.TrimSpace(cells.Eq(5).Text()))
					healingStr := strings.ReplaceAll(strings.TrimSpace(cells.Eq(6).Text()), ",", "")
					c.HealingDone, _ = strconv.ParseInt(healingStr, 10, 64)
					c.Points, _ = strconv.Atoi(strings.TrimSpace(cells.Eq(7).Text()))
					c.Score = 0

					mu.Lock()
					allWoeChars[c.Name] = c
					mu.Unlock()
					pageParsedCount++
				}) // End .Each row

				// Update total counts
				mu.Lock()
				totalParsedCount += int32(pageParsedCount)
				totalMatchedCount += int32(pageMatchedCount)
				mu.Unlock()

				log.Printf("[D] [Scraper/WoE] Scraped page %d/%d. Matched %d chars from DB, parsed %d.", pageIndex, lastPage, pageMatchedCount, pageParsedCount)
				// --- End parsing logic ---

				// Success, break from retry loop
				break
			}
			// --- END MODIFICATION ---

			if numRows == 0 {
				log.Printf("[E] [Scraper/WoE] Failed to scrape page %d/%d after all retries.", pageIndex, lastPage)
			}
		}(page) // <-- *** THIS IS THE FIX (was pageIndex) ***
	} // End page loop

	wg.Wait() // Wait for all page scraping goroutines to finish
	// --- End Step 3 ---

	if emptyPageEncountered.Load() {
		log.Println("[W] [Scraper/WoE] Aborting database update because at least one page was found to be empty after retries. Selectors may be broken.")
		return
	}

	log.Printf("[I] [Scraper/WoE] Finished scraping all pages. Total Matched from DB: %d. Total Parsed Details: %d.", totalMatchedCount, totalParsedCount)

	if len(allWoeChars) == 0 {
		log.Println("[W] [Scraper/WoE] No WoE character information could be matched/parsed across all pages. Skipping database update.")
		return
	}

	processWoeCharacterData(allWoeChars)
}

// scrapeMvpKills is now only responsible for concurrent scraping.
func scrapeMvpKills() {
	log.Println("[I] [Scraper/MVP] Starting MVP kill count scrape...")

	playerBlockRegex := regexp.MustCompile(`\\"name\\":\\"([^"]+)\\",\\"base_level\\".*?\\"mvp_kills\\":\[(.*?)]`)
	mvpKillsRegex := regexp.MustCompile(`{\\"mob_id\\":(\d+),\\"kills\\":(\d+)}`)

	const firstPageURL = "https://projetoyufa.com/rankings/mvp?page=1"
	lastPage := scraperClient.findLastPage(firstPageURL, "[Scraper/MVP]")

	allMvpKills := make(map[string]map[string]int) // Map[characterName]Map[mobID]killCount
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	log.Printf("[I] [Scraper/MVP] Scraping all %d pages...", lastPage)
	for page := 1; page <= lastPage; page++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(pageIndex int) {
			defer wg.Done()
			defer func() { <-sem }()

			var bodyContent string
			var err error
			var numPlayerBlocks int
			var pageKills map[string]map[string]int

			// --- MODIFICATION: Added retry loop for parsing ---
			for attempt := 1; attempt <= maxParseRetries; attempt++ {
				url := fmt.Sprintf("https://projetoyufa.com/rankings/mvp?page=%d", pageIndex)
				bodyContent, err = scraperClient.getPage(url, "[Scraper/MVP]")
				if err != nil {
					log.Printf("[E] [Scraper/MVP] Network/HTTP error for page %d (attempt %d/%d): %v. Retrying...", pageIndex, attempt, maxParseRetries, err)
					time.Sleep(parseRetryDelay)
					continue
				}

				playerBlocks := playerBlockRegex.FindAllStringSubmatch(bodyContent, -1)
				numPlayerBlocks = len(playerBlocks)
				pageKills = make(map[string]map[string]int) // Reset

				if numPlayerBlocks == 0 {
					// --- THIS IS THE NEW LOGIC ---
					log.Printf("[W] [Scraper/MVP] Page %d returned 0 MVP player blocks on parse attempt %d/%d. Retrying...", pageIndex, attempt, maxParseRetries)
					time.Sleep(parseRetryDelay)
					continue // Try fetching and parsing again
				}

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
				// --- End parsing logic ---

				// Success, break from retry loop
				break
			}
			// --- END MODIFICATION ---

			if len(pageKills) > 0 {
				mu.Lock()
				for charName, kills := range pageKills {
					allMvpKills[charName] = kills
				}
				mu.Unlock()
			}

			// Log final status for this page
			if numPlayerBlocks > 0 {
				log.Printf("[D] [Scraper/MVP] Scraped page %d/%d, found %d characters with MVP kills.", pageIndex, lastPage, len(pageKills))
			} else {
				log.Printf("[E] [Scraper/MVP] Failed to scrape page %d/%d after all retries.", pageIndex, lastPage)
			}
		}(page)
	}

	wg.Wait()
	log.Printf("[I] [Scraper/MVP] Finished scraping all pages. Found %d unique characters with MVP kills.", len(allMvpKills))

	if len(allMvpKills) == 0 {
		log.Println("[W] [Scraper/MVP] No MVP kills found after scrape. Skipping database update.")
		return
	}

	// Call the dedicated database function
	processMvpKills(allMvpKills)
}

const ptNameDelay = 3 * time.Second // Delay between requests

// fetchPortugueseName fetches a single item's PT name from RagnarokDatabase
func fetchPortugueseName(itemID int) (string, error) {
	rdbURL := fmt.Sprintf("https://ragnarokdatabase.com/item/%d", itemID)

	// Use the shared scraperClient's HTTP client
	req, err := http.NewRequest("GET", rdbURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", scraperClient.UserAgent)

	rdbRes, err := scraperClient.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get URL from RDB: %w", err)
	}
	defer rdbRes.Body.Close()

	if rdbRes.StatusCode != 200 {
		if rdbRes.StatusCode == 404 {
			return "", fmt.Errorf("item not found on RDB (404)")
		}
		return "", fmt.Errorf("RDB status code error: %d %s", rdbRes.StatusCode, rdbRes.Status)
	}

	body, readErr := io.ReadAll(rdbRes.Body)
	if readErr != nil {
		return "", fmt.Errorf("could not read body from RDB: %w", readErr)
	}

	// Parse the name from the H1 tag
	matches := ptNameRegex.FindStringSubmatch(string(body))
	if len(matches) > 1 {
		rawNamePT := strings.TrimSpace(matches[1])
		// Clean the name (e.g., remove "[1]")
		cleanNamePT := slotRemoverRegex.ReplaceAllString(rawNamePT, " ")
		return strings.TrimSpace(cleanNamePT), nil
	}

	return "", fmt.Errorf("could not find name regex on page")
}

// fetchAndUpdatePortugueseName fetches and updates the PT name for a single item ID.
func fetchAndUpdatePortugueseName(itemID int) (string, error) {
	// 1. Check current status in DB
	var namePT sql.NullString
	err := db.QueryRow("SELECT name_pt FROM internal_item_db WHERE item_id = ?", itemID).Scan(&namePT)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("item ID %d not found in local database. Cannot update", itemID)
		}
		return "", fmt.Errorf("database query error for item %d: %w", itemID, err)
	}

	// 2. Check if name already exists
	if namePT.Valid && namePT.String != "" {
		log.Printf("[I] [PT-Name] Item %d already has a Portuguese name: %s", itemID, namePT.String)
		return "", fmt.Errorf("item ID %d already has a Portuguese name (%s)", itemID, namePT.String)
	}

	// 3. Fetch the name
	log.Printf("[I] [PT-Name] Fetching Portuguese name for item %d...", itemID)
	fetchedName, err := fetchPortugueseName(itemID)
	if err != nil {
		return "", fmt.Errorf("failed to fetch name for item %d from RDB: %w", itemID, err)
	}

	if fetchedName == "" {
		return "", fmt.Errorf("fetched name for item %d was empty. Not updating", itemID)
	}

	// 4. Update the DB
	_, err = db.Exec("UPDATE internal_item_db SET name_pt = ? WHERE item_id = ?", fetchedName, itemID)
	if err != nil {
		return "", fmt.Errorf("failed to update database for item %d: %w", itemID, err)
	}

	log.Printf("[I] [PT-Name] Successfully updated item %d with Portuguese name: %s", itemID, fetchedName)
	return fetchedName, nil
}

// populateMissingPortugueseNames is the background job function
func populateMissingPortugueseNames() {
	if !ptNameMutex.TryLock() {
		log.Println("[I] [Scraper/PT-Name] Portuguese name population job is already running. Skipping.")
		return
	}
	defer ptNameMutex.Unlock()

	log.Println("[I] [Scraper/PT-Name] Starting job to populate missing Portuguese names...")

	// 1. Get all item IDs that need a PT name
	rows, err := db.Query("SELECT item_id FROM internal_item_db WHERE name_pt IS NULL OR name_pt = ''")
	if err != nil {
		log.Printf("[E] [Scraper/PT-Name] Failed to query for items: %v", err)
		return
	}
	defer rows.Close()

	var itemIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err == nil {
			itemIDs = append(itemIDs, id)
		}
	}

	if len(itemIDs) == 0 {
		log.Println("[I] [Scraper/PT-Name] No items need Portuguese names. Job complete.")
		return
	}

	log.Printf("[I] [Scraper/PT-Name] Found %d items to update.", len(itemIDs))

	// 2. Loop, fetch, update, and sleep
	var successCount, failCount int
	for i, itemID := range itemIDs {
		ptName, err := fetchPortugueseName(itemID)
		if err != nil {
			log.Printf("[W] [Scraper/PT-Name] [%d/%d] Failed to fetch name for item %d: %v", i+1, len(itemIDs), itemID, err)
			failCount++
		} else {
			// Update the DB
			_, err := db.Exec("UPDATE internal_item_db SET name_pt = ? WHERE item_id = ?", ptName, itemID)
			if err != nil {
				log.Printf("[E] [Scraper/PT-Name] [%d/%d] Failed to update DB for item %d: %v", i+1, len(itemIDs), itemID, err)
				failCount++
			} else {
				log.Printf("[I] [Scraper/PT-Name] [%d/%d] Updated item %d with name: %s", i+1, len(itemIDs), itemID, ptName)
				successCount++
			}
		}

		// 3. Add the requested delay to avoid being blocked
		if i < len(itemIDs)-1 { // Don't sleep after the last item
			time.Sleep(ptNameDelay)
		}
	}

	log.Printf("[I] [Scraper/PT-Name] Job finished. Successfully updated: %d, Failed: %d", successCount, failCount)
}

// saveChatMessagesToDB inserts a batch of new messages in a single transaction.
func saveChatMessagesToDB(messages []ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	// De-duplication logic
	// The key is the ChatMessage struct (Channel, CharacterName, Message).
	// This will now correctly de-duplicate retransmitted packets.
	seen := make(map[ChatMessage]struct{}, len(messages))
	dedupedMessages := make([]ChatMessage, 0, len(messages))

	for _, msg := range messages {
		if _, exists := seen[msg]; !exists {
			seen[msg] = struct{}{}
			dedupedMessages = append(dedupedMessages, msg)
		}
	}

	if len(dedupedMessages) == 0 {
		log.Printf("[D] [Scraper/Chat] Skipped saving batch of %d, all were duplicates.", len(messages))
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback() // Rollback on error

	// --- MODIFIED: SQL includes channel ---
	stmt, err := tx.Prepare("INSERT INTO chat (timestamp, channel, character_name, message) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	now := time.Now().Format(time.RFC3339)
	for _, msg := range dedupedMessages {
		// --- MODIFIED: Exec call includes msg.Channel ---
		if _, err := stmt.Exec(now, msg.Channel, msg.CharacterName, msg.Message); err != nil {
			log.Printf("[W] [Scraper/Chat] Failed to insert message from '%s' (%s): %v", msg.CharacterName, msg.Channel, err)
			// Continue inserting other messages
		}
	}

	log.Printf("[I] [Scraper/Chat] Saved %d new chat messages to DB (out of %d batched).", len(dedupedMessages), len(messages))
	return tx.Commit()
}

func logChatActivityPeriodically() {
	activityLogMutex.Lock()
	defer activityLogMutex.Unlock()

	now := time.Now()
	// Check if we've already logged an entry within the last minute
	if now.Sub(lastActivityLog) < 1*time.Minute {
		return // Already logged this minute
	}

	// It's a new minute (or the first log), so update the timestamp
	lastActivityLog = now

	// Store the timestamp truncated to the minute (e.g., 15:04:00)
	timestamp := now.Truncate(time.Minute).Format(time.RFC3339)

	// Use "INSERT OR IGNORE" to avoid errors on duplicate (which shouldn't
	// happen with the mutex, but it's safer)
	_, err := db.Exec("INSERT OR IGNORE INTO chat_activity_log (timestamp) VALUES (?)", timestamp)
	if err != nil {
		log.Printf("[E] [Scraper/Chat] Failed to log chat activity heartbeat: %v", err)
	} else if enableChatScraperDebugLogs {
		log.Printf("[D] [Scraper/Chat] Logged activity heartbeat for %s", timestamp)
	}
}

// startChatPacketCapture is the new long-running service to replace processChatLogFile
func startChatPacketCapture(ctx context.Context) {
	log.Println("[I] [Scraper/Chat] Initializing live packet capture...")

	// --- 1. Find Network Device ---
	device := os.Getenv("CHAT_CAPTURE_DEVICE")
	if device == "" {
		// Use Go's standard 'net' package to find a suitable device.
		ifaces, err := net.Interfaces()
		if err != nil {
			log.Printf("[E] [Scraper/Chat] net.Interfaces() failed: %v. Chat capture disabled.", err)
			return
		}

		for _, i := range ifaces {
			// Check if interface is up and not a loopback
			isUp := (i.Flags & net.FlagUp) != 0
			isLoopback := (i.Flags & net.FlagLoopback) != 0

			if isUp && !isLoopback {
				// Check if it has a usable address
				addrs, err := i.Addrs()
				if err == nil && len(addrs) > 0 {
					device = i.Name
					log.Printf("[I] [Scraper/Chat] No CHAT_CAPTURE_DEVICE set. Auto-selected device: %s", device)
					break
				}
			}
		}

		if device == "" {
			log.Printf("[E] [Scraper/Chat] Could not find a suitable non-loopback network device. Please set CHAT_CAPTURE_DEVICE. Chat capture disabled.")
			return
		}
	}

	// --- 2. Get Port ---
	port := os.Getenv("CHAT_CAPTURE_PORT")
	if port == "" {
		port = "6121" // Default Ragnarok Online Char Server port
		log.Printf("[W] [Scraper/Chat] CHAT_CAPTURE_PORT not set. Defaulting to %s. This may not be correct.", port)
	}

	// --- 3. Open pcap Handle ---
	handle, err := pcap.OpenLive(device, 65536, true, pcap.BlockForever)
	if err != nil {
		log.Printf("[E] [Scraper/Chat] Failed to open pcap handle on %s: %v. (Do you have libpcap/Npcap installed and root/admin privileges?)", device, err)
		return
	}
	defer handle.Close()

	// --- 4. Set BPF Filter ---
	filter := fmt.Sprintf("tcp port %s", port)
	if err := handle.SetBPFFilter(filter); err != nil {
		log.Printf("[E] [Scraper/Chat] Failed to set BPF filter (%s): %v", filter, err)
		return
	}
	log.Printf("[I] [Scraper/Chat] Started packet capture on %s, filtering for %s.", device, filter)

	// --- 5. Start Packet Processing Loop ---
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	var newMessages []ChatMessage
	flushTicker := time.NewTicker(5 * time.Second) // Flush messages to DB every 5s
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Shutdown signal received
			log.Println("[I] [Scraper/Chat] Stopping packet capture...")
			if len(newMessages) > 0 {
				log.Println("[I] [Scraper/Chat] Flushing final batch of chat messages...")
				// Call and check error
				if err := saveChatMessagesToDB(newMessages); err != nil {
					log.Printf("[E] [Scraper/Chat] Error flushing final message batch to DB: %v", err)
				}
			}
			return

		case <-flushTicker.C:
			// Periodic flush to DB
			if len(newMessages) > 0 {
				log.Printf("[I] [Scraper/Chat] Flushing %d batched messages to DB.", len(newMessages))
				// Call and check error
				if err := saveChatMessagesToDB(newMessages); err != nil {
					log.Printf("[E] [Scraper/Chat] Error flushing message batch to DB: %v", err)
				}
				newMessages = nil // Clear the batch
			}

		case packet := <-packetSource.Packets():

			// 1. Update the "last seen" time (for navbar)
			lastChatPacketTime.Store(time.Now().Unix())
			// 2. Log this minute's activity for the graph
			logChatActivityPeriodically()

			if enableChatScraperDebugLogs {
				log.Printf("[D] [Scraper/Chat] Received packet. PktData size: %d", len(packet.Data()))
			}

			// We have a packet
			tcpLayer := packet.Layer(layers.LayerTypeTCP)
			if tcpLayer == nil {
				continue
			}
			tcp, _ := tcpLayer.(*layers.TCP)
			payload := tcp.Payload // This is the raw byte payload
			if len(payload) == 0 {
				continue
			}

			if enableChatScraperDebugLogs {
				log.Printf("[D] [Scraper/Chat] Found TCP packet. Payload size: %d bytes", len(payload))
				// Log the full payload in hex and as a sanitized string

				// --- FIX: Decode payload from Latin-1 for logging ---
				// *** CHANGED HERE ***
				reader := transform.NewReader(bytes.NewReader(payload), charmap.ISO8859_1.NewDecoder())
				utf8Bytes, _ := io.ReadAll(reader) // Ignore error for logging

				sanitizedPayload := strings.Map(func(r rune) rune {
					if unicode.IsPrint(r) {
						return r
					}
					return '.' // Replace non-printable with a dot
				}, string(utf8Bytes)) // Use the decoded bytes
				// --- END FIX ---
				log.Printf("[D] [Scraper/Chat] RAW PAYLOAD (HEX): %s", hex.EncodeToString(payload))
				log.Printf("[D] [Scraper/Chat] RAW PAYLOAD (STR): %s", sanitizedPayload)
			}

			// --- REFACTORED PARSING LOOP ---
			i := 0
			for i < len(payload) {
				firstPrefixIdx := -1
				var firstPacketDef chatPacketDefinition

				// Find the *closest* known prefix from our current position 'i'
				for _, packetDef := range knownChatPackets {
					idx := bytes.Index(payload[i:], packetDef.prefix)
					if idx != -1 { // Found this prefix
						if firstPrefixIdx == -1 || idx < firstPrefixIdx {
							firstPrefixIdx = idx
							firstPacketDef = packetDef
						}
					}
				}

				if firstPrefixIdx == -1 {
					break // No more known prefixes in this payload
				}

				absIdx := i + firstPrefixIdx // Absolute index in payload
				def := firstPacketDef        // The definition for the packet we found

				if enableChatScraperDebugLogs {
					log.Printf("[D] [Scraper/Chat] Matched prefix %s at index %d.", hex.EncodeToString(def.prefix), absIdx)
				}

				// Check if we have enough bytes to read the length (prefix + 2 bytes for length)
				if absIdx+4 > len(payload) {
					if enableChatScraperDebugLogs {
						log.Printf("[D] [Scraper/Chat] Fragmented header. Skipping.")
					}
					i = absIdx + 1 // Search after this partial prefix
					continue
				}

				// Read the packet length (2 bytes, little-endian)
				length := int(binary.LittleEndian.Uint16(payload[absIdx+2 : absIdx+4]))
				msgLen := length - def.headerLength // Use definition's header length
				msgEnd := absIdx + def.messageOffset + msgLen

				if enableChatScraperDebugLogs {
					log.Printf("[D] [Scraper/Chat] Parsed packet length: %d. Header: %d. Message length: %d. Required end index: %d", length, def.headerLength, msgLen, msgEnd)
				}

				if msgLen <= 0 {
					if enableChatScraperDebugLogs {
						log.Printf("[D] [Scraper/Chat] Invalid message length (%d). Skipping.", msgLen)
					}
					i = absIdx + 1
					continue
				}

				if msgEnd > len(payload) {
					if enableChatScraperDebugLogs {
						log.Printf("[D] [Scraper/Chat] Fragmented body. Need %d bytes, have %d. Skipping.", msgEnd, len(payload))
					}
					i = absIdx + 1 // Search after this partial prefix
					continue
				}

				// If we're here, we have a full message!
				// Use the definition's message offset
				msgBytes := payload[absIdx+def.messageOffset : msgEnd]
				if enableChatScraperDebugLogs {
					log.Printf("[D] [Scraper/Chat] Extracted message (raw hex): %s", hex.EncodeToString(msgBytes))
				}

				// --- FIX: Decode from Latin-1 (ISO-8859-1) to UTF-8 ---
				// The game client likely sends in a legacy encoding.
				// *** CHANGED HERE ***
				reader := transform.NewReader(bytes.NewReader(msgBytes), charmap.ISO8859_1.NewDecoder())
				utf8Bytes, err := io.ReadAll(reader)
				if err != nil {
					log.Printf("[W] [Scraper/Chat] Failed to decode message from Latin-1: %v", err)
					// Fallback to the old (broken) method just in case
					utf8Bytes = msgBytes
				}

				// Trim NULL bytes *after* decoding
				message := string(bytes.Trim(utf8Bytes, "\x00"))
				// --- END FIX ---

				// Sanitize (this part now operates on a valid UTF-8 string)
				message = strings.Map(func(r rune) rune {
					if unicode.IsPrint(r) {
						return r
					}
					return -1 // Discard
				}, message)
				message = strings.TrimSpace(message)

				if enableChatScraperDebugLogs && message != "" {
					log.Printf("[D] [Scraper/Chat] Sanitized message: '%s'", message)
				}

				// --- UPDATED PARSING LOGIC TO HANDLE DROPS ---
				if message != "" {
					var channel, charName, chatMsg string

					// --- NEW: Check for Drop Packet ---
					// We can compare the prefix of the definition we just used to parse
					if bytes.Equal(def.prefix, []byte{0x9a, 0x00}) {
						channel = "Drop"
						// This is an announcement/drop packet.
						// Check if it's the specific 0.01% drop message.
						if strings.Contains(message, "(chance: 0.01%)") && (strings.Contains(message, "got") || strings.Contains(message, "stole")) {
							channel = "Drop"

							// <<< --- START: Real-time Drop Logging --- >>>
							// We have a drop message. Let's parse it for the changelog.
							// We use the regexes from handlers.go (same 'main' package).
							dropMatches := dropMessageRegex.FindStringSubmatch(message)
							if len(dropMatches) == 4 {
								// dropMatches[1] = character name (e.g., "Lindinha GC")
								// dropMatches[3] = rest of message (e.g., "Raydric's Iron Cain (chance: 0.01%)")
								playerName := dropMatches[1]
								itemMsgFragment := dropMatches[3]

								// Now extract the item name from the fragment
								itemMatches := reItemFromDrop.FindStringSubmatch(itemMsgFragment)
								var itemName string
								if len(itemMatches) == 4 {
									if itemMatches[1] != "" {
										itemName = itemMatches[1]
									} else if itemMatches[2] != "" {
										itemName = itemMatches[2]
									} else if itemMatches[3] != "" {
										itemName = itemMatches[3]
									}
								}

								itemName = strings.TrimSpace(itemName)

								if playerName != "" && itemName != "" {
									// We have both! Log it to the changelog in real-time.
									// We use a goroutine so it doesn't block the packet capture loop.
									// The new function handles duplicate protection.
									go logDropToChangelog(time.Now().Format(time.RFC3339), playerName, itemName)
								}
							}
							// <<< --- END: Real-time Drop Logging --- >>>

						} else {
							// Otherwise, it's a general announcement.
							channel = "Announcement"
						}
						charName = "System"
						chatMsg = message
					} else if bytes.Equal(def.prefix, []byte{0xc3, 0x01}) {
						// This is a System/Event announcement packet (e.g., Invasion)
						channel = "Event"
						charName = "System"
						chatMsg = message
					} else if strings.HasPrefix(message, "[") && strings.Contains(message, "] ") {
						// Case: "[Global] golbin : bom dia!"
						channelPart, rest, _ := strings.Cut(message, "] ")
						channel = strings.TrimPrefix(channelPart, "[") // "Global"

						// Now parse the 'rest' for "char : msg"
						charNamePart, chatMsgPart, found := strings.Cut(rest, " : ")
						if found {
							// Standard: [Channel] Char : Msg
							charName = strings.TrimSpace(charNamePart) // "golbin"
							chatMsg = strings.TrimSpace(chatMsgPart)   // "bom dia!"
						} else {
							// No colon. Is it a system broadcast?
							// Whitelist known system channels.
							if channel == "Notice" {
								charName = "System"
								chatMsg = strings.TrimSpace(rest)
							} else {
								// It's [Global] golbin or [Trade] M2LOKERO (no colon)
								// This is not a chat message. Discard it.
								chatMsg = "" // Set to empty to be discarded
								if enableChatScraperDebugLogs {
									log.Printf("[D] [Scraper/Chat] Discarding non-chat message (no ' : ' in channel '%s'): '%s'", channel, message)
								}
							}
						}
					} else {
						// 2. No channel prefix, assume "Local"
						channel = "Local"

						// Case: "golbin : segunda aaa"
						charNamePart, chatMsgPart, found := strings.Cut(message, " : ")
						if found {
							// Standard: Char : Msg
							charName = strings.TrimSpace(charNamePart) // "golbin"
							chatMsg = strings.TrimSpace(chatMsgPart)   // "segunda aaa"
						} else {
							// No colon. Assume it's a local system broadcast.
							// Case: "Welcome to the server!"
							charName = "System"
							chatMsg = message // message is already trimmed
						}
					}

					// 3. Add to batch (if message is not empty)
					if chatMsg != "" {
						newMessages = append(newMessages, ChatMessage{
							Channel:       channel,
							CharacterName: charName,
							Message:       chatMsg,
						})
					} else if enableChatScraperDebugLogs {
						log.Printf("[D] [Scraper/Chat] Parsed an empty message. Discarding.")
					}
				}
				// --- END UPDATED PARSING LOGIC ---

				// Continue search *after* this full message
				i = msgEnd
			}
			// --- END REFACTORED PARSING LOOP ---
		}
	}
}

// logDropToChangelog inserts a drop event directly into the character_changelog table.
// This is called in real-time by the packet capture service.
func logDropToChangelog(timestamp, charName, itemName string) {
	if charName == "" || itemName == "" {
		return
	}

	activityDescription := "Dropped item: " + itemName

	// We perform a quick check to prevent duplicate entries if the packet is re-sent
	// and processed multiple times in a short window (e.g., 5 seconds).
	var exists int
	err := db.QueryRow(`
		SELECT 1 FROM character_changelog 
		WHERE character_name = ? 
		  AND activity_description = ? 
		  AND change_time > datetime(?, '-5 second') 
		LIMIT 1`,
		charName, activityDescription, timestamp,
	).Scan(&exists)

	// If it's not found (ErrNoRows) or another error, proceed with insert.
	// If it *is* found (err == nil), we skip.
	if err == nil {
		if enableChatScraperDebugLogs {
			log.Printf("[D] [Scraper/DropLog] Duplicate drop log detected for %s. Skipping.", charName)
		}
		return
	}

	// Insert the new drop log entry
	_, err = db.Exec(`
		INSERT INTO character_changelog (character_name, change_time, activity_description) 
		VALUES (?, ?, ?)`,
		charName, timestamp, activityDescription,
	)

	if err != nil {
		log.Printf("[E] [Scraper/DropLog] Failed to insert drop log for %s: %v", charName, err)
	} else if enableChatScraperDebugLogs {
		log.Printf("[D] [Scraper/DropLog] Successfully logged drop for %s: %s", charName, itemName)
	}
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

	log.Printf("[I] [Job] Starting initial run for %s job...", job.Name)
	//job.Func() // Run immediately on start

	for {
		select {
		case <-ctx.Done():
			log.Printf("[I] [Job] Stopping %s job due to shutdown.", job.Name)
			return
		case <-ticker.C:
			log.Printf("[I] [Job] Starting scheduled %s scrape...", job.Name)
			job.Func()
		}
	}
}

func startBackgroundJobs(ctx context.Context) {
	// Define all scheduled jobs
	jobs := []Job{
		{Name: "Market", Func: scrapeData, Interval: 3 * time.Minute},
		{Name: "Player Count", Func: scrapeAndStorePlayerCount, Interval: 1 * time.Minute},
		{Name: "Player Character", Func: scrapePlayerCharacters, Interval: 6 * time.Hour},
		{Name: "Guild", Func: scrapeGuilds, Interval: 1 * time.Hour},
		{Name: "Zeny", Func: scrapeZeny, Interval: 6 * time.Hour},
		{Name: "MVP Kill", Func: scrapeMvpKills, Interval: 5 * time.Minute},
		//		{Name: "PT-Name-Populator", Func: populateMissingPortugueseNames, Interval: 6 * time.Hour},
		{Name: "WoE-Char-Rankings", Func: scrapeWoeCharacterRankings, Interval: 12 * time.Hour},
	}

	// Start all standard jobs
	for _, job := range jobs {
		go runJobOnTicker(ctx, job)
	}

	go startChatPacketCapture(ctx)
	// --- MODIFICATION: Removed the entire "Special RMS Cache Refresh Job" goroutine ---
	// The go func() { ... } block that called runFullRMSCacheJob() is deleted.
	// --- END MODIFICATION ---
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
