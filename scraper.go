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

var mvpMobIDsScrape = []string{
	"1038", "1039", "1046", "1059", "1086", "1087", "1112", "1115", "1147",
	"1150", "1157", "1159", "1190", "1251", "1252", "1272", "1312", "1373",
	"1389", "1418", "1492", "1511",
}

var mvpNamesScraper = map[string]string{
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

var (
	marketMutex      sync.Mutex
	characterMutex   sync.Mutex
	playerCountMutex sync.Mutex
)

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

	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET", "https://projetoyufa.com/info", nil)
	if err != nil {
		log.Printf("❌ [Counter] Failed to create HTTP request: %v", err)
		return
	}

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

	doc, err := goquery.NewDocumentFromReader(resp.Body)
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

func scrapePlayerCharacters() {
	log.Println("🏆 [Characters] Starting player character scrape...")

	const maxRetries = 3
	const retryDelay = 3 * time.Second

	rankRegex := regexp.MustCompile(`p\-1 text\-center font\-medium\\",\\"children\\":(\d+)\}\]`)
	nameRegex := regexp.MustCompile(`max-w-10 truncate p-1 font-semibold">([^<]+)</td>`)
	baseLevelRegex := regexp.MustCompile(`\\"level\\":(\d+),`)
	jobLevelRegex := regexp.MustCompile(`\\"job_level\\":(\d+),\\"exp`)
	expRegex := regexp.MustCompile(`\\"exp\\":(\d+)`)
	classRegex := regexp.MustCompile(`"hidden text\-sm sm:inline\\",\\"children\\":\\"([^"]+)\\"`)

	client := &http.Client{Timeout: 45 * time.Second}

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
					Experience: rawExp / 1000000.0,
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

	characterMutex.Lock()
	defer characterMutex.Unlock()

	if enableCharacterScraperDebugLogs {
		log.Println("    -> [DB] Fetching existing player data for activity comparison...")
	}
	existingPlayers := make(map[string]PlayerCharacter)
	rowsPre, err := db.Query("SELECT name, base_level, job_level, experience, class, last_active FROM characters")
	if err != nil {
		log.Printf("❌ [DB] Failed to query existing characters for comparison: %v", err)
	} else {
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
	}

	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [DB] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

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

	for _, p := range allPlayers {
		lastActiveTime := updateTime
		if oldPlayer, exists := existingPlayers[p.Name]; exists {
			baseLeveledUp := false
			if p.BaseLevel > oldPlayer.BaseLevel {

				logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Leveled up to Base Level %d!", p.BaseLevel))
				baseLeveledUp = true
			}
			if p.JobLevel > oldPlayer.JobLevel {

				logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Leveled up to Job Level %d!", p.JobLevel))
			}

			if !baseLeveledUp {
				expDelta := p.Experience - oldPlayer.Experience

				if expDelta > 0.001 {

					logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Gained %.2f%% experience (now at %.2f%%).", expDelta, p.Experience))
				} else if expDelta < -0.001 {

					logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Lost %.2f%% experience (now at %.2f%%).", -expDelta, p.Experience))
				}
			} else {

				logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Gained %.2f%% experience (now at %.2f%%).", p.Experience, p.Experience))
			}

			if p.Class != oldPlayer.Class {

				logCharacterActivity(changelogStmt, p.Name, fmt.Sprintf("Changed class from '%s' to '%s'.", oldPlayer.Class, p.Class))
			}

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

	const maxRetries = 5
	const retryDelay = 5 * time.Second
	client := &http.Client{Timeout: 60 * time.Second}

	nameRegex := regexp.MustCompile(`<span class="font-medium">([^<]+)</span>`)
	levelRegex := regexp.MustCompile(`\\"guild_lv\\":(\d+),\\"connect_member\\"`)
	masterRegex := regexp.MustCompile(`\\"master\\":\\"([^"]+)\\",\\"members\\"`)
	membersRegex := regexp.MustCompile(`\\"members\\":\[(.*?)\]\}`)
	memberNameRegex := regexp.MustCompile(`\\"name\\":\\"([^"]+)\\",\\"base_level\\"`)

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
	allMembers := make(map[string]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

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

			nameMatches := nameRegex.FindAllStringSubmatch(bodyContent, -1)
			levelMatches := levelRegex.FindAllStringSubmatch(bodyContent, -1)
			masterMatches := masterRegex.FindAllStringSubmatch(bodyContent, -1)
			membersMatches := membersRegex.FindAllStringSubmatch(bodyContent, -1)

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

				guild := Guild{Name: name, Level: level, Master: master}
				pageGuilds = append(pageGuilds, guild)

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

	characterMutex.Lock()
	defer characterMutex.Unlock()

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

	tx, errDb := db.Begin()
	if errDb != nil {
		log.Printf("❌ [Guilds][DB] Failed to begin transaction for guilds update: %v", errDb)
		return
	}
	defer tx.Rollback()

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

	allInvolvedChars := make(map[string]bool)
	for charName := range oldAssociations {
		allInvolvedChars[charName] = true
	}
	for charName := range allMembers {
		allInvolvedChars[charName] = true
	}

	changelogStmt, err := tx.Prepare(`
		INSERT INTO character_changelog (character_name, change_time, activity_description)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		log.Printf("❌ [DB] Failed to prepare changelog statement for guilds: %v", err)

	} else {
		defer changelogStmt.Close()
	}

	for charName := range allInvolvedChars {

		var exists int
		err := tx.QueryRow("SELECT COUNT(*) FROM characters WHERE name = ?", charName).Scan(&exists)
		if err != nil {
			log.Printf("    -> [Guilds][DB] WARN: Could not check for existence of character '%s' before logging guild change: %v. Skipping log.", charName, err)
			continue
		}
		if exists == 0 {

			if enableGuildScraperDebugLogs {
				log.Printf("    -> [Guilds] Skipping guild change log for non-existent character '%s'.", charName)
			}
			continue
		}

		oldGuild, hadOld := oldAssociations[charName]
		newGuild, hasNew := allMembers[charName]

		if changelogStmt != nil {
			if hadOld && !hasNew {
				logCharacterActivity(changelogStmt, charName, fmt.Sprintf("Left guild '%s'.", oldGuild))
			} else if !hadOld && hasNew {
				logCharacterActivity(changelogStmt, charName, fmt.Sprintf("Joined guild '%s'.", newGuild))
			} else if hadOld && hasNew && oldGuild != newGuild {
				logCharacterActivity(changelogStmt, charName, fmt.Sprintf("Moved from guild '%s' to '%s'.", oldGuild, newGuild))
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ [Guilds][DB] Failed to commit guilds and characters transaction: %v", err)
		return
	}

	log.Printf("✅ [Guilds] Scrape and update complete. Saved %d guild records and updated character associations.", len(allGuilds))
}

type CharacterZenyInfo struct {
	Zeny       sql.NullInt64
	LastActive string
}

func scrapeZeny() {
	log.Println("💰 [Zeny] Starting Zeny ranking scrape...")

	const maxRetries = 3
	const retryDelay = 3 * time.Second
	client := &http.Client{Timeout: 45 * time.Second}

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

	updateTime := time.Now().Format(time.RFC3339)
	allZenyInfo := make(map[string]int64)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

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

	characterMutex.Lock()
	defer characterMutex.Unlock()

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

	} else {
		defer changelogStmt.Close()
	}

	updatedCount := 0
	unchangedCount := 0
	for name, newZeny := range allZenyInfo {
		oldInfo, exists := existingCharacters[name]

		if !exists || !oldInfo.Zeny.Valid || oldInfo.Zeny.Int64 != newZeny {
			var oldZeny int64
			if exists && oldInfo.Zeny.Valid {
				oldZeny = oldInfo.Zeny.Int64
			}

			delta := newZeny - oldZeny

			formatWithCommas := func(n int64) string {
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

			formattedNewZeny := formatWithCommas(newZeny)
			var description string

			if delta > 0 {
				formattedDelta := formatWithCommas(delta)
				description = fmt.Sprintf("Zeny increased by %sz (New total: %sz).", formattedDelta, formattedNewZeny)
			} else if delta < 0 {
				formattedDelta := formatWithCommas(-delta)
				description = fmt.Sprintf("Zeny decreased by %sz (New total: %sz).", formattedDelta, formattedNewZeny)
			}

			if description != "" && changelogStmt != nil {
				logCharacterActivity(changelogStmt, name, description)
			}

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

	reRefineMid := regexp.MustCompile(`\s(\+\d+)`)
	reRefineStart := regexp.MustCompile(`^(\+\d+)\s`)

	const maxRetries = 3
	const retryDelay = 5 * time.Second
	const requestURL = "https://projetoyufa.com/market"

	client := &http.Client{Timeout: 45 * time.Second}

	var htmlContent string
	var scrapeSuccessful bool

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🚀 [Market] Scraping market page (Attempt %d/%d)...", attempt, maxRetries)

		req, err := http.NewRequest("GET", requestURL, nil)
		if err != nil {
			log.Printf("⚠️ [Market] Attempt %d/%d failed: could not create request: %v", attempt, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

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
		break
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
	activeSellers := make(map[string]bool)

	doc.Find(`div[data-slot="card"]`).Each(func(i int, s *goquery.Selection) {
		shopName := strings.TrimSpace(s.Find(`div[data-slot="card-title"]`).Text())
		sellerName := strings.TrimSpace(s.Find("svg.lucide-user").Next().Text())
		mapName := strings.TrimSpace(s.Find("svg.lucide-map-pin").Next().Text())
		mapCoordinates := strings.TrimSpace(s.Find("svg.lucide-copy").Next().Text())
		activeSellers[sellerName] = true

		if enableMarketScraperDebugLogs == true {
			log.Printf("[Market] shop name: %s, seller name: %s, map_name: %s, mapcoord: %s", shopName, sellerName, mapName, mapCoordinates)
		}

		s.Find(`div[data-slot="card-content"] .flex.items-center.space-x-2`).Each(func(j int, itemSelection *goquery.Selection) {
			itemName := strings.TrimSpace(itemSelection.Find("p.truncate").Text())

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

	marketMutex.Lock()
	defer marketMutex.Unlock()

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

func scrapeMvpKills() {
	log.Println("☠️  [MVP] Starting MVP kill count scrape...")

	const maxRetries = 3
	const retryDelay = 3 * time.Second

	playerBlockRegex := regexp.MustCompile(`\\"name\\":\\"([^"]+)\\",\\"base_level\\".*?\\"mvp_kills\\":\[(.*?)]`)

	mvpKillsRegex := regexp.MustCompile(`{\\"mob_id\\":(\d+),\\"kills\\":(\d+)}`)

	client := &http.Client{Timeout: 45 * time.Second}

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
				mvpsJSON := block[2]

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

	characterMutex.Lock()
	defer characterMutex.Unlock()

	allCharacterNames := make(map[string]bool)
	charRows, err := db.Query("SELECT name FROM characters")
	if err != nil {
		log.Printf("❌ [MVP][DB] Failed to pre-fetch character names: %v. Aborting update.", err)
		return
	}
	for charRows.Next() {
		var name string
		if err := charRows.Scan(&name); err == nil {
			allCharacterNames[name] = true
		}
	}
	charRows.Close()

	allExistingKills := make(map[string]map[string]int)

	selectCols := make([]string, 0, len(mvpMobIDsScrape)+1)
	selectCols = append(selectCols, "character_name")
	scanDest := make([]interface{}, len(mvpMobIDsScrape)+1)
	scanDest[0] = new(string)
	columnValues := make([]sql.NullInt64, len(mvpMobIDsScrape))

	for i, mobID := range mvpMobIDsScrape {
		selectCols = append(selectCols, fmt.Sprintf("mvp_%s", mobID))
		scanDest[i+1] = &columnValues[i]
	}

	mvpQuery := fmt.Sprintf("SELECT %s FROM character_mvp_kills", strings.Join(selectCols, ", "))
	mvpRows, err := db.Query(mvpQuery)
	if err != nil {
		log.Printf("❌ [MVP][DB] Failed to pre-fetch MVP kills: %v. Aborting update.", err)
		return
	}

	for mvpRows.Next() {
		if err := mvpRows.Scan(scanDest...); err != nil {
			log.Printf("    -> [MVP][DB] WARN: Failed to scan existing MVP row: %v", err)
			continue
		}
		charName := *(scanDest[0].(*string))
		playerKills := make(map[string]int)
		for i, mobID := range mvpMobIDsScrape {
			if columnValues[i].Valid {
				playerKills[mobID] = int(columnValues[i].Int64)
			}
		}
		allExistingKills[charName] = playerKills
	}
	mvpRows.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ [MVP][DB] Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	columnNames := []string{"character_name"}
	valuePlaceholders := []string{"?"}
	updateSetters := []string{}
	for _, mobID := range mvpMobIDsScrape {
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

	for charName, kills := range allMvpKills {

		if !allCharacterNames[charName] {
			if enableMvpScraperDebugLogs {
				log.Printf("    -> [MVP][DB] Character '%s' not found in main table. Skipping MVP data insert.", charName)
			}
			continue
		}

		existingKills := allExistingKills[charName]
		if existingKills == nil {
			existingKills = make(map[string]int)
		}

		params := []interface{}{charName}
		for _, mobID := range mvpMobIDsScrape {
			newKillCount := 0
			if count, ok := kills[mobID]; ok {
				newKillCount = count
			}
			existingKillCount := existingKills[mobID]

			finalKillCount := newKillCount
			if existingKillCount > newKillCount {
				finalKillCount = existingKillCount
				if enableMvpScraperDebugLogs {
					log.Printf("    -> [MVP] Stale data for %s on MVP %s. DB has %d, scrape has %d. Keeping DB value.", charName, mobID, existingKillCount, newKillCount)
				}
			}

			if finalKillCount > existingKillCount {

				log.Printf("    -> [MVP] Stale data for %s on MVP %s. DB has %d, scrape has %d. updating DB value.", charName, mobID, existingKillCount, newKillCount)

			}

			params = append(params, finalKillCount)
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
		scrapeData()
		for {
			log.Printf("🕒 [Job] Waiting for the next 5-minute market scrape schedule...")
			<-ticker.C
			scrapeData()
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		scrapeAndStorePlayerCount()
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
		scrapeGuilds()
		ticker := time.NewTicker(25 * time.Minute)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 30-minute guild schedule...")
			<-ticker.C
			scrapeGuilds()
		}
	}()

	go func() {

		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 1-hour Zeny ranking schedule...")
			<-ticker.C
			scrapeZeny()
		}
	}()

	go func() {
		scrapeMvpKills()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			log.Printf("🕒 [Job] Waiting for the next 5-minute MVP kill count schedule...")
			<-ticker.C
			scrapeMvpKills()
		}
	}()

	go func() {
		go runFullRMSCacheJob()
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		log.Printf("🕒 [Job] RMS Cache Refresh job scheduled. Will run once every 24 hours.")

		for {
			<-ticker.C
			log.Printf("🕒 [Job] Starting scheduled 24-hour full RMS cache refresh...")

			go runFullRMSCacheJob()
		}
	}()
}
