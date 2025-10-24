package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func getVisitorHash(r *http.Request) string {

	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	} else {

		ip = strings.Split(ip, ",")[0]
	}

	ua := r.UserAgent()
	data := fmt.Sprintf("%s-%s", ip, ua)

	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

type pageViewLog struct {
	VisitorHash string
	PageURI     string
	Timestamp   string
}

var pageViewChannel = make(chan pageViewLog, 1000)

func startVisitorLogger(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	var batch []pageViewLog
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	const batchSize = 100

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		tx, err := db.Begin()
		if err != nil {
			log.Printf("❌ Visitor Logger: Failed to begin transaction: %v", err)
			return
		}

		visitorStmt, err := tx.Prepare(`
			INSERT INTO visitors (visitor_hash, first_visit, last_visit)
			VALUES (?, ?, ?)
			ON CONFLICT(visitor_hash) DO UPDATE SET
				last_visit = excluded.last_visit;
		`)
		if err != nil {
			log.Printf("❌ Visitor Logger: Failed to prepare visitor statement: %v", err)
			tx.Rollback()
			return
		}
		defer visitorStmt.Close()

		viewStmt, err := tx.Prepare(`
			INSERT INTO page_views (visitor_hash, page_path, view_timestamp)
			VALUES (?, ?, ?);
		`)
		if err != nil {
			log.Printf("❌ Visitor Logger: Failed to prepare page_view statement: %v", err)
			tx.Rollback()
			return
		}
		defer viewStmt.Close()

		visitorsProcessed := make(map[string]bool)

		for _, logEntry := range batch {
			if !visitorsProcessed[logEntry.VisitorHash] {
				_, err := visitorStmt.Exec(logEntry.VisitorHash, logEntry.Timestamp, logEntry.Timestamp)
				if err != nil {
					log.Printf("⚠️ Visitor Logger: Failed to exec visitor upsert: %v", err)
				}
				visitorsProcessed[logEntry.VisitorHash] = true
			}

			_, err := viewStmt.Exec(logEntry.VisitorHash, logEntry.PageURI, logEntry.Timestamp)
			if err != nil {
				log.Printf("⚠️ Visitor Logger: Failed to exec page_view insert: %v", err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Printf("❌ Visitor Logger: Failed to commit batch: %v", err)

		} else {
			log.Printf("📝 Visitor Logger: Flushed %d page views to database.", len(batch))
			batch = nil
		}
	}

	for {
		select {
		case <-ctx.Done():

			log.Println("🔌 Visitor Logger: Shutdown signal received. Draining channel...")

			for logEntry := range pageViewChannel {
				batch = append(batch, logEntry)
				if len(batch) >= batchSize {
					flushBatch()
				}
			}
			log.Println("🔌 Visitor Logger: Flushing final batch...")
			flushBatch()
			log.Println("✅ Visitor Logger: Shut down gracefully.")
			return
		case logEntry := <-pageViewChannel:
			batch = append(batch, logEntry)
			if len(batch) >= batchSize {
				flushBatch()
			}
		case <-ticker.C:

			flushBatch()
		}
	}
}

func visitorTracker(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		visitorHash := getVisitorHash(r)
		now := time.Now().Format(time.RFC3339)
		pageURI := r.URL.RequestURI()

		logEntry := pageViewLog{
			VisitorHash: visitorHash,
			PageURI:     pageURI,
			Timestamp:   now,
		}

		select {
		case pageViewChannel <- logEntry:

		default:

			log.Println("⚠️ Page view log channel is full. Dropping a page view.")
		}

		next.ServeHTTP(w, r)
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("ℹ️ No .env file found, relying on system environment variables.")
	}

	var err error

	db, err = initDB("./market_data.db")
	if err != nil {
		log.Fatalf("❌ Failed to initialize database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	var wg sync.WaitGroup

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		close(sigChan)
		switch sig {
		case syscall.SIGINT:
			log.Println("🚨 CTRL+C (SIGINT) received. Forcing immediate shutdown...")
			os.Exit(0)
		case syscall.SIGTERM:
			log.Println("🚨 SIGTERM received. Forcing immediate shutdown...")
			os.Exit(0)
		}
	}()

	adminPass = generateRandomPassword(16)

	err = os.WriteFile("pwd.txt", []byte(adminPass), 0644)
	if err != nil {
		log.Printf("⚠️ Could not write admin password to file: %v", err)
	} else {
		log.Println("🔑 Admin password saved to pwd.txt")
	}

	go func() {
		time.Sleep(5 * time.Second)
		log.Println("==================================================")
		log.Printf("👤 Admin User: %s", adminUser)
		log.Printf("🔑 Admin Pass: %s", adminPass)
		log.Println("==================================================")
	}()

	go populateMissingCachesOnStartup()
	go startBackgroundJobs()

	wg.Add(2)
	go startDiscordBot(ctx, &wg)
	go startVisitorLogger(ctx, &wg)

	http.HandleFunc("/", visitorTracker(summaryHandler))
	http.HandleFunc("/full-list", visitorTracker(fullListHandler))
	http.HandleFunc("/item", visitorTracker(itemHistoryHandler))
	http.HandleFunc("/activity", visitorTracker(activityHandler))
	http.HandleFunc("/players", visitorTracker(playerCountHandler))
	http.HandleFunc("/characters", visitorTracker(characterHandler))
	http.HandleFunc("/guilds", visitorTracker(guildHandler))
	http.HandleFunc("/guild", visitorTracker(guildDetailHandler))
	http.HandleFunc("/mvp-kills", visitorTracker(mvpKillsHandler))
	http.HandleFunc("/character", visitorTracker(characterDetailHandler))
	http.HandleFunc("/character-changelog", visitorTracker(characterChangelogHandler))
	http.HandleFunc("/store", visitorTracker(storeDetailHandler))

	http.HandleFunc("/discord", visitorTracker(tradingPostListHandler))

	http.HandleFunc("/admin", basicAuth(adminHandler))
	http.HandleFunc("/admin/parse-trade", basicAuth(adminParseTradeHandler))
	http.HandleFunc("/admin/views/delete-visitor", basicAuth(adminDeleteVisitorViewsHandler))
	http.HandleFunc("/admin/cache", basicAuth(adminCacheActionHandler))
	http.HandleFunc("/admin/cache/delete-entry", basicAuth(adminDeleteCacheEntryHandler))

	http.HandleFunc("/admin/guild/update-emblem", basicAuth(adminUpdateGuildEmblemHandler))
	http.HandleFunc("/admin/character/clear-last-active", basicAuth(adminClearLastActiveHandler))
	http.HandleFunc("/admin/character/clear-mvp-kills", basicAuth(adminClearMvpKillsHandler))
	http.HandleFunc("/admin/trading-post/delete", basicAuth(adminDeleteTradingPostHandler))
	http.HandleFunc("/admin/trading-post/edit", basicAuth(adminEditTradingPostHandler))

	http.HandleFunc("/admin/trading-post/reparse", basicAuth(adminReparseTradingPostHandler))
	http.HandleFunc("/admin/trading/clear-items", basicAuth(adminClearTradingPostItemsHandler))
	http.HandleFunc("/admin/trading/clear-posts", basicAuth(adminClearTradingPostsHandler))

	http.HandleFunc("/admin/scrape/market", basicAuth(adminTriggerScrapeHandler(scrapeData, "Market")))
	http.HandleFunc("/admin/scrape/players", basicAuth(adminTriggerScrapeHandler(scrapeAndStorePlayerCount, "Player-Count")))
	http.HandleFunc("/admin/scrape/characters", basicAuth(adminTriggerScrapeHandler(scrapePlayerCharacters, "Character")))
	http.HandleFunc("/admin/scrape/guilds", basicAuth(adminTriggerScrapeHandler(scrapeGuilds, "Guild")))
	http.HandleFunc("/admin/scrape/zeny", basicAuth(adminTriggerScrapeHandler(scrapeZeny, "Zeny")))
	http.HandleFunc("/admin/scrape/mvp", basicAuth(adminTriggerScrapeHandler(scrapeMvpKills, "MVP")))
	http.HandleFunc("/admin/scrape/rms-cache", basicAuth(adminTriggerScrapeHandler(runFullRMSCacheJob, "RMS-Cache-Refresh")))

	port := "8080"
	server := &http.Server{Addr: ":" + port}

	go func() {
		log.Printf("🚀 Web server started. Open http://localhost:%s in your browser.", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Failed to start web server: %v", err)
		}
	}()

	<-ctx.Done()

	log.Println("🔌 Shutting down HTTP server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("⚠️ HTTP server shutdown error: %v", err)
	} else {
		log.Println("✅ HTTP server shut down gracefully.")
	}

	log.Println("⏳ Waiting for background processes to shut down...")
	wg.Wait()
	log.Println("✅ All processes shut down cleanly. Exiting.")
}
