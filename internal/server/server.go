// Package server is the main application package: HTTP handlers,
// scrapers, background jobs, the Discord bot, and Gemini-backed trade
// message parsing all live here. cmd/server/main.go is the entrypoint
// and just calls Run.
package server

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/denislee/yufa-mt/internal/config"
	"github.com/denislee/yufa-mt/internal/i18n"
	"github.com/denislee/yufa-mt/internal/middleware"
	"github.com/denislee/yufa-mt/internal/storage"
	"github.com/denislee/yufa-mt/internal/visitor"
)

// appConfig holds the validated config loaded by cmd/server/main.go and
// passed into Run. It mirrors srv.cfg for the legacy free-function
// scrape/discord/chat-capture call sites that don't yet thread srv
// through their signatures.
var appConfig *config.Config

// pageViewLog, visitorTracker, and the batch flusher live in
// visitor_logger.go.

// registerRoutes sets up all the HTTP handlers for the application.
func registerRoutes() *http.ServeMux {
	initStaticAssetHashes()
	mux := http.NewServeMux()

	// --- Public Routes ---
	// Wrap public routes with the visitorTracker middleware
	mux.HandleFunc("/", visitorTracker(summaryHandler))
	mux.HandleFunc("/full-list", visitorTracker(fullListHandler))
	mux.HandleFunc("/item", visitorTracker(itemHistoryHandler))
	mux.HandleFunc("/activity", visitorTracker(activityHandler))
	mux.HandleFunc("/players", visitorTracker(playerCountHandler))
	mux.HandleFunc("/characters", visitorTracker(characterHandler))
	mux.HandleFunc("/guilds", visitorTracker(guildHandler))
	mux.HandleFunc("/guild", visitorTracker(guildDetailHandler))
	mux.HandleFunc("/mvp-kills", visitorTracker(mvpKillsHandler))
	mux.HandleFunc("/character", visitorTracker(characterDetailHandler))
	mux.HandleFunc("/character-changelog", visitorTracker(characterChangelogHandler))
	mux.HandleFunc("/store", visitorTracker(storeDetailHandler))
	mux.HandleFunc("/discord", visitorTracker(tradingPostListHandler))
	mux.HandleFunc("/woe", visitorTracker(woeRankingsHandler))
	mux.HandleFunc("/chat", visitorTracker(chatHandler))
	mux.HandleFunc("/xp-calculator", visitorTracker(xpCalculatorHandler))
	mux.HandleFunc("/about", visitorTracker(aboutHandler))
	mux.HandleFunc("/set-lang", i18n.SetLangHandler)
	mux.HandleFunc("/search", visitorTracker(globalSearchHandler))
	mux.HandleFunc("/stats/drops", visitorTracker(dropStatsHandler))
	mux.HandleFunc("/stats/market", visitorTracker(marketStatsHandler))
	mux.HandleFunc("/stats/characters", visitorTracker(characterStatsHandler))

	// --- Static Assets ---
	// /static/* is served from in-memory pre-gzipped bytes (see
	// serveStaticAsset). Bypasses http.FileServer and the global gzip
	// middleware so the hot path is just a map lookup + Write.
	mux.HandleFunc("/static/", serveStaticAsset)

	// Processed guild emblems live on disk under data/runtime/emblems
	// (written by processGuildEmblems after each guild scrape).
	if dir := emblemDir(); dir != "" {
		mux.Handle("/emblems/", http.StripPrefix("/emblems/", cacheStatic(http.FileServer(http.Dir(dir)))))
	}

	// --- Admin Routes ---
	adminRouter := registerAdminRoutes()

	// Apply the basicAuth middleware to the entire admin router
	// Note the trailing slash on "/admin/" is important for sub-path matching
	mux.Handle("/admin/", middleware.BasicAuth(adminUser, adminPass, http.StripPrefix("/admin", adminRouter)))

	return mux
}

// cacheStatic adds cache headers to embedded static assets. When the
// request URL carries a content-hash query string (?v=…) that matches
// the known hash, we serve the response as immutable for one year so
// repeat visitors never re-fetch. Bare requests (no hash) fall back to
// a one-day cache. assetURL in assets.go appends the hash query string
// for all template-rendered asset references.
func cacheStatic(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hasAssetHash(r.URL.Path, r.URL.RawQuery) {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=86400")
		}
		h.ServeHTTP(w, r)
	})
}

// registerAdminRoutes creates a sub-router for all admin-facing endpoints.
func registerAdminRoutes() *http.ServeMux {
	adminRouter := http.NewServeMux()

	// Core Admin
	adminRouter.HandleFunc("/", adminHandler) // Handles /admin/
	adminRouter.HandleFunc("/parse-trade", adminParseTradeHandler)
	adminRouter.HandleFunc("/views/delete-visitor", adminDeleteVisitorViewsHandler)
	adminRouter.HandleFunc("/guild/update-emblem", adminUpdateGuildEmblemHandler)
	adminRouter.HandleFunc("/character/clear-last-active", adminClearLastActiveHandler)
	adminRouter.HandleFunc("/character/clear-mvp-kills", adminClearMvpKillsHandler)
	adminRouter.HandleFunc("/backfill/drops", adminBackfillDropLogsHandler)

	// Admin RMS Cache Management
	adminRouter.HandleFunc("/cache", adminCacheActionHandler)
	adminRouter.HandleFunc("/cache/delete-entry", adminDeleteCacheEntryHandler)
	adminRouter.HandleFunc("/cache/save-entry", adminSaveCacheEntryHandler)

	// Admin Trading Post Management
	adminRouter.HandleFunc("/trading-post/delete", adminDeleteTradingPostHandler)
	adminRouter.HandleFunc("/trading-post/edit", adminEditTradingPostHandler)
	adminRouter.HandleFunc("/trading-post/reparse", adminReparseTradingPostHandler)
	adminRouter.HandleFunc("/trading/clear-items", adminClearTradingPostItemsHandler)
	adminRouter.HandleFunc("/trading/clear-posts", adminClearTradingPostsHandler)

	// Admin Manual Scrape Triggers
	adminRouter.HandleFunc("/scrape/market", adminTriggerScrapeHandler(scrapeData, "Market"))
	adminRouter.HandleFunc("/scrape/players", adminTriggerScrapeHandler(scrapeAndStorePlayerCount, "Player-Count"))
	adminRouter.HandleFunc("/scrape/characters", adminTriggerScrapeHandler(scrapePlayerCharacters, "Character"))
	adminRouter.HandleFunc("/scrape/guilds", adminTriggerScrapeHandler(scrapeGuilds, "Guild"))
	adminRouter.HandleFunc("/scrape/emblems", adminTriggerScrapeHandler(processGuildEmblems, "Emblem-Process"))
	adminRouter.HandleFunc("/scrape/zeny", adminTriggerScrapeHandler(scrapeZeny, "Zeny"))
	adminRouter.HandleFunc("/scrape/mvp", adminTriggerScrapeHandler(scrapeMvpKills, "MVP"))
	adminRouter.HandleFunc("/scrape/pt-names", adminTriggerScrapeHandler(populateMissingPortugueseNames, "PT-Name-Populator"))
	adminRouter.HandleFunc("/scrape/woe", adminTriggerScrapeHandler(scrapeWoeCharacterRankings, "WoE-Char-Rankings"))

	// Admin Chat Management
	adminRouter.HandleFunc("/chat/delete", adminDeleteChatHandler)
	adminRouter.HandleFunc("/chat/edit", adminEditChatHandler)

	adminRouter.HandleFunc("/cleanup/guild-history", adminCleanupGuildHistoryHandler)

	return adminRouter
}

// Run starts the application: opens the database, hydrates the item DB,
// launches background services (scrapers, Discord bot, visitor logger),
// serves HTTP, and blocks until SIGINT/SIGTERM triggers a graceful
// shutdown. Config is loaded by the caller (cmd/server/main.go) and
// passed in.
func Run(cfg *config.Config) {
	appConfig = cfg
	initLogger()

	dbh, err := initDB(cfg.DBPath)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	srv = &App{db: dbh, cfg: cfg}
	visitorLogger = visitor.New(dbh)
	defer func() {
		if err := storage.Close(srv.db); err != nil {
			slog.Error("Failed to close database", "error", err)
		}
	}()

	// Run this synchronously on startup before starting other services
	populateItemDBOnStartup()

	// Create a context that gets cancelled on OS signals (SIGINT, SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine to listen for OS signals
	go func() {
		<-sigChan
		slog.Info("Shutdown signal received. Initiating graceful shutdown...")
		cancel() // Trigger context cancellation
	}()

	// --- Admin Password Logic ---
	adminUser = cfg.AdminUser
	adminPass = cfg.AdminPassword
	if adminPass == "" {
		slog.Info("ADMIN_PASSWORD not set. Generating a new random password (will be printed once, below).")
		adminPass = generateRandomPassword(16)
	} else {
		slog.Info("Loaded admin password from ADMIN_PASSWORD environment variable.")
	}

	slog.Info("==================================================")
	slog.Info("Admin Credentials", "user", adminUser, "pass", adminPass)
	slog.Info("==================================================")

	// Start Background Services with the cancellable context. The WaitGroup
	// lets Run block on a clean shutdown of every background goroutine
	// (visitor logger drains its channel, scrape jobs finish in-flight
	// requests, discord bot closes its session) before returning.
	var bgWg sync.WaitGroup
	startBackgroundJobs(ctx, &bgWg)
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		startDiscordBot(ctx)
	}()
	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		startVisitorLogger(ctx)
	}()

	// --- Setup Routers ---
	mux := registerRoutes()

	// --- Server Start and Shutdown ---
	// Wrap dynamic routes in the per-request Gzip middleware, but route
	// pre-encoded /static/* and binary /emblems/* around it: serveStaticAsset
	// already writes a pre-gzipped body and emblems are PNG/JPG so re-gzipping
	// is pure waste. Both paths still get cache headers from their handlers.
	gzWrapped := middleware.EarlyHints(earlyHintLinks(), middleware.ServerTiming(middleware.Gzip(mux)))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if strings.HasPrefix(p, "/static/") || strings.HasPrefix(p, "/emblems/") {
			mux.ServeHTTP(w, r)
			return
		}
		gzWrapped.ServeHTTP(w, r)
	})
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: handler}

	// Goroutine to handle server shutdown when context is cancelled
	go func() {
		<-ctx.Done() // Wait for the cancel() signal
		slog.Info("Shutting down web server...")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("Web server graceful shutdown failed", "error", err)
		}
	}()

	// Start server and block
	slog.Info("Web server started", "addr", cfg.HTTPAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("Web server failed to start", "error", err)
		os.Exit(1)
	}

	// HTTP server has finished shutting down. Now wait for background
	// services to drain so the visitor logger flushes its final batch.
	slog.Info("Waiting for background services to drain...")
	bgWg.Wait()

	slog.Info("All services shut down. Exiting.")
}
