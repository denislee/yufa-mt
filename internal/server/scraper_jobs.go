package server

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

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

	slog.Info("Starting initial background job run", "job", job.Name)
	go func() {
		// Run initial run immediately on startup in a separate goroutine so it doesn't block other tickers starting
		job.Func()
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopping background job due to shutdown", "job", job.Name)
			return
		case <-ticker.C:
			slog.Info("Starting scheduled background job scrape", "job", job.Name)
			job.Func()
		}
	}
}

func startBackgroundJobs(ctx context.Context, wg *sync.WaitGroup) {
	// The in-process character-select PIN proxy runs independently of the
	// scrape/capture flags: it installs an iptables REDIRECT and injects the
	// PIN so the game client clears the keypad with no click. Start it first so
	// it's up before the listener logs the client in. (No-op unless PIN_PROXY=1
	// and, off Linux, a logged stub.)
	if appConfig != nil && appConfig.PinProxyEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			startPinProxy(ctx)
		}()
	}

	chatOnly := appConfig != nil && appConfig.ChatCaptureOnly

	// DISABLE_SCRAPERS turns off everything, including chat capture — unless
	// CHAT_CAPTURE_ONLY is also set, in which case chat capture still runs.
	if appConfig != nil && appConfig.DisableScrapers && !chatOnly {
		slog.Info("DISABLE_SCRAPERS is set; skipping all background scrape jobs and chat packet capture")
		return
	}

	if chatOnly {
		slog.Info("CHAT_CAPTURE_ONLY is set; running chat packet capture only, all scrape jobs disabled")
	} else {
		// Define all scheduled jobs
		jobs := []Job{
			{Name: "Market", Func: scrapeData, Interval: 3 * time.Minute},
			{Name: "Player Count", Func: scrapeAndStorePlayerCount, Interval: 1 * time.Minute},
			{Name: "Player Character", Func: scrapePlayerCharacters, Interval: 6 * time.Hour},
			{Name: "Guild", Func: scrapeGuilds, Interval: 1 * time.Hour},
			{Name: "Zeny", Func: scrapeZeny, Interval: 6 * time.Hour},
			{Name: "MVP Kill", Func: scrapeMvpKills, Interval: 5 * time.Minute},
			// {Name: "PT-Name-Populator", Func: populateMissingPortugueseNames, Interval: 6 * time.Hour},
			{Name: "WoE-Char-Rankings", Func: scrapeWoeCharacterRankings, Interval: 12 * time.Hour},
		}

		for _, job := range jobs {
			wg.Add(1)
			go func(j Job) {
				defer wg.Done()
				runJobOnTicker(ctx, j)
			}(job)
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		startChatPacketCapture(ctx)
	}()
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
