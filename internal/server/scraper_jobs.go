package server

import (
	"context"
	"log"
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

	log.Printf("[I] [Job] Starting initial run for %s job...", job.Name)

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

func startBackgroundJobs(ctx context.Context, wg *sync.WaitGroup) {
	if appConfig != nil && appConfig.DisableScrapers {
		log.Println("[I] [Job] DISABLE_SCRAPERS is set; skipping all scrape jobs and chat packet capture.")
		return
	}
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
