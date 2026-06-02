package server

import (
	"context"
	"log/slog"
	"sync"
)

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

	// The zone proxy is the sibling of the PIN proxy on the zone/map port: it
	// relays the live game connection and lets the mob-info scrape inject
	// @mobinfo commands. It defaults ON (unlike the opt-in PIN proxy), so we
	// guard it with DISABLE_SCRAPERS: a dev instance (`make run`) must not
	// install an iptables REDIRECT. In production it comes up before the client
	// connects. Set ZONE_PROXY=0 to force it off even in production.
	if appConfig != nil && appConfig.ZoneProxyEnabled && !appConfig.DisableScrapers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			startZoneProxy(ctx)
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
		// Any run still marked 'running' belongs to the previous process that
		// exited mid-run; rewrite those rows before we start recording new ones.
		markInterruptedRuns()

		// The scheduler owns all scrape jobs: it hydrates each job's interval
		// and enabled flag from job_config, runs them on live-adjustable
		// tickers, and records every run (success/failure) in job_runs. The
		// admin "Schedulers" tab drives it. See scheduler.go.
		globalScheduler = newScheduler(jobRegistry)
		globalScheduler.Start(ctx, wg)
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
