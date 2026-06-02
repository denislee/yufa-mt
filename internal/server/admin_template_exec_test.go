package server

import (
	"io"
	"testing"
)

// TestAdminTemplateExecutes renders admin.html the same way adminHandler does
// (tmpl.Execute with an *AdminDashboardData). Unlike a parse check, executing
// surfaces bad field references — e.g. a typo in the .MobScrape control card —
// which html/template only reports at execution time.
func TestAdminTemplateExecutes(t *testing.T) {
	tmpl, ok := templateCache["admin.html"]
	if !ok || tmpl == nil {
		t.Fatal("admin.html not in templateCache")
	}
	data := &AdminDashboardData{
		SchedulerJobs: []SchedulerJobView{{
			Name: "mobinfo", Label: "Mob Info Scrape (@mobinfo)", Category: "Reference Data",
			Enabled: false, IntervalValue: 24, IntervalUnit: "hours", IntervalText: "24h0m0s",
		}},
		MobScrape: MobScrapeView{
			FromID: 1001, ToID: 2500, DelayMs: 400,
			ZoneEnabled: true, ZoneReady: true, CharName: "3com",
			Running: true, Current: 1042, Sent: 41, StartedAgo: "2 minutes ago",
		},
	}
	if err := tmpl.Execute(io.Discard, data); err != nil {
		t.Fatalf("admin.html execute failed: %v", err)
	}
}
