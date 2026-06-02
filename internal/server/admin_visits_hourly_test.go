package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdminVisitsHourly(t *testing.T) {
	setupSchedulerTestDB(t)
	now := time.Now()
	rfc := func(d time.Duration) string { return now.Add(d).Format(time.RFC3339) }
	// 3 views this hour (2 distinct visitors), 1 view two hours ago.
	_, err := srv.db.Exec(`
		INSERT INTO page_views (page_path, visitor_hash, view_timestamp) VALUES
		('/', 'a', ?), ('/players', 'a', ?), ('/', 'b', ?), ('/', 'c', ?)`,
		rfc(0), rfc(-1*time.Minute), rfc(-2*time.Minute), rfc(-2*time.Hour))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/visits-hourly?hours=24", nil)
	rr := httptest.NewRecorder()
	adminVisitsHourlyHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body.String())
	}
	var got []HourlyVisit
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 24 {
		t.Fatalf("expected 24 buckets, got %d", len(got))
	}
	if last := got[len(got)-1]; last.Views != 3 || last.Visitors != 2 {
		t.Errorf("current hour: got views=%d visitors=%d, want 3/2", last.Views, last.Visitors)
	}
	if twoAgo := got[len(got)-3]; twoAgo.Views != 1 {
		t.Errorf("two hours ago: got views=%d, want 1", twoAgo.Views)
	}
}
