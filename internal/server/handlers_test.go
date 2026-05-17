package server

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewPaginationData(t *testing.T) {
	tests := []struct {
		name         string
		page         string
		total        int
		perPage      int
		wantCurrent  int
		wantTotalPgs int
		wantHasPrev  bool
		wantHasNext  bool
		wantOffset   int
	}{
		{"empty", "", 0, 10, 1, 1, false, false, 0},
		{"first page", "1", 50, 10, 1, 5, false, true, 0},
		{"middle page", "3", 50, 10, 3, 5, true, true, 20},
		{"last page", "5", 50, 10, 5, 5, true, false, 40},
		{"clamp over", "99", 50, 10, 5, 5, true, false, 40},
		{"clamp under", "-1", 50, 10, 1, 5, false, true, 0},
		{"non-numeric", "abc", 50, 10, 1, 5, false, true, 0},
		{"exact multiple", "2", 20, 10, 2, 2, true, false, 10},
		{"single overflow", "1", 11, 10, 1, 2, false, true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/?page="+tc.page, nil)
			pd := newPaginationData(r, tc.total, tc.perPage)
			if pd.CurrentPage != tc.wantCurrent {
				t.Errorf("CurrentPage=%d want %d", pd.CurrentPage, tc.wantCurrent)
			}
			if pd.TotalPages != tc.wantTotalPgs {
				t.Errorf("TotalPages=%d want %d", pd.TotalPages, tc.wantTotalPgs)
			}
			if pd.HasPrevPage != tc.wantHasPrev {
				t.Errorf("HasPrevPage=%v want %v", pd.HasPrevPage, tc.wantHasPrev)
			}
			if pd.HasNextPage != tc.wantHasNext {
				t.Errorf("HasNextPage=%v want %v", pd.HasNextPage, tc.wantHasNext)
			}
			if pd.Offset != tc.wantOffset {
				t.Errorf("Offset=%d want %d", pd.Offset, tc.wantOffset)
			}
		})
	}
}

func TestGetSortClause(t *testing.T) {
	allowed := map[string]string{
		"name":  "n.name",
		"price": "i.price",
	}

	tests := []struct {
		name         string
		query        string
		wantClause   string
		wantSortBy   string
		wantOrder    string
	}{
		{"defaults", "", "ORDER BY n.name ASC", "name", "ASC"},
		{"valid sort+order", "sort_by=price&order=DESC", "ORDER BY i.price DESC", "price", "DESC"},
		{"invalid sort falls back", "sort_by=bogus", "ORDER BY n.name ASC", "name", "ASC"},
		{"invalid order falls back", "sort_by=price&order=funny", "ORDER BY i.price ASC", "price", "ASC"},
		{"order lowercase", "sort_by=price&order=asc", "ORDER BY i.price asc", "price", "asc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/?"+tc.query, nil)
			clause, sortBy, order := getSortClause(r, allowed, "name", "ASC")
			if clause != tc.wantClause {
				t.Errorf("clause=%q want %q", clause, tc.wantClause)
			}
			if sortBy != tc.wantSortBy {
				t.Errorf("sortBy=%q want %q", sortBy, tc.wantSortBy)
			}
			if order != tc.wantOrder {
				t.Errorf("order=%q want %q", order, tc.wantOrder)
			}
		})
	}
}

func TestFormatZeny(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{99, "99"},
		{999, "999"},
		{1000, "1.000"},
		{12345, "12.345"},
		{123456, "123.456"},
		{1234567, "1.234.567"},
		{1000000, "1.000.000"},
		{100000000, "100.000.000"},
	}
	for _, tc := range tests {
		if got := formatZeny(tc.in); got != tc.want {
			t.Errorf("formatZeny(%d)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestFormatRMT(t *testing.T) {
	if got := formatRMT(0); got != "R$ 0" {
		t.Errorf("formatRMT(0)=%q want R$ 0", got)
	}
	if got := formatRMT(42); got != "R$ 42" {
		t.Errorf("formatRMT(42)=%q want R$ 42", got)
	}
}

func TestCapitalizeASCII(t *testing.T) {
	cases := map[string]string{
		"":        "",
		"a":       "A",
		"buying":  "Buying",
		"selling": "Selling",
		"already": "Already",
	}
	for in, want := range cases {
		if got := capitalizeASCII(in); got != want {
			t.Errorf("capitalizeASCII(%q)=%q want %q", in, got, want)
		}
	}
}

func TestCleanCardName(t *testing.T) {
	cases := map[string]string{
		"Hydra Card":  "Hydra",
		"Carta Hydra": "Hydra",
		"":            "",
		"Card Card":   "",
	}
	for in, want := range cases {
		got := strings.TrimSpace(cleanCardName(in))
		if got != want {
			t.Errorf("cleanCardName(%q)=%q want %q", in, got, want)
		}
	}
}

func TestQueryCountHelperSignature(t *testing.T) {
	// queryCount is exercised end-to-end by handler tests under a real DB;
	// this guard test just pins the signature so future refactors notice.
	var _ = queryCount
}
