package server

import (
	"strings"
	"testing"
)

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

func TestItemCacheOperations(t *testing.T) {
	// Initialize cache with mock values
	itemCacheMu.Lock()
	itemExactCache = make(map[string]int64)
	itemFuzzyCache = []cachedItem{
		{id: 501, name: "Red Potion", namePT: "", slots: 0},
		{id: 502, name: "Blue Potion", namePT: "Poção Azul", slots: 1},
	}
	itemExactCache["red potion_0"] = 501
	itemExactCache["blue potion_1"] = 502
	itemExactCache["poção azul_1"] = 502
	itemCacheLoaded = true
	itemCacheMu.Unlock()

	// Test findItemIDInCache for exact match
	idNull, found := findItemIDInCache("Red Potion", 0)
	if !found || !idNull.Valid || idNull.Int64 != 501 {
		t.Errorf("Expected to find Red Potion (0 slots) in cache with ID 501, got %v, %v", idNull, found)
	}

	// Test findItemIDInCache for exact PT match
	idNull, found = findItemIDInCache("Poção Azul", 1)
	if !found || !idNull.Valid || idNull.Int64 != 502 {
		t.Errorf("Expected to find Poção Azul (1 slot) in cache with ID 502, got %v, %v", idNull, found)
	}

	// Test updateItemInCache
	updateItemInCache(501, "Poção Vermelha")

	// Verify that the new translation is available in cache
	idNull, found = findItemIDInCache("Poção Vermelha", 0)
	if !found || !idNull.Valid || idNull.Int64 != 501 {
		t.Errorf("Expected to find dynamically updated Poção Vermelha in cache with ID 501, got %v, %v", idNull, found)
	}

	// Verify that the old english name still works too
	idNull, found = findItemIDInCache("Red Potion", 0)
	if !found || !idNull.Valid || idNull.Int64 != 501 {
		t.Errorf("Expected to still find Red Potion in cache with ID 501, got %v, %v", idNull, found)
	}
}
