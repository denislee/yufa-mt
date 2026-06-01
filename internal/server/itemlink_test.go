package server

import (
	"strings"
	"testing"
)

func TestBase62Decode(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"ap", 645, true},   // OpenKore reference example: item 645
		{"zD", 2209, true},  // 35*62 + 39
		{"15Q", 4206, true}, // 1*62^2 + 5*62 + 52
		{"00", 0, true},
		{"", 0, false},
		{"!", 0, false}, // not in alphabet
	}
	for _, c := range cases {
		got, ok := base62Decode(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("base62Decode(%q) = (%d, %v), want (%d, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestDecodeItemLink(t *testing.T) {
	// Verified sample: +4 Hat (2209) with a Drooping Kitty Card (4206).
	d := decodeItemLink("000481zD%04&0h'00)15Q)00)00)00")
	if !d.ok {
		t.Fatalf("expected decode to succeed")
	}
	if d.itemID != 2209 {
		t.Errorf("itemID = %d, want 2209", d.itemID)
	}
	if d.refine != 4 {
		t.Errorf("refine = %d, want 4", d.refine)
	}
	if len(d.cards) != 1 || d.cards[0] != 4206 {
		t.Errorf("cards = %v, want [4206]", d.cards)
	}

	// Too short to carry a 5-char link id + flag + item id.
	if decodeItemLink("00000").ok {
		t.Errorf("expected short blob to fail decode")
	}
}

func TestRenderChatMessage(t *testing.T) {
	withItemCache(t, []cachedItem{
		{id: 2209, name: "Hat"},
		{id: 4206, name: "Drooping Kitty Card"},
	})

	out := string(renderChatMessage(`look <ITEML>000481zD%04&0h'00)15Q)00)00)00</ITEML> nice`, "en"))
	if !strings.Contains(out, "[+4 Hat (Drooping Kitty Card)]") {
		t.Errorf("decoded label missing in %q", out)
	}
	if !strings.Contains(out, `/item?name=Hat`) {
		t.Errorf("item link href missing in %q", out)
	}

	// Plain text with no link is HTML-escaped, not passed through raw.
	esc := string(renderChatMessage(`a <b> & c`, "en"))
	if strings.Contains(esc, "<b>") || !strings.Contains(esc, "&lt;b&gt;") {
		t.Errorf("plain message not escaped: %q", esc)
	}

	// Unknown item id falls back to a non-linked marker.
	unknown := string(renderChatMessage(`<ITEML>00000099zz</ITEML>`, "en"))
	if strings.Contains(unknown, "href=") {
		t.Errorf("unknown item should not be linked: %q", unknown)
	}
}

// withItemCache installs a fixed item cache for the duration of a test.
func withItemCache(t *testing.T, items []cachedItem) {
	t.Helper()
	itemCacheMu.Lock()
	defer itemCacheMu.Unlock()
	itemByIDCache = make(map[int64]cachedItem, len(items))
	for _, it := range items {
		itemByIDCache[it.id] = it
	}
	itemCacheLoaded = true
}
