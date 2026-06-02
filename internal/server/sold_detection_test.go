package server

import "testing"

// mkItem builds a market listing for the classifier tests.
func mkItem(seller string, itemID, qty int, price, store string) Item {
	return Item{
		Name:           "Apple",
		ItemID:         itemID,
		Quantity:       qty,
		Price:          price,
		StoreName:      store,
		SellerName:     seller,
		MapName:        "prontera",
		MapCoordinates: "(100,100)",
	}
}

func TestClassifyListingChanges(t *testing.T) {
	const seller = "Alice"
	online := map[string]bool{seller: true}
	offline := map[string]bool{}
	sizes := map[string]int{seller: 2} // multi-item store unless a test overrides

	type want struct {
		eventType string
		quantity  int
		remaining int
		price     string
	}

	tests := []struct {
		name    string
		current []Item
		last    []Item
		sellers map[string]bool
		sizes   map[string]int
		want    []want // empty => expect no events
	}{
		{
			name:    "reprice by online seller is not a sale",
			last:    []Item{mkItem(seller, 501, 100, "1,000", "Shop")},
			current: []Item{mkItem(seller, 501, 100, "900", "Shop")},
			sellers: online, sizes: sizes,
			want: nil,
		},
		{
			name:    "restock by online seller is not a sale",
			last:    []Item{mkItem(seller, 501, 50, "1,000", "Shop")},
			current: []Item{mkItem(seller, 501, 80, "1,000", "Shop")},
			sellers: online, sizes: sizes,
			want: nil,
		},
		{
			name:    "relocate by online seller is not a sale",
			last:    []Item{mkItem(seller, 501, 50, "1,000", "Shop")},
			current: []Item{func() Item { i := mkItem(seller, 501, 50, "1,000", "Shop"); i.MapCoordinates = "(50,50)"; return i }()},
			sellers: online, sizes: sizes,
			want: nil,
		},
		{
			name:    "partial sale records the delta and remaining units",
			last:    []Item{mkItem(seller, 501, 100, "1,000", "Shop")},
			current: []Item{mkItem(seller, 501, 90, "1,000", "Shop")},
			sellers: online, sizes: sizes,
			want: []want{{eventType: "SOLD", quantity: 10, remaining: 90, price: "1,000"}},
		},
		{
			name:    "full sale by online seller logs the whole stack",
			last:    []Item{mkItem(seller, 501, 100, "1,000", "Shop")},
			current: nil,
			sellers: online, sizes: sizes,
			want: []want{{eventType: "SOLD", quantity: 100, remaining: 0, price: "1,000"}},
		},
		{
			name:    "offline seller with multiple items is REMOVED",
			last:    []Item{mkItem(seller, 501, 100, "1,000", "Shop")},
			current: nil,
			sellers: offline, sizes: map[string]int{seller: 3},
			want: []want{{eventType: "REMOVED", quantity: 100, price: "1,000"}},
		},
		{
			name:    "offline seller with a single item is REMOVED_SINGLE",
			last:    []Item{mkItem(seller, 501, 100, "1,000", "Shop")},
			current: nil,
			sellers: offline, sizes: map[string]int{seller: 1},
			want: []want{{eventType: "REMOVED_SINGLE", quantity: 100, price: "1,000"}},
		},
		{
			name: "multi-slot: only the sold-out slot is a sale, at its own price",
			last: []Item{
				mkItem(seller, 501, 100, "1,000", "Shop"),
				mkItem(seller, 501, 50, "2,000", "Shop"),
			},
			current: []Item{mkItem(seller, 501, 100, "1,000", "Shop")},
			sellers: online, sizes: sizes,
			want: []want{{eventType: "SOLD", quantity: 50, remaining: 0, price: "2,000"}},
		},
		{
			name:    "untouched listing produces no event",
			last:    []Item{mkItem(seller, 501, 100, "1,000", "Shop")},
			current: []Item{mkItem(seller, 501, 100, "1,000", "Shop")},
			sellers: online, sizes: sizes,
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyListingChanges(tc.current, tc.last, tc.sellers, tc.sizes)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d events %+v, want %d", len(got), got, len(tc.want))
			}
			for i, w := range tc.want {
				g := got[i]
				if g.EventType != w.eventType || g.Quantity != w.quantity || g.Remaining != w.remaining || g.Price != w.price {
					t.Errorf("event %d = {%s qty=%d rem=%d price=%s}, want {%s qty=%d rem=%d price=%s}",
						i, g.EventType, g.Quantity, g.Remaining, g.Price,
						w.eventType, w.quantity, w.remaining, w.price)
				}
			}
		})
	}
}
