package server

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
)

// divinePrideMapImage returns the public URL of the rendered map image for a
// given rAthena map name. divine-pride serves these "original" renders at
// game-cell resolution, so cell (x, y) maps to image pixel (x, height-y).
func divinePrideMapImage(mapName string) string {
	return "https://www.divine-pride.net/img/map/original/" + strings.ToLower(strings.TrimSpace(mapName))
}

// reMapCoords pulls the X and Y cell out of a stored "(123,45)" coordinate.
var reMapCoords = regexp.MustCompile(`\(\s*(\d+)\s*,\s*(\d+)\s*\)`)

func parseMapCoords(s string) (int, int, bool) {
	m := reMapCoords.FindStringSubmatch(s)
	if m == nil {
		return 0, 0, false
	}
	return atoiSafe(m[1]), atoiSafe(m[2]), true
}

func atoiSafe(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// vendorsHandler renders the vendor-map page. With no ?map= query it lists
// every map that currently has active vendings; with ?map=<name> it renders
// the interactive Leaflet view for that single map (the vendor markers are
// fetched client-side from vendorsDataHandler).
func vendorsHandler(w http.ResponseWriter, r *http.Request) {
	selected := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("map")))

	// Index: one card per map with active stores, ordered by store count.
	rows, err := srv.db.Query(`
		SELECT lower(map_name) AS m,
		       COUNT(DISTINCT seller_name || '|' || store_name) AS stores,
		       COUNT(*) AS listings
		FROM items
		WHERE is_available = 1 AND map_name != ''
		GROUP BY m
		ORDER BY stores DESC, m ASC`)
	if err != nil {
		http.Error(w, "Could not query vendor maps", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var maps []VendorMapSummary
	totalStores := 0
	for rows.Next() {
		var m VendorMapSummary
		if err := rows.Scan(&m.MapName, &m.StoreCount, &m.Listings); err != nil {
			continue
		}
		m.ImageURL = divinePrideMapImage(m.MapName)
		totalStores += m.StoreCount
		maps = append(maps, m)
	}

	// Distinct vendor cell positions per map, for the thumbnail red dots.
	coordsByMap := map[string][][2]int{}
	if crows, cerr := srv.db.Query(`
		SELECT lower(map_name) AS m, map_coordinates
		FROM items
		WHERE is_available = 1 AND map_name != ''
		GROUP BY m, map_coordinates`); cerr == nil {
		defer crows.Close()
		for crows.Next() {
			var m, c string
			if err := crows.Scan(&m, &c); err != nil {
				continue
			}
			if x, y, ok := parseMapCoords(c); ok {
				coordsByMap[m] = append(coordsByMap[m], [2]int{x, y})
			}
		}
	}
	for i := range maps {
		if pts := coordsByMap[maps[i].MapName]; len(pts) > 0 {
			if b, err := json.Marshal(pts); err == nil {
				maps[i].CoordsJSON = string(b)
			}
		}
	}

	data := VendorsPageData{
		PageTitle:      "Vendor Map",
		SelectedMap:    selected,
		Maps:           maps,
		StoreCount:     totalStores,
		LastScrapeTime: GetLastScrapeTime(),
	}
	if selected != "" {
		data.MapImageURL = divinePrideMapImage(selected)
	}
	renderTemplate(w, r, "vendors.html", data)
}

// vendorsDataHandler returns the active vending stalls on a single map as
// JSON, one entry per stall with its cell coordinates and item list. The
// Leaflet front-end consumes this to place markers.
func vendorsDataHandler(w http.ResponseWriter, r *http.Request) {
	mapName := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("map")))
	if mapName == "" {
		http.Error(w, "map is required", http.StatusBadRequest)
		return
	}

	// Latest row per (seller, shop, item) so a stall isn't duplicated by
	// historical snapshots that are still flagged available.
	rows, err := srv.db.Query(`
		WITH Ranked AS (
			SELECT i.seller_name, i.store_name, i.map_coordinates,
			       i.item_id, i.name_of_the_item, d.name_pt, i.quantity, i.price,
			       ROW_NUMBER() OVER (
			           PARTITION BY i.seller_name, i.store_name, i.name_of_the_item
			           ORDER BY i.id DESC) AS rn
			FROM items i
			LEFT JOIN internal_item_db d ON i.item_id = d.item_id
			WHERE lower(i.map_name) = ? AND i.is_available = 1
		)
		SELECT seller_name, store_name, map_coordinates, item_id, name_of_the_item, name_pt, quantity, price
		FROM Ranked WHERE rn = 1
		ORDER BY seller_name, store_name`, mapName)
	if err != nil {
		http.Error(w, "Could not query vendors", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Group rows into stalls keyed by seller+shop+coords.
	order := []string{}
	stalls := map[string]*VendorStoreJSON{}
	for rows.Next() {
		var seller, shop, coords, name, price string
		var namePT sql.NullString
		var itemID, qty int
		if err := rows.Scan(&seller, &shop, &coords, &itemID, &name, &namePT, &qty, &price); err != nil {
			continue
		}
		x, y, ok := parseMapCoords(coords)
		if !ok {
			continue
		}
		key := seller + "|" + shop + "|" + coords
		s, exists := stalls[key]
		if !exists {
			s = &VendorStoreJSON{Seller: seller, Shop: shop, X: x, Y: y}
			stalls[key] = s
			order = append(order, key)
		}
		display := name
		if namePT.Valid && namePT.String != "" {
			display = namePT.String
		}
		s.Items = append(s.Items, VendorItemJSON{ItemID: itemID, Name: display, Qty: qty, Price: price})
	}

	out := make([]*VendorStoreJSON, 0, len(order))
	for _, k := range order {
		out = append(out, stalls[k])
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_ = json.NewEncoder(w).Encode(out)
}
