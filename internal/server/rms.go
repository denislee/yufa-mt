package server

import (
	"github.com/denislee/yufa-mt/internal/rms"
)

// getItemDetailsFromCache adapts internal/rms.Lookup to the main-package
// RMSItem type. DroppedBy and ObtainableFrom are not populated from the
// local DB.
func getItemDetailsFromCache(itemID int) (*RMSItem, error) {
	it, err := rms.Lookup(srv.db, itemID)
	if err != nil {
		return nil, err
	}
	return &RMSItem{
		ID:          it.ID,
		Name:        it.Name,
		NamePT:      it.NamePT,
		ImageURL:    it.ImageURL,
		Type:        it.Type,
		Buy:         it.Buy,
		Sell:        it.Sell,
		Weight:      it.Weight,
		Slots:       it.Slots,
		Script:      it.Script,
		Class:       it.Class,
		Prefix:      it.Prefix,
		Description: it.Description,
	}, nil
}

func scrapeRODatabaseSearch(query string, slots int) ([]ItemSearchResult, error) {
	results, err := rms.Search(query, slots)
	if err != nil {
		return nil, err
	}
	out := make([]ItemSearchResult, 0, len(results))
	for _, r := range results {
		out = append(out, ItemSearchResult{ID: r.ID, Name: r.Name, ImageURL: r.ImageURL})
	}
	return out, nil
}
