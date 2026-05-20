package httpx

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
)

type PaginationData struct {
	CurrentPage  int
	TotalPages   int
	PrevPage     int
	NextPage     int
	HasPrevPage  bool
	HasNextPage  bool
	Offset       int
	ItemsPerPage int
}

// NewPaginationData creates a pagination object based on the request and total items.
func NewPaginationData(r *http.Request, totalItems int, itemsPerPage int) PaginationData {
	pageStr := r.FormValue("page")
	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	pd := PaginationData{
		ItemsPerPage: itemsPerPage,
	}

	if totalItems <= 0 {
		pd.TotalPages = 1
		pd.CurrentPage = 1
	} else {
		pd.TotalPages = int(math.Ceil(float64(totalItems) / float64(itemsPerPage)))
		// Clamp page to be within valid bounds
		if page > pd.TotalPages {
			page = pd.TotalPages
		}
		pd.CurrentPage = page
	}

	pd.Offset = (pd.CurrentPage - 1) * itemsPerPage
	pd.PrevPage = pd.CurrentPage - 1
	pd.NextPage = pd.CurrentPage + 1
	pd.HasPrevPage = pd.CurrentPage > 1
	pd.HasNextPage = pd.CurrentPage < pd.TotalPages

	return pd
}

// GetSortClause validates and constructs a SQL ORDER BY clause from the request query.
func GetSortClause(r *http.Request, allowedSorts map[string]string, defaultSortBy, defaultOrder string) (string, string, string) {
	sortBy := r.FormValue("sort_by")
	order := r.FormValue("order")

	orderByColumn, ok := allowedSorts[sortBy]
	if !ok {
		sortBy = defaultSortBy
		order = defaultOrder
		orderByColumn = allowedSorts[sortBy]
	}

	if strings.ToUpper(order) != "ASC" && strings.ToUpper(order) != "DESC" {
		order = defaultOrder
	}

	return fmt.Sprintf("ORDER BY %s %s", orderByColumn, order), sortBy, order
}
