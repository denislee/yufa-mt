package main

import (
	"database/sql"
	"html/template"
	"time"
)

// Item represents a single listing scraped from the market.
type Item struct {
	ID             int
	Name           string
	ItemID         int
	Quantity       int
	Price          string
	StoreName      string
	SellerName     string
	Timestamp      string
	MapName        string
	MapCoordinates string
	IsAvailable    bool
}

// comparableItem is a version of Item used for checking for differences without considering the ID or Timestamp.
type comparableItem struct {
	Name           string
	ItemID         int
	Quantity       int
	Price          string
	StoreName      string
	SellerName     string
	MapName        string
	MapCoordinates string
}

// Column defines a toggleable column in the full list view.
type Column struct {
	ID          string
	DisplayName string
}

// MarketEvent logs a change in the market, such as an item being added or removed.
type MarketEvent struct {
	Timestamp string
	EventType string
	ItemName  string
	ItemID    int
	Details   map[string]interface{}
}

// ItemSummary aggregates data for an item for the main page view.
type ItemSummary struct {
	Name         string
	ItemID       int
	LowestPrice  sql.NullInt64 // Handles cases with no available listings.
	HighestPrice sql.NullInt64
	ListingCount int
}

// ItemListing holds details for a single current listing, used for the info cards on the history page.
type ItemListing struct {
	Price          int    `json:"Price"`
	Quantity       int    `json:"Quantity"`
	StoreName      string `json:"StoreName"`
	SellerName     string `json:"SellerName"`
	MapName        string `json:"MapName"`
	MapCoordinates string `json:"MapCoordinates"`
	Timestamp      string `json:"Timestamp"`
}

// PricePointDetails captures the state of an item's price at a specific point in time for the history chart.
type PricePointDetails struct {
	Timestamp         string `json:"Timestamp"`
	LowestPrice       int    `json:"LowestPrice"`
	LowestQuantity    int    `json:"LowestQuantity"`
	LowestStoreName   string `json:"LowestStoreName"`
	LowestSellerName  string `json:"LowestSellerName"`
	LowestMapName     string `json:"LowestMapName"`
	LowestMapCoords   string `json:"LowestMapCoords"`
	HighestPrice      int    `json:"HighestPrice"`
	HighestQuantity   int    `json:"HighestQuantity"`
	HighestStoreName  string `json:"HighestStoreName"`
	HighestSellerName string `json:"HighestSellerName"`
	HighestMapName    string `json:"HighestMapName"`
	HighestMapCoords  string `json:"HighestMapCoords"`
}

// RMSItem holds detailed information scraped from RateMyServer.
type RMSItem struct {
	ID             int
	Name           string
	ImageURL       string
	Type           string
	Class          string
	Buy            string
	Sell           string
	Weight         string
	Prefix         string
	Description    string
	Script         string
	DroppedBy      []RMSDrop
	ObtainableFrom []string
}

// RMSDrop holds monster drop information from RateMyServer.
type RMSDrop struct {
	Monster string `json:"Monster"`
	Rate    string `json:"Rate"`
}

// EventDefinition defines a recurring event with a name, a time, and days of the week.
type EventDefinition struct {
	Name      string
	StartTime string // "HH:MM"
	EndTime   string // "HH:MM"
	Days      []time.Weekday
}

// ItemTypeTab defines the data for a category tab.
type ItemTypeTab struct {
	FullName   string
	ShortName  string
	IconItemID int
}

// PlayerCharacter represents a single player's data from the characters page.
type PlayerCharacter struct {
	Rank          int
	Name          string
	BaseLevel     int
	JobLevel      int
	Experience    float64
	Class         string
	Zeny          int64
	GuildName     sql.NullString // Use sql.NullString to handle players without a guild.
	LastUpdated   string
	LastActive    string
	IsActive      bool
	IsGuildLeader bool
	IsSpecial     bool
}

// Guild represents a single guild's data from the rankings.
type Guild struct {
	Rank         int
	Name         string
	Level        int
	Experience   int
	Master       string
	EmblemURL    string
	MemberCount  int
	TotalZeny    int64
	AvgBaseLevel float64
}

// MvpKillEntry holds the kill counts for a single character.
type MvpKillEntry struct {
	CharacterName string
	TotalKills    int
	Kills         map[string]int // Map of MobID to Kill Count
}

// MvpHeader defines a column header for the MVP kills table.
type MvpHeader struct {
	MobID   string
	MobName string
}

// --- Page Data Structs for HTML Templates ---

// SummaryPageData holds all data needed for the main summary page template.
type SummaryPageData struct {
	Items          []ItemSummary
	SearchQuery    string
	SortBy         string
	Order          string
	ShowAll        bool
	LastScrapeTime string
	ItemTypes      []ItemTypeTab
	SelectedType   string
}

// PageData holds data for the detailed full list view template.
type PageData struct {
	Items          []Item
	SearchQuery    string
	StoreNameQuery string
	AllStoreNames  []string
	SortBy         string
	Order          string
	ShowAll        bool
	LastScrapeTime string
	VisibleColumns map[string]bool
	AllColumns     []Column
	ColumnParams   template.URL
	ItemTypes      []ItemTypeTab
	SelectedType   string
}

// ActivityPageData holds data for the market activity page template.
type ActivityPageData struct {
	MarketEvents   []MarketEvent
	LastScrapeTime string
	// Pagination
	CurrentPage int
	TotalPages  int
	PrevPage    int
	NextPage    int
	HasPrevPage bool
	HasNextPage bool
}

// HistoryPageData holds data for the item history page template.
type HistoryPageData struct {
	ItemName           string
	PriceDataJSON      template.JS
	OverallLowest      int
	OverallHighest     int
	CurrentLowestJSON  template.JS
	CurrentHighestJSON template.JS
	ItemDetails        *RMSItem
	AllListings        []Item
	LastScrapeTime     string
	// Pagination for AllListings table
	CurrentPage   int
	TotalPages    int
	PrevPage      int
	NextPage      int
	HasPrevPage   bool
	HasNextPage   bool
	TotalListings int
}

// PlayerCountPoint represents a single data point for the player history chart.
type PlayerCountPoint struct {
	Timestamp   string `json:"Timestamp"`
	Count       int    `json:"Count"`
	SellerCount int    `json:"SellerCount"`
	Delta       int    `json:"Delta"` // Added this field
}

// PlayerCountPageData holds data for the player count history page template.
type PlayerCountPageData struct {
	PlayerDataJSON                 template.JS
	LastScrapeTime                 string
	SelectedInterval               string
	EventDataJSON                  template.JS
	LatestActivePlayers            int
	HistoricalMaxActivePlayers     int
	HistoricalMaxActivePlayersTime string
}

// models.go

type CharacterPageData struct {
	Players        []PlayerCharacter
	LastScrapeTime string
	// Search and Filter
	SearchName    string
	SelectedClass string
	SelectedGuild string
	AllClasses    []string
	// Sorting
	SortBy string
	Order  string
	// Column Visibility
	VisibleColumns map[string]bool
	AllColumns     []Column
	ColumnParams   template.URL
	// Pagination
	CurrentPage  int
	TotalPages   int
	PrevPage     int
	NextPage     int
	TotalPlayers int
	TotalZeny    int64
	HasPrevPage  bool
	HasNextPage  bool
	// Graph Data
	ClassDistributionJSON template.JS
	GraphFilter           map[string]bool
	GraphFilterParams     template.URL
	HasChartData          bool
}

// GuildPageData holds data for the guild listing page template.
type GuildPageData struct {
	Guilds              []Guild
	LastGuildUpdateTime string
	// Search and Filter
	SearchName string
	// Sorting
	SortBy string
	Order  string
	// Pagination
	CurrentPage int
	TotalPages  int
	PrevPage    int
	NextPage    int
	TotalGuilds int
	HasPrevPage bool
	HasNextPage bool
}

// ADDED: GuildDetailPageData holds all data for the single guild view.
type GuildDetailPageData struct {
	Guild                 Guild
	Members               []PlayerCharacter
	LastScrapeTime        string
	ClassDistributionJSON template.JS
	HasChartData          bool
	// Sorting for members table
	SortBy string
	Order  string
	// Changelog data and pagination
	ChangelogEntries     []CharacterChangelog
	ChangelogCurrentPage int
	ChangelogTotalPages  int
	ChangelogPrevPage    int
	ChangelogNextPage    int
	HasChangelogPrevPage bool
	HasChangelogNextPage bool
}

// MvpKillPageData holds all data needed for the MVP kill rankings page.
type MvpKillPageData struct {
	Players        []MvpKillEntry
	Headers        []MvpHeader
	SortBy         string
	Order          string
	LastScrapeTime string
}

// CharacterDetailPageData holds all data for the single character view.
type CharacterDetailPageData struct {
	Character      PlayerCharacter
	Guild          *Guild // Pointer to handle characters without a guild
	MvpKills       MvpKillEntry
	MvpHeaders     []MvpHeader
	LastScrapeTime string
	// Changelog data and pagination
	ChangelogEntries     []CharacterChangelog
	ChangelogCurrentPage int
	ChangelogTotalPages  int
	ChangelogPrevPage    int
	ChangelogNextPage    int
	HasChangelogPrevPage bool
	HasChangelogNextPage bool
}

// CharacterChangelog holds a record of a change to a character.
type CharacterChangelog struct {
	ID                  int
	CharacterName       string
	ActivityDescription string
	ChangeTime          string
}

// CharacterChangelogPageData holds data for the character changelog page.
type CharacterChangelogPageData struct {
	ChangelogEntries []CharacterChangelog
	LastScrapeTime   string
	// Pagination
	CurrentPage int
	TotalPages  int
	PrevPage    int
	NextPage    int
	HasPrevPage bool
	HasNextPage bool
}

// AdminDashboardData holds statistics for the admin dashboard.
type AdminDashboardData struct {
	Message               string
	TotalItems            int
	AvailableItems        int
	UniqueItems           int
	CachedItems           int
	TotalCharacters       int
	TotalGuilds           int
	PlayerHistoryEntries  int
	MarketEvents          int
	ChangelogEntries      int
	LastMarketScrape      string
	LastPlayerCountScrape string
	LastCharacterScrape   string
	LastGuildScrape       string
}
