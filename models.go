package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"time"
)

// Item represents a single listing scraped from the market.
type Item struct {
	ID             int
	Name           string
	NamePT         sql.NullString
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
	NamePT    sql.NullString
	ItemID    int
	Details   map[string]interface{}
}

// ItemSummary aggregates data for an item for the main page view.
type ItemSummary struct {
	Name         string
	NamePT       sql.NullString
	ItemID       int
	LowestPrice  sql.NullInt64
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
	NamePT         string
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
	Kills         map[string]int
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
	TotalVisitors  int
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
	// Search and Filter
	SearchQuery string
	SoldOnly    bool
	// Pagination
	Pagination PaginationData
}

// HistoryPageData holds data for the item history page template.
type HistoryPageData struct {
	ItemName           string
	ItemNamePT         sql.NullString
	PriceDataJSON      template.JS
	OverallLowest      int
	OverallHighest     int
	CurrentLowestJSON  template.JS
	CurrentHighestJSON template.JS
	ItemDetails        *RMSItem
	AllListings        []Item
	LastScrapeTime     string
	// Pagination for AllListings table
	Pagination    PaginationData
	TotalListings int
}

// PlayerCountPoint represents a single data point for the player history chart.
type PlayerCountPoint struct {
	Timestamp   string `json:"Timestamp"`
	Count       int    `json:"Count"`
	SellerCount int    `json:"SellerCount"`
	Delta       int    `json:"Delta"`
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
	Pagination   PaginationData
	TotalPlayers int
	TotalZeny    int64
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
	Pagination  PaginationData
	TotalGuilds int
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
	ChangelogEntries    []CharacterChangelog
	ChangelogPagination PaginationData
}

// ADDED: StoreDetailPageData holds all data for the single store view.
type StoreDetailPageData struct {
	StoreName      string
	SellerName     string
	MapName        string
	MapCoordinates string
	Items          []Item
	LastScrapeTime string
	// Sorting
	SortBy string
	Order  string
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
	GuildHistory   []CharacterChangelog
	ClassImageURL  string
	// Changelog data and pagination
	ChangelogEntries    []CharacterChangelog
	ChangelogPagination PaginationData
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
	Pagination PaginationData
}

// GuildInfo holds basic info for the admin dropdown.
type GuildInfo struct {
	Name      string
	EmblemURL string
}

// PageViewEntry holds data for a single recent page view.
type PageViewEntry struct {
	Path        string
	Timestamp   string
	VisitorHash string
}

// ShortHash returns a truncated version of the VisitorHash for display.
func (p PageViewEntry) ShortHash() string {
	if len(p.VisitorHash) > 12 {
		return p.VisitorHash[:12]
	}
	return p.VisitorHash
}

// GeminiTradeItem holds the parsed data for a single item from a trade message.
type GeminiTradeItem struct {
	Name       string `json:"name"`
	Quantity   int    `json:"quantity"`
	Price      int64  `json:"price"`
	Currency   string `json:"currency"`
	Refinement int    `json:"refinement"`
	Slots      int    `json:"slots"`
	Card1      string `json:"card1"`
	Card2      string `json:"card2"`
	Card3      string `json:"card3"`
	Card4      string `json:"card4"`
}

// GeminiTradeResult holds the complete parsed result from a trade message.
type GeminiTradeResult struct {
	Action string            `json:"action"`
	Items  []GeminiTradeItem `json:"items"`
}

// Modify the AdminDashboardData struct to include the new fields.
type AdminDashboardData struct {
	Message               string
	AllGuilds             []GuildInfo
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
	TotalVisitors         int
	VisitorsToday         int
	MostVisitedPage       string
	MostVisitedPageCount  int
	RecentPageViews       []PageViewEntry
	// Pagination for Recent Page Views
	PageViewsCurrentPage int
	PageViewsTotalPages  int
	PageViewsHasPrevPage bool
	PageViewsHasNextPage bool
	PageViewsPrevPage    int
	PageViewsNextPage    int
	PageViewsTotal       int
	// Trading Posts
	RecentTradingPosts     []TradingPost
	TradingPostCurrentPage int
	TradingPostTotalPages  int
	TradingPostHasPrevPage bool
	TradingPostHasNextPage bool
	TradingPostPrevPage    int
	TradingPostNextPage    int
	TradingPostTotal       int
	// ADDED: Fields for Gemini Trade Parser results
	TradeParseResult     *GeminiTradeResult
	OriginalTradeMessage string
	TradeParseError      string
}

// ADDED: AdminEditPostPageData holds data for the admin post edit page.
type AdminEditPostPageData struct {
	Post           TradingPost
	LastScrapeTime string
	Message        string
}

// in models.go

// MODIFIED: TradingPostItem represents one item within a larger post.
type TradingPostItem struct {
	ItemName   string
	NamePT     sql.NullString
	ItemID     sql.NullInt64 // To handle optional item ID
	Quantity   int
	Price      int64
	Currency   string // "zeny" or "rmt"
	Refinement int
	Slots      int
	Card1      sql.NullString
	Card2      sql.NullString
	Card3      sql.NullString
	Card4      sql.NullString
}

// MODIFIED: TradingPost now holds post-level info and a slice of items.
type TradingPost struct {
	ID            int
	Title         string
	PostType      string // "buying" or "selling"
	CharacterName string
	ContactInfo   sql.NullString
	Notes         sql.NullString
	CreatedAt     string
	EditTokenHash string
	Items         []TradingPostItem // A post can now have multiple items
}

// ADDED: TradingPostPageData holds data for the trading post list view.
type TradingPostPageData struct {
	Posts          []TradingPost
	LastScrapeTime string // To keep the header consistent
	// Add filter/sort/pagination fields here as needed
	FilterType  string
	SearchQuery string
}

// ADDED: TradingPostSuccessData holds data for the post-creation success page.
type TradingPostSuccessData struct {
	Post      TradingPost
	EditToken string // The raw token to show the user ONCE
}

// ADDED: Helper to format creation time for display.
func (tp TradingPost) CreatedAgo() string {
	t, err := time.Parse(time.RFC3339, tp.CreatedAt)
	if err != nil {
		return "a while ago"
	}

	d := time.Since(t)
	if d.Hours() < 24 {
		return fmt.Sprintf("%d hours ago", int(d.Hours()))
	}
	return fmt.Sprintf("%d days ago", int(d.Hours()/24))
}

// ADDED: TradingPostFormPageData holds data for the new/edit post form.
type TradingPostFormPageData struct {
	Title     string
	ActionURL string
	Post      TradingPost
	EditToken string // To pass the token to the edit form for re-submission
	Message   string // For showing errors
}
