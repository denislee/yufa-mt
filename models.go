package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"time"
)

var (
	// mvpMobIDs is the central list of MVP mob IDs used across the application.
	mvpMobIDs = []string{
		"1038", "1039", "1046", "1059", "1086", "1087", "1112", "1115", "1147",
		"1150", "1157", "1159", "1190", "1251", "1252", "1272", "1312", "1373",
		"1389", "1418", "1492", "1511",
	}

	// mvpNames is the central map of MVP mob IDs to their names.
	mvpNames = map[string]string{
		"1038": "Osiris",
		"1039": "Baphomet",
		"1046": "Doppelganger",
		"1059": "Mistress",
		"1086": "Golden Thief Bug",
		"1087": "Orc Hero",
		"1112": "Drake",
		"1115": "Eddga",
		"1147": "Maya",
		"1150": "Moonlight Flower",
		"1157": "Pharaoh",
		"1159": "Phreeoni",
		"1190": "Orc Lord",
		"1251": "Stormy Knight",
		"1252": "Hatii",
		"1272": "Dark Lord",
		"1312": "Turtle General",
		"1373": "Lord of Death",
		"1389": "Dracula",
		"1418": "Evil Snake Lord",
		"1492": "Incantation Samurai",
		"1511": "Amon Ra",
	}
)

// ItemDBEntry represents a single item entry in the YAML 'Body'.
type ItemDBEntry struct {
	ID            int             `yaml:"Id"`
	AegisName     string          `yaml:"AegisName"`
	Name          string          `yaml:"Name"`
	Type          string          `yaml:"Type"`
	Buy           *int64          `yaml:"Buy"`    // Changed from sql.NullInt64 to *int64
	Sell          *int64          `yaml:"Sell"`   // Changed from sql.NullInt64 to *int64
	Weight        *int64          `yaml:"Weight"` // Changed from sql.NullInt64 to *int64
	Slots         *int64          `yaml:"Slots"`  // Changed from sql.NullInt64 to *int64
	Jobs          map[string]bool `yaml:"Jobs"`
	Locations     map[string]bool `yaml:"Locations"`
	Script        string          `yaml:"Script"`
	EquipScript   string          `yaml:"EquipScript"`
	UnEquipScript string          `yaml:"UnEquipScript"`
}

// ItemDBFile represents the top-level structure of an item_db YAML file.
type ItemDBFile struct {
	Header map[string]interface{} `yaml:"Header"`
	Body   []ItemDBEntry          `yaml:"Body"`
}

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

type Column struct {
	ID          string
	DisplayName string
}

type MarketEvent struct {
	Timestamp string
	EventType string
	ItemName  string
	NamePT    sql.NullString
	ItemID    int
	Details   map[string]interface{}
}

type ItemSummary struct {
	Name         string
	NamePT       sql.NullString
	ItemID       int
	LowestPrice  sql.NullInt64
	HighestPrice sql.NullInt64
	ListingCount int
}

type ItemListing struct {
	Price          int64  `json:"Price"`
	Quantity       int    `json:"Quantity"`
	StoreName      string `json:"StoreName"`
	SellerName     string `json:"SellerName"`
	MapName        string `json:"MapName"`
	MapCoordinates string `json:"MapCoordinates"`
	Timestamp      string `json:"Timestamp"`
}

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
	Slots          int
	Prefix         string
	Description    string
	Script         string
	DroppedBy      []RMSDrop
	ObtainableFrom []string
}

type RMSDrop struct {
	Monster string `json:"Monster"`
	Rate    string `json:"Rate"`
}

type EventDefinition struct {
	Name      string
	StartTime string
	EndTime   string
	Days      []time.Weekday
}

type ItemTypeTab struct {
	FullName   string
	ShortName  string
	IconItemID int
}

type PlayerCharacter struct {
	Rank          int
	Name          string
	BaseLevel     int
	JobLevel      int
	Experience    float64
	Class         string
	Zeny          int64
	GuildName     sql.NullString
	LastUpdated   string
	LastActive    string
	IsActive      bool
	IsGuildLeader bool
	IsSpecial     bool
}

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

type MvpKillEntry struct {
	CharacterName string
	TotalKills    int
	Kills         map[string]int
}

type MvpHeader struct {
	MobID   string
	MobName string
}

type SummaryPageData struct {
	Items            []ItemSummary
	SearchQuery      string
	SortBy           string
	Order            string
	ShowAll          bool
	LastScrapeTime   string
	ItemTypes        []ItemTypeTab
	SelectedType     string
	TotalVisitors    int
	TotalUniqueItems int
	PageTitle        string
}

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
	PageTitle      string
}

type ActivityPageData struct {
	MarketEvents   []MarketEvent
	LastScrapeTime string

	SearchQuery string
	SoldOnly    bool

	Pagination PaginationData
	PageTitle  string
}

type HistoryPageData struct {
	ItemName           string
	ItemNamePT         sql.NullString
	PriceDataJSON      template.JS
	CurrentLowestJSON  template.JS
	CurrentHighestJSON template.JS
	OverallLowest      int64
	OverallHighest     int64
	CurrentLowest      *ItemListing
	CurrentHighest     *ItemListing
	ItemDetails        *RMSItem
	AllListings        []Item
	LastScrapeTime     string
	Pagination         PaginationData
	TotalListings      int
	PageTitle          string
	Filter             template.URL
	DropHistory        []PlayerDropInfo
}

type PlayerCountPoint struct {
	Timestamp   string `json:"Timestamp"`
	Count       int    `json:"Count"`
	SellerCount int    `json:"SellerCount"`
	Delta       int    `json:"Delta"`
}

type PlayerCountPageData struct {
	PlayerDataJSON                 template.JS
	LastScrapeTime                 string
	SelectedInterval               string
	EventDataJSON                  template.JS
	LatestActivePlayers            int
	HistoricalMaxActivePlayers     int
	HistoricalMaxActivePlayersTime string
	PageTitle                      string

	IntervalPeakActive     int
	IntervalPeakActiveTime string
	IntervalAvgActive      int
	IntervalLowActive      int
}

type CharacterPageData struct {
	Players        []PlayerCharacter
	LastScrapeTime string

	SearchQuery   string
	SelectedClass string
	SelectedGuild string
	AllClasses    []string

	SortBy string
	Order  string

	VisibleColumns map[string]bool
	AllColumns     []Column
	Filter         template.URL

	Pagination   PaginationData
	TotalPlayers int
	TotalZeny    int64

	ClassDistributionJSON template.JS
	GraphFilter           map[string]bool
	HasChartData          bool
	PageTitle             string
}

type GuildPageData struct {
	Guilds              []Guild
	LastGuildUpdateTime string
	SearchQuery         string
	SortBy              string
	Order               string
	Pagination          PaginationData
	TotalGuilds         int
	PageTitle           string
	Filter              template.URL
}

type GuildDetailPageData struct {
	Guild                 Guild
	Members               []PlayerCharacter
	LastScrapeTime        string
	ClassDistributionJSON template.JS
	HasChartData          bool

	SortBy string
	Order  string

	ChangelogEntries    []CharacterChangelog
	ChangelogPagination PaginationData
	PageTitle           string
	Filter              template.URL
}

type WoeGuildClassRank struct {
	Class          string
	MemberCount    int64
	TotalKills     int64
	TotalDeaths    int64
	TotalDamage    int64
	TotalHealing   int64
	TotalEmpKills  int64
	TotalPoints    int64
	KillDeathRatio float64
}

// WoeGuildRank holds aggregated WoE stats for an entire guild.
type WoeGuildRank struct {
	GuildName      sql.NullString
	GuildID        sql.NullInt64
	MemberCount    int64
	TotalKills     int64
	TotalDeaths    int64
	TotalDamage    int64
	TotalHealing   int64
	TotalEmpKills  int64
	TotalPoints    int64
	KillDeathRatio float64
}

// WoeCharacterRank holds per-character WoE ranking data.
type WoeCharacterRank struct {
	Name         string
	Class        string
	GuildID      sql.NullInt64
	GuildName    sql.NullString
	KillCount    int
	DeathCount   int
	DamageDone   int64
	EmperiumKill int
	HealingDone  int64
	Score        int
	Points       int
	LastUpdated  string
}

// WoeSeasonInfo holds basic info for the season dropdown
type WoeSeasonInfo struct {
	SeasonID  int
	StartDate string
	EndDate   sql.NullString
}

// WoeEventInfo holds basic info for the event dropdown
type WoeEventInfo struct {
	EventID         int
	EventDate       string
	IsSeasonSummary bool
}

type WoePageData struct {
	// New Season/Event Data
	AllSeasons        []WoeSeasonInfo
	SelectedSeasonID  int
	EventsForSeason   []WoeEventInfo
	SelectedEventID   int
	SelectedEventDate string // Replaces LastScrapeTime

	// Data for the selected event
	Characters         []WoeCharacterRank
	Guilds             []WoeGuildRank
	GuildClassRanksMap map[string][]WoeGuildClassRank

	// Page state
	ActiveTab     string
	SortBy        string
	Order         string
	SearchQuery   string
	PageTitle     string
	Filter        template.URL
	AllClasses    []string
	SelectedClass string
}

type FlatTradingPostItem struct {
	PostID        int
	Title         string
	PostType      string
	CharacterName string
	ContactInfo   sql.NullString
	Notes         sql.NullString
	CreatedAt     string

	ItemName       string
	NamePT         sql.NullString
	ItemID         sql.NullInt64
	Quantity       int
	PriceZeny      int64
	PriceRMT       int64
	PaymentMethods string
	Refinement     int
	Card1          sql.NullString
	Card2          sql.NullString
	Card3          sql.NullString
	Card4          sql.NullString
}

type StoreDetailPageData struct {
	StoreName      string
	SellerName     string
	MapName        string
	MapCoordinates string
	Items          []Item
	LastScrapeTime string

	SortBy    string
	Order     string
	PageTitle string
	Filter    template.URL
}

type MvpKillPageData struct {
	Players        []MvpKillEntry
	Headers        []MvpHeader
	SortBy         string
	Order          string
	LastScrapeTime string
	PageTitle      string
}

type CharacterDetailPageData struct {
	Character            PlayerCharacter
	Guild                *Guild
	MvpKills             MvpKillEntry
	MvpHeaders           []MvpHeader
	LastScrapeTime       string
	GuildHistory         []CharacterChangelog
	ClassImageURL        string
	ActivityHistory      []CharacterChangelog // This will hold the paginated/searched list
	DropHistory          []CharacterChangelog // This will hold the full drop list
	ChangelogPagination  PaginationData
	PageTitle            string
	Filter               template.URL
	ChangelogSearchQuery string
}

type CharacterChangelog struct {
	ID                  int
	CharacterName       string
	ActivityDescription string
	ChangeTime          string
}

type CharacterChangelogPageData struct {
	ChangelogEntries []CharacterChangelog
	LastScrapeTime   string
	Pagination       PaginationData
	PageTitle        string
	Filter           template.URL
}

type GuildInfo struct {
	Name      string
	EmblemURL string
}

type PageViewEntry struct {
	Path        string
	Timestamp   string
	VisitorHash string
}

func (p PageViewEntry) ShortHash() string {
	if len(p.VisitorHash) > 12 {
		return p.VisitorHash[:12]
	}
	return p.VisitorHash
}

type GeminiTradeItem struct {
	Name           string `json:"name"`
	Action         string `json:"action"`
	Quantity       int    `json:"quantity"`
	PriceZeny      int64  `json:"price_zeny"`
	PriceRMT       int64  `json:"price_rmt"`
	PaymentMethods string `json:"payment_methods"`
	Refinement     int    `json:"refinement"`
	Slots          int    `json:"slots"`
	Card1          string `json:"card1"`
	Card2          string `json:"card2"`
	Card3          string `json:"card3"`
	Card4          string `json:"card4"`
}

type GeminiTradeResult struct {
	Items []GeminiTradeItem `json:"items"`
}

type RMSCacheSearchResult struct {
	ItemID int
	Name   string
	NamePT sql.NullString
}

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

	PageVisitCounts []PageViewSummary

	PageViewsCurrentPage int
	PageViewsTotalPages  int
	PageViewsHasPrevPage bool
	PageViewsHasNextPage bool
	PageViewsPrevPage    int
	PageViewsNextPage    int
	PageViewsTotal       int

	RecentTradingPosts     []TradingPost
	TradingPostCurrentPage int
	TradingPostTotalPages  int
	TradingPostHasPrevPage bool
	TradingPostHasNextPage bool
	TradingPostPrevPage    int
	TradingPostNextPage    int
	TradingPostTotal       int

	TradeParseResult     *GeminiTradeResult
	OriginalTradeMessage string
	TradeParseError      string

	RMSCacheSearchQuery   string
	RMSCacheSearchResults []RMSCacheSearchResult

	RMSLiveSearchQuery   string
	RMSLiveSearchResults []ItemSearchResult

	ChatMessages      []ChatMessage
	ChatSearchQuery   string
	ChatCurrentPage   int
	ChatTotalPages    int
	ChatHasPrevPage   bool
	ChatHasNextPage   bool
	ChatPrevPage      int
	ChatNextPage      int
	ChatTotalMessages int
}

type AdminEditPostPageData struct {
	Post           TradingPost
	LastScrapeTime string
	Message        string
}

type TradingPostItem struct {
	ItemName       string
	NamePT         sql.NullString
	ItemID         sql.NullInt64
	Quantity       int
	PriceZeny      int64
	PriceRMT       int64
	PaymentMethods string
	Refinement     int
	Slots          int
	Card1          sql.NullString
	Card2          sql.NullString
	Card3          sql.NullString
	Card4          sql.NullString
}

type TradingPost struct {
	ID            int
	Title         string
	PostType      string
	CharacterName string
	ContactInfo   sql.NullString
	Notes         sql.NullString
	CreatedAt     string
	EditTokenHash string
	Items         []TradingPostItem
}

type TradingPostPageData struct {
	Items          []FlatTradingPostItem
	LastScrapeTime string
	FilterType     string
	SearchQuery    string
	FilterCurrency string
	SortBy         string
	Order          string
	PageTitle      string
	Filter         template.URL
}

// timeAgo formats an RFC3339 timestamp as a human-readable relative time string.
func timeAgo(rfc3339 string) string {
	t, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		return "a while ago"
	}

	d := time.Since(t)
	if d.Hours() < 1 {
		m := int(d.Minutes())
		if m < 1 {
			return "just now"
		}
		return fmt.Sprintf("%d minutes ago", m)
	}
	if d.Hours() < 24 {
		return fmt.Sprintf("%d hours ago", int(d.Hours()))
	}
	return fmt.Sprintf("%d days ago", int(d.Hours()/24))
}

func (tp TradingPost) CreatedAgo() string       { return timeAgo(tp.CreatedAt) }
func (fi FlatTradingPostItem) CreatedAgo() string { return timeAgo(fi.CreatedAt) }

func (fi FlatTradingPostItem) DisplayName() string {
	if fi.NamePT.Valid && fi.NamePT.String != "" {
		return fi.NamePT.String
	}
	return fi.ItemName
}

func (fi FlatTradingPostItem) OriginalName() string {
	displayName := fi.DisplayName()
	if fi.ItemName != "" && fi.ItemName != displayName {
		return fi.ItemName
	}
	return ""
}

// ChatMessage holds a single chat log entry
type ChatMessage struct {
	ID            int
	Timestamp     string
	Channel       string
	CharacterName string
	Message       string
}

// ChatPageData holds all data for the chat.html template
type ChatPageData struct {
	Messages          []ChatMessage
	LastScrapeTime    string
	Pagination        PaginationData
	PageTitle         string
	AllChannels       []string     `json:"-"`
	ActiveChannel     string       `json:"-"`
	QueryFilter       template.URL `json:"-"`
	SearchQuery       string       `json:"-"`
	ActivityGraphJSON template.JS  `json:"-"`
}

type DropStatPlayer struct {
	PlayerName string
	Count      int64
	CardCount  int64
	ItemCount  int64
}

// DropStatsPageData holds all data for the drop_stats.html template
type DropStatsPageData struct {
	PageTitle       string
	LastScrapeTime  string
	DropStats       []DropStatItem
	PlayerStats     []DropStatPlayer
	TotalDrops      int64
	UniqueDropItems int64
	ItemSortBy      string
	ItemOrder       string
	PlayerSortBy    string
	PlayerOrder     string
}

// XPCalculatorPageData holds all data for the xp_calculator.html template
type XPCalculatorPageData struct {
	PageTitle      string
	LastScrapeTime string

	// Form Inputs
	StartLevel  int
	StartPerc   float64
	EndLevel    int
	EndPerc     float64
	TimeHours   int
	TimeMinutes int
	CalcType    string
	TimeSpent   float64

	// Results
	TotalXPGained int64
	XPPerHour     int64
	ShowResults   bool
	ErrorMessage  string
}

type PageViewSummary struct {
	Path string
	Hits int
}

// GlobalSearchCharacterResult holds a single character result.
type GlobalSearchCharacterResult struct {
	Name      string
	Class     string
	GuildName sql.NullString
}

// GlobalSearchGuildResult holds a single guild result.
type GlobalSearchGuildResult struct {
	Name   string
	Master string
}

// GlobalSearchChatResult holds a single chat message result.
type GlobalSearchChatResult struct {
	CharacterName string
	Message       string
	Channel       string
	Timestamp     string
	FormattedTime string // e.g., "2023-10-27 15:04"
}

// GlobalSearchTradeResult holds a single trading post item result.
type GlobalSearchTradeResult struct {
	PostID        int
	PostType      string
	CharacterName string
	ItemName      string
	NamePT        sql.NullString
}

// GlobalSearchPageData holds all data for the new search.html template.
type GlobalSearchPageData struct {
	PageTitle        string
	LastScrapeTime   string
	SearchQuery      string
	CharacterResults []GlobalSearchCharacterResult
	GuildResults     []GlobalSearchGuildResult
	ChatResults      []GlobalSearchChatResult
	TradeResults     []GlobalSearchTradeResult
	MarketResults    []ItemSummary
	HasResults       bool
}

type DropStatItem struct {
	ItemID   sql.NullInt64
	Name     string
	NamePT   sql.NullString
	Count    int
	LastSeen string // Formatted as "YYYY-MM-DD HH:MM"
}

type PlayerDropInfo struct {
	PlayerName string
	Timestamp  string // Formatted as "YYYY-MM-DD HH:MM"
}

// MarketStatItem holds aggregated data for a top-selling item.
type MarketStatItem struct {
	ItemName  string
	ItemID    sql.NullInt64
	NamePT    sql.NullString
	Count     int64
	TotalZeny int64
}

// MarketStatSeller holds aggregated data for a top seller.
type MarketStatSeller struct {
	SellerName string
	Count      int64
	TotalZeny  int64
}

// MarketSalesPoint holds data for a single day in the chart.
type MarketSalesPoint struct {
	Day   string `json:"Day"`
	Count int64  `json:"Count"`
	Zeny  int64  `json:"Zeny"`
}

// MarketStatsPageData holds all data for the market_stats.html template.
type MarketStatsPageData struct {
	PageTitle           string
	LastScrapeTime      string
	SelectedInterval    string
	TotalSoldItems      int64
	TotalZenyTransacted int64
	SalesOverTimeJSON   template.JS
	TopSoldItems        []MarketStatItem
	TopSellers          []MarketStatSeller
	ItemSortBy          string // e.g., "count", "zeny"
	ItemOrder           string // "ASC" or "DESC"
	SellerSortBy        string // e.g., "name", "count", "zeny"
	SellerOrder         string // "ASC" or "DESC"
	Filter              template.URL
}

// LevelDistPoint holds data for a single bar in the level distribution chart.
type LevelDistPoint struct {
	Range string `json:"Range"`
	Count int    `json:"Count"`
}

// CharacterStatsPageData holds all data for the character_stats.html template.
type CharacterStatsPageData struct {
	PageTitle               string
	LastCharacterScrapeTime string
	TotalCharacters         int64
	TotalZeny               int64
	AvgBaseLevel            float64
	AvgJobLevel             float64
	ClassDistributionJSON   template.JS
	LevelDistributionJSON   template.JS
	TopRichestCharacters    []PlayerCharacter
	TopExperienceCharacters []PlayerCharacter
	GraphFilter             map[string]bool
}
