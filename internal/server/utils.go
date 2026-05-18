package server

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

// capitalizeASCII title-cases a single ASCII word ("buying" -> "Buying").
// Used instead of the deprecated strings.Title for the known post-type values.
func capitalizeASCII(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// updateTimeCacheKey is a struct key for the GetLastUpdateTime cache. Using
// a struct avoids per-call fmt.Sprintf allocations on this hot helper, which
// is called multiple times per request.
type updateTimeCacheKey struct {
	table  string
	column string
}

// updateTimeCacheEntry holds a cached timestamp and its expiry.
type updateTimeCacheEntry struct {
	value  string
	expiry time.Time
}

// updateTimeCache holds the in-memory cache for GetLastUpdateTime.
var (
	updateTimeCache      = make(map[updateTimeCacheKey]updateTimeCacheEntry)
	updateTimeCacheMutex sync.RWMutex
	updateTimeCacheTTL   = 30 * time.Second
)

// InvalidateUpdateTimeCache drops the cached MAX(column) entry for a
// table+column so a subsequent read sees the fresh write. Scrapers call
// this right after committing instead of waiting up to 30s for the TTL.
func InvalidateUpdateTimeCache(columnName, tableName string) {
	key := updateTimeCacheKey{table: tableName, column: columnName}
	updateTimeCacheMutex.Lock()
	delete(updateTimeCache, key)
	updateTimeCacheMutex.Unlock()
}

func generateRandomPassword(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		log.Println("[W] [Util] Could not generate secure random password, using fallback.")
		// Fallback for environments where /dev/urandom might not be available
		return "fallback-password-yufa-change-me"
	}
	for i := 0; i < length; i++ {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}

// GetLastUpdateTime is a centralized helper to get the max timestamp from any table/column.
// This version is optimized with an in-memory cache to reduce redundant DB queries.
func GetLastUpdateTime(columnName, tableName string) string {
	cacheKey := updateTimeCacheKey{table: tableName, column: columnName}
	now := time.Now()

	// Check cache first (read lock)
	updateTimeCacheMutex.RLock()
	entry, found := updateTimeCache[cacheKey]
	updateTimeCacheMutex.RUnlock()

	if found && now.Before(entry.expiry) {
		return entry.value
	}

	// Cache miss or expired, query the database
	var lastTimestamp sql.NullString
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", columnName, tableName)
	err := srv.db.QueryRow(query).Scan(&lastTimestamp)
	if err != nil {
		log.Printf("[W] [Util] Could not get last update time for %s.%s: %v", tableName, columnName, err)
	}

	var resultValue string
	if lastTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastTimestamp.String)
		if err == nil {
			resultValue = parsedTime.Format("2006-01-02 15:04:05")
		} else {
			resultValue = "Never" // Handle parse error
		}
	} else {
		resultValue = "Never"
	}

	// Update cache (write lock)
	updateTimeCacheMutex.Lock()
	updateTimeCache[cacheKey] = updateTimeCacheEntry{
		value:  resultValue,
		expiry: now.Add(updateTimeCacheTTL),
	}
	updateTimeCacheMutex.Unlock()

	return resultValue
}

// queryCount runs the given COUNT(*) query and returns the result. It
// keeps the noise of `var n int; srv.db.QueryRow(...).Scan(&n)` out of every
// pagination handler. The query itself stays at the call site so the
// SQL is grep-able.
func queryCount(query string, params ...any) (int, error) {
	var n int
	err := srv.db.QueryRow(query, params...).Scan(&n)
	return n, err
}

// GetLastScrapeTime gets the timestamp of the last market scrape.
func GetLastScrapeTime() string {
	return GetLastUpdateTime("timestamp", "scrape_history")
}

// GetLastPlayerCountTime gets the timestamp of the last player count scrape.
func GetLastPlayerCountTime() string {
	return GetLastUpdateTime("timestamp", "player_history")
}

// GetLastGuildScrapeTime gets the timestamp of the last guild data update.
func GetLastGuildScrapeTime() string {
	return GetLastUpdateTime("last_updated", "guilds")
}

// GetLastCharacterScrapeTime gets the timestamp of the last character data update.
func GetLastCharacterScrapeTime() string {
	return GetLastUpdateTime("last_updated", "characters")
}

// GetLastChatLogTime gets the timestamp of the last chat message.
func GetLastChatLogTime() string {
	return GetLastUpdateTime("timestamp", "chat")
}

// GetLastChatPacketTime gets the timestamp of the last packet seen by the sniffer.
func GetLastChatPacketTime() string {
	// This var is from scraper.go (same package)
	unixTime := lastChatPacketTime.Load()
	if unixTime == 0 {
		// Fallback to the last saved message time if no packets seen this session
		return GetLastChatLogTime()
	}
	parsedTime := time.Unix(unixTime, 0)
	// Format it to match the other GetLast...Time functions
	return parsedTime.Format("2006-01-02 15:04:05")
}
