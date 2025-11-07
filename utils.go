package main

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"log"
	"sync" // Added
	"time"
)

// --- NEW ---
// updateTimeCacheEntry holds a cached timestamp and its expiry.
type updateTimeCacheEntry struct {
	value  string
	expiry time.Time
}

// updateTimeCache holds the in-memory cache for GetLastUpdateTime.
var (
	updateTimeCache      = make(map[string]updateTimeCacheEntry)
	updateTimeCacheMutex sync.RWMutex
	updateTimeCacheTTL   = 30 * time.Second // Cache for 30 seconds
)

// --- END NEW ---

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
	cacheKey := fmt.Sprintf("%s.%s", tableName, columnName)
	now := time.Now()

	// --- OPTIMIZATION: Check cache first (Read Lock) ---
	updateTimeCacheMutex.RLock()
	entry, found := updateTimeCache[cacheKey]
	updateTimeCacheMutex.RUnlock()

	if found && now.Before(entry.expiry) {
		// Cache hit and not expired
		return entry.value
	}
	// --- END OPTIMIZATION ---

	// Cache miss or expired, run the query
	var lastTimestamp sql.NullString
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", columnName, tableName)
	err := db.QueryRow(query).Scan(&lastTimestamp)
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

	// --- OPTIMIZATION: Update cache (Write Lock) ---
	updateTimeCacheMutex.Lock()
	updateTimeCache[cacheKey] = updateTimeCacheEntry{
		value:  resultValue,
		expiry: now.Add(updateTimeCacheTTL),
	}
	updateTimeCacheMutex.Unlock()
	// --- END OPTIMIZATION ---

	return resultValue
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
