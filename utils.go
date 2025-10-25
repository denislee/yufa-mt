package main

import (
	"crypto/rand"
	"database/sql"
	"fmt" // Added missing import
	"io"
	"log"
	"time"
)

func generateRandomPassword(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		log.Println("❗️ Could not generate secure random password, using fallback.")
		// Fallback for environments where /dev/urandom might not be available
		return "fallback-password-yufa-change-me"
	}
	for i := 0; i < length; i++ {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}

// queryLastUpdateTime is an unexported helper to get the max timestamp from any table/column.
// Renamed from getLastUpdateTime to avoid conflict with handlers.go
func queryLastUpdateTime(columnName, tableName string) string {
	var lastScrapeTimestamp sql.NullString
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", columnName, tableName)
	err := db.QueryRow(query).Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last update time for %s.%s: %v", tableName, columnName, err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

// GetLastScrapeTime gets the timestamp of the last market scrape.
func GetLastScrapeTime() string {
	return queryLastUpdateTime("timestamp", "scrape_history")
}

// GetLastPlayerCountTime gets the timestamp of the last player count scrape.
func GetLastPlayerCountTime() string {
	return queryLastUpdateTime("timestamp", "player_history")
}

// GetLastGuildScrapeTime gets the timestamp of the last guild data update.
func GetLastGuildScrapeTime() string {
	return queryLastUpdateTime("last_updated", "guilds")
}

// GetLastCharacterScrapeTime gets the timestamp of the last character data update.
func GetLastCharacterScrapeTime() string {
	return queryLastUpdateTime("last_updated", "characters")
}

