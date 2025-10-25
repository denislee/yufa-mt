package main

import (
	"crypto/rand"
	"database/sql"
	"fmt"
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

// GetLastUpdateTime is a centralized helper to get the max timestamp from any table/column.
func GetLastUpdateTime(columnName, tableName string) string {
	var lastTimestamp sql.NullString
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", columnName, tableName)
	err := db.QueryRow(query).Scan(&lastTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last update time for %s.%s: %v", tableName, columnName, err)
	}
	if lastTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastTimestamp.String)
		if err == nil {
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
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
