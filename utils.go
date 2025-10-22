package main

import (
	"crypto/rand"
	"database/sql"
	"io"
	"log"
	"time"
)

// generateRandomPassword creates a random alphanumeric password of a given length.
func generateRandomPassword(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		log.Println("❗️ Could not generate secure random password, using fallback.")
		// Fallback to a less random but still usable password on error
		return "fallback-password-yufa-change-me"
	}
	for i := 0; i < length; i++ {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}

// GetLastScrapeTime is a helper function to get the most recent market scrape time.
func GetLastScrapeTime() string {
	var lastScrapeTimestamp sql.NullString
	err := db.QueryRow("SELECT MAX(timestamp) FROM scrape_history").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last scrape time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {
			// Format for display
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

// GetLastPlayerCountTime is a helper function to get the most recent player count scrape time.
func GetLastPlayerCountTime() string {
	var lastScrapeTimestamp sql.NullString
	err := db.QueryRow("SELECT MAX(timestamp) FROM player_history").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last player count time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {
			// Format for display
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

// GetLastGuildScrapeTime is a helper function to get the most recent guild scrape time.
func GetLastGuildScrapeTime() string {
	var lastScrapeTimestamp sql.NullString
	// Query the 'guilds' table for the most recent 'last_updated' timestamp.
	err := db.QueryRow("SELECT MAX(last_updated) FROM guilds").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last guild scrape time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {
			// Format for display
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

// GetLastCharacterScrapeTime is a helper function to get the most recent character scrape time.
func GetLastCharacterScrapeTime() string {
	var lastScrapeTimestamp sql.NullString
	// Query the 'characters' table for the most recent 'last_updated' timestamp.
	err := db.QueryRow("SELECT MAX(last_updated) FROM characters").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last character scrape time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {
			// Format for display
			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}
