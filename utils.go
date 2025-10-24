package main

import (
	"crypto/rand"
	"database/sql"
	"io"
	"log"
	"time"
)

func generateRandomPassword(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		log.Println("❗️ Could not generate secure random password, using fallback.")

		return "fallback-password-yufa-change-me"
	}
	for i := 0; i < length; i++ {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}

func GetLastScrapeTime() string {
	var lastScrapeTimestamp sql.NullString
	err := db.QueryRow("SELECT MAX(timestamp) FROM scrape_history").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last scrape time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {

			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

func GetLastPlayerCountTime() string {
	var lastScrapeTimestamp sql.NullString
	err := db.QueryRow("SELECT MAX(timestamp) FROM player_history").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last player count time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {

			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

func GetLastGuildScrapeTime() string {
	var lastScrapeTimestamp sql.NullString

	err := db.QueryRow("SELECT MAX(last_updated) FROM guilds").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last guild scrape time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {

			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}

func GetLastCharacterScrapeTime() string {
	var lastScrapeTimestamp sql.NullString

	err := db.QueryRow("SELECT MAX(last_updated) FROM characters").Scan(&lastScrapeTimestamp)
	if err != nil {
		log.Printf("⚠️ Could not get last character scrape time: %v", err)
	}
	if lastScrapeTimestamp.Valid {
		parsedTime, err := time.Parse(time.RFC3339, lastScrapeTimestamp.String)
		if err == nil {

			return parsedTime.Format("2006-01-02 15:04:05")
		}
	}
	return "Never"
}
