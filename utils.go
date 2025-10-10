package main

import (
	"crypto/rand"
	"io"
	"log"
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
