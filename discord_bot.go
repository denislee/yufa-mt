package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bwmarrin/discordgo"
)

var targetChannelIDs = make(map[string]struct{})

func startDiscordBot(ctx context.Context) {
	botToken := os.Getenv("DISCORD_BOT_TOKEN")
	channelIDsStr := os.Getenv("DISCORD_CHANNEL_IDS")

	if botToken == "" {
		log.Println("[W] [Discord] DISCORD_BOT_TOKEN not set. Bot will not start.")
		return
	}
	if channelIDsStr == "" {
		log.Println("[W] [Discord] DISCORD_CHANNEL_IDS not set. Bot will not start.")
		return
	}

	// ... (channel ID parsing is unchanged) ...
	channelIDs := strings.Split(channelIDsStr, ",")
	for _, id := range channelIDs {
		trimmedID := strings.TrimSpace(id)
		if trimmedID != "" {
			targetChannelIDs[trimmedID] = struct{}{}
		}
	}
	if len(targetChannelIDs) == 0 {
		log.Println("[W] [Discord] No valid channel IDs found in DISCORD_CHANNEL_IDS. Bot will not start.")
		return
	}

	dg, err := discordgo.New("Bot " + botToken)
	if err != nil {
		log.Printf("[E] [Discord] Error creating Discord session: %v", err)
		return
	}

	dg.AddHandler(ready)
	dg.AddHandler(messageCreate)
	dg.Identify.Intents = discordgo.IntentsGuildMessages

	err = dg.Open()
	if err != nil {
		log.Printf("[E] [Discord] Error opening connection: %v", err)
		return
	}

	log.Println("[I] [Discord] Bot is running. Waiting for shutdown signal...")

	// Wait for the shutdown signal from the main app's context
	<-ctx.Done()

	log.Println("[I] [Discord] Shutdown signal received. Closing Discord connection...")
	if err := dg.Close(); err != nil {
		log.Printf("[W] [Discord] Error while closing Discord session: %v", err)
	} else {
		log.Println("[I] [Discord] Discord connection closed gracefully.")
	}
}

func ready(s *discordgo.Session, event *discordgo.Ready) {
	log.Println("[I] [Discord] Bot is connected and ready!")
	log.Printf("[I] [Discord] Logged in as: %s", event.User.String())
	log.Printf("[I] [Discord] Listening on %d channel(s).", len(targetChannelIDs))
}

func messageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {

	if m.Author.ID == s.State.User.ID {
		return
	}

	if _, ok := targetChannelIDs[m.ChannelID]; !ok {
		return
	}

	log.Printf("[I] [Discord] Processing message from '%s': \"%s\"", m.Author.Username, m.Content)

	go func() {
		tradeResult, err := parseTradeMessageWithGemini(m.Content)
		if err != nil {
			log.Printf("[E] [Gemini] Failed to parse trade message from '%s': %v", m.Author.Username, err)

			return
		}

		if tradeResult == nil || len(tradeResult.Items) == 0 {
			log.Printf("[W] [Gemini] Returned no valid items for message from '%s'. Ignoring.", m.Author.Username)

			return
		}

		log.Printf("[I] [Gemini] Successfully parsed trade message. Found %d items.", len(tradeResult.Items))

		postIDs, err := CreateTradingPostFromDiscord(m.Author.Username, m.Content, tradeResult)
		if err != nil {
			log.Printf("[E] [DB] Failed to create trading post(s) for '%s': %v", m.Author.Username, err)

			return
		}

		if len(postIDs) == 0 {
			log.Printf("[W] [DB] No trading posts were created for '%s' (e.g., only empty items).", m.Author.Username)
			return
		}

		var postIDStrings []string
		for _, pid := range postIDs {
			postIDStrings = append(postIDStrings, fmt.Sprintf("#%d", pid))
		}
		log.Printf("[I] [DB] Successfully created trading post(s) %s for '%s'.", strings.Join(postIDStrings, ", "), m.Author.Username)

		_ = fmt.Sprintf("✅ Trade post(s) %s created for **%s**.", strings.Join(postIDStrings, ", "), m.Author.Username)

	}()
}

