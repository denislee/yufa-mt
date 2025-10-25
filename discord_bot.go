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
		log.Println("⚠️ [Discord Bot] DISCORD_BOT_TOKEN not set. Bot will not start.")
		return
	}
	if channelIDsStr == "" {
		log.Println("⚠️ [Discord Bot] DISCORD_CHANNEL_IDS not set. Bot will not start.")
		return
	}

	channelIDs := strings.Split(channelIDsStr, ",")

	for _, id := range channelIDs {
		trimmedID := strings.TrimSpace(id)
		if trimmedID != "" {
			targetChannelIDs[trimmedID] = struct{}{}
		}
	}

	if len(targetChannelIDs) == 0 {
		log.Println("⚠️ [Discord Bot] No valid channel IDs found in DISCORD_CHANNEL_IDS. Bot will not start.")
		return
	}

	dg, err := discordgo.New("Bot " + botToken)
	if err != nil {
		log.Printf("❌ [Discord Bot] Error creating Discord session: %v", err)
		return
	}
	defer dg.Close()

	dg.AddHandler(ready)
	dg.AddHandler(messageCreate)
	dg.Identify.Intents = discordgo.IntentsGuildMessages

	err = dg.Open()
	if err != nil {
		log.Printf("❌ [Discord Bot] Error opening connection: %v", err)
		return
	}

	log.Println("🤖 [Discord Bot] Bot is running. Waiting for shutdown signal from main app...")

	// Block forever, as the context will never be canceled.
	// This keeps the goroutine alive and the bot connected.
	<-ctx.Done()

	log.Println("🔌 [Discord Bot] Shutdown signal received. Closing Discord connection...")
}

func ready(s *discordgo.Session, event *discordgo.Ready) {
	log.Println("✅ [Discord Bot] Bot is connected and ready!")
	log.Printf("   -> Logged in as: %v#%v", s.State.User.Username, s.State.User.Discriminator)
	log.Printf("   -> Bot User ID: %s", s.State.User.ID)

	var listeningIDs []string
	for id := range targetChannelIDs {
		listeningIDs = append(listeningIDs, id)
	}
	log.Printf("   -> Listening on %d Channel(s): %s", len(targetChannelIDs), strings.Join(listeningIDs, ", "))
}

func messageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {

	if m.Author.ID == s.State.User.ID {
		return
	}

	if _, ok := targetChannelIDs[m.ChannelID]; !ok {
		return
	}

	log.Printf("🗣️  [Discord] Processing message from '%s': \"%s\"", m.Author.Username, m.Content)

	go func() {
		tradeResult, err := parseTradeMessageWithGemini(m.Content)
		if err != nil {
			log.Printf("❌ [Discord->Gemini] Failed to parse trade message from '%s': %v", m.Author.Username, err)

			return
		}

		if tradeResult == nil || len(tradeResult.Items) == 0 {
			log.Printf("⚠️ [Discord->Gemini] Gemini returned no valid items for message from '%s'. Ignoring.", m.Author.Username)

			return
		}

		log.Printf("✅ [Discord->Gemini] Successfully parsed trade message. Found %d items.", len(tradeResult.Items))

		postIDs, err := CreateTradingPostFromDiscord(m.Author.Username, m.Content, tradeResult)
		if err != nil {
			log.Printf("❌ [Discord->DB] Failed to create trading post(s) for '%s': %v", m.Author.Username, err)

			return
		}

		if len(postIDs) == 0 {
			log.Printf("⚠️ [Discord->DB] No trading posts were created for '%s' (e.g., only empty items).", m.Author.Username)
			return
		}

		var postIDStrings []string
		for _, pid := range postIDs {
			postIDStrings = append(postIDStrings, fmt.Sprintf("#%d", pid))
		}
		log.Printf("✅ [Discord->DB] Successfully created trading post(s) %s for '%s'.", strings.Join(postIDStrings, ", "), m.Author.Username)

		_ = fmt.Sprintf("✅ Trade post(s) %s created for **%s**.", strings.Join(postIDStrings, ", "), m.Author.Username)

	}()
}
