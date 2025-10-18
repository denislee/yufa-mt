package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings" // <-- Added import
	"sync"

	"github.com/bwmarrin/discordgo"
)

// Global map to hold the target channel IDs for efficient lookup
var targetChannelIDs = make(map[string]struct{})

// startDiscordBot initializes and runs the Discord bot.
func startDiscordBot(ctx context.Context, wg *sync.WaitGroup) {
	// Ensure that wg.Done() is called when this function exits.
	defer wg.Done()

	botToken := os.Getenv("DISCORD_BOT_TOKEN")
	// Read a comma-separated list of channel IDs
	channelIDsStr := os.Getenv("DISCORD_CHANNEL_IDS") // <-- Changed to plural

	if botToken == "" {
		log.Println("⚠️ [Discord Bot] DISCORD_BOT_TOKEN not set. Bot will not start.")
		return
	}
	if channelIDsStr == "" {
		log.Println("⚠️ [Discord Bot] DISCORD_CHANNEL_IDS not set. Bot will not start.")
		return
	}

	// Split the comma-separated string into a slice
	channelIDs := strings.Split(channelIDsStr, ",")

	// Populate the global map for quick lookups
	for _, id := range channelIDs {
		trimmedID := strings.TrimSpace(id) // Handle potential whitespace
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
	defer dg.Close() // Ensure the connection is closed on exit.

	dg.AddHandler(ready)
	dg.AddHandler(messageCreate)
	dg.Identify.Intents = discordgo.IntentsGuildMessages

	err = dg.Open()
	if err != nil {
		log.Printf("❌ [Discord Bot] Error opening connection: %v", err)
		return
	}

	log.Println("🤖 [Discord Bot] Bot is running. Waiting for shutdown signal from main app...")

	// Block here until the context is canceled by the main function.
	<-ctx.Done()

	log.Println("🔌 [Discord Bot] Shutdown signal received. Closing Discord connection...")
}

// ready is a handler that runs when the bot successfully connects and is ready.
func ready(s *discordgo.Session, event *discordgo.Ready) {
	log.Println("✅ [Discord Bot] Bot is connected and ready!")
	log.Printf("   -> Logged in as: %v#%v", s.State.User.Username, s.State.User.Discriminator)
	log.Printf("   -> Bot User ID: %s", s.State.User.ID)

	// Log all the channels the bot is listening to
	var listeningIDs []string
	for id := range targetChannelIDs {
		listeningIDs = append(listeningIDs, id)
	}
	log.Printf("   -> Listening on %d Channel(s): %s", len(targetChannelIDs), strings.Join(listeningIDs, ", "))
}

// messageCreate is called every time a new message is created on any channel the bot has access to.
func messageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	// Ignore all messages created by the bot itself
	if m.Author.ID == s.State.User.ID {
		return
	}

	// Only process messages from the target channels
	// Check if the message's channel ID is in our map
	if _, ok := targetChannelIDs[m.ChannelID]; !ok {
		return
	}

	// Log the message being processed.
	log.Printf("🗣️  [Discord] Processing message from '%s': \"%s\"", m.Author.Username, m.Content)

	// --- NEW LOGIC: PARSE AND CREATE TRADING POST ---
	// Run the parsing and DB insertion in a goroutine to avoid blocking the bot.
	go func() {
		tradeResult, err := parseTradeMessageWithGemini(m.Content)
		if err != nil {
			log.Printf("❌ [Discord->Gemini] Failed to parse trade message from '%s': %v", m.Author.Username, err)
			// s.MessageReactionAdd(m.ChannelID, m.ID, "❌") // React with an error
			return
		}

		// Validate the result
		if tradeResult == nil || len(tradeResult.Items) == 0 {
			log.Printf("⚠️ [Discord->Gemini] Gemini returned no valid items for message from '%s'. Ignoring.", m.Author.Username)
			// s.MessageReactionAdd(m.ChannelID, m.ID, "❓") // React with a question mark
			return
		}

		log.Printf("✅ [Discord->Gemini] Successfully parsed trade message. Action: %s, Items: %d", tradeResult.Action, len(tradeResult.Items))

		// Create the trading post entry in the database
		// MODIFIED: Added m.Content as the second argument
		postID, err := CreateTradingPostFromDiscord(m.Author.Username, m.Content, tradeResult)
		if err != nil {
			log.Printf("❌ [Discord->DB] Failed to create trading post for '%s': %v", m.Author.Username, err)
			// s.MessageReactionAdd(m.ChannelID, m.ID, "🔥") // React with a server error emoji
			return
		}

		log.Printf("✅ [Discord->DB] Successfully created trading post #%d for '%s'.", postID, m.Author.Username)

		// React to the original message with a success emoji
		// s.MessageReactionAdd(m.ChannelID, m.ID, "✅")

		// Send a confirmation message back to the channel
		_ = fmt.Sprintf("✅ Trade post #%d created for **%s**.", postID, m.Author.Username)
		// s.ChannelMessageSend(m.ChannelID, confirmationMessage)
	}()
}

