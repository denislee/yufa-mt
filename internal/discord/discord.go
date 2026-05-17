// Package discord runs the Discord bot that listens to configured
// channels and forwards trade messages into the application's trading
// post system. Both the trade-message parser and the post creator are
// passed in as callbacks so this package has no dependency on
// internal/server.
package discord

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/bwmarrin/discordgo"

	"github.com/denislee/yufa-mt/internal/gemini"
)

// TradeParser parses a Discord message into a structured trade result.
type TradeParser func(message string) (*gemini.TradeResult, error)

// PostCreator persists a parsed trade as one or more trading posts and
// returns the created post IDs.
type PostCreator func(authorName, originalMessage string, tradeData *gemini.TradeResult) ([]int64, error)

// Start opens a Discord session for botToken, listens on the given
// channel IDs, and forwards inbound messages through parse → post. It
// blocks until ctx is cancelled, then closes the session and returns.
func Start(ctx context.Context, botToken string, channelIDs []string, parse TradeParser, post PostCreator) {
	if botToken == "" {
		log.Println("[W] [Discord] DISCORD_BOT_TOKEN not set. Bot will not start.")
		return
	}
	if len(channelIDs) == 0 {
		log.Println("[W] [Discord] DISCORD_CHANNEL_IDS not set. Bot will not start.")
		return
	}

	targets := make(map[string]struct{}, len(channelIDs))
	for _, id := range channelIDs {
		targets[id] = struct{}{}
	}
	if len(targets) == 0 {
		log.Println("[W] [Discord] No valid channel IDs found in DISCORD_CHANNEL_IDS. Bot will not start.")
		return
	}

	dg, err := discordgo.New("Bot " + botToken)
	if err != nil {
		log.Printf("[E] [Discord] Error creating Discord session: %v", err)
		return
	}

	dg.AddHandler(func(s *discordgo.Session, event *discordgo.Ready) {
		log.Println("[I] [Discord] Bot is connected and ready!")
		log.Printf("[I] [Discord] Logged in as: %s", event.User.String())
		log.Printf("[I] [Discord] Listening on %d channel(s).", len(targets))
	})
	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		handleMessage(s, m, targets, parse, post)
	})
	dg.Identify.Intents = discordgo.IntentsGuildMessages

	if err := dg.Open(); err != nil {
		log.Printf("[E] [Discord] Error opening connection: %v", err)
		return
	}

	log.Println("[I] [Discord] Bot is running. Waiting for shutdown signal...")
	<-ctx.Done()

	log.Println("[I] [Discord] Shutdown signal received. Closing Discord connection...")
	if err := dg.Close(); err != nil {
		log.Printf("[W] [Discord] Error while closing Discord session: %v", err)
	} else {
		log.Println("[I] [Discord] Discord connection closed gracefully.")
	}
}

func handleMessage(s *discordgo.Session, m *discordgo.MessageCreate, targets map[string]struct{}, parse TradeParser, post PostCreator) {
	if m.Author.ID == s.State.User.ID {
		return
	}
	if _, ok := targets[m.ChannelID]; !ok {
		return
	}

	log.Printf("[I] [Discord] Processing message from '%s': \"%s\"", m.Author.Username, m.Content)

	go func() {
		tradeResult, err := parse(m.Content)
		if err != nil {
			log.Printf("[E] [Gemini] Failed to parse trade message from '%s': %v", m.Author.Username, err)
			return
		}
		if tradeResult == nil || len(tradeResult.Items) == 0 {
			log.Printf("[W] [Gemini] Returned no valid items for message from '%s'. Ignoring.", m.Author.Username)
			return
		}
		log.Printf("[I] [Gemini] Successfully parsed trade message. Found %d items.", len(tradeResult.Items))

		postIDs, err := post(m.Author.Username, m.Content, tradeResult)
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
	}()
}
