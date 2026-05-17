package server

import (
	"context"

	"github.com/denislee/yufa-mt/internal/discord"
)

// startDiscordBot is a thin shim that hands the discord package the
// callbacks it needs (trade parse + post create). The real bot lives in
// internal/discord.
func startDiscordBot(ctx context.Context) {
	discord.Start(
		ctx,
		appConfig.DiscordBotToken,
		appConfig.DiscordChannelIDs,
		parseTradeMessageWithGemini,
		CreateTradingPostFromDiscord,
	)
}
