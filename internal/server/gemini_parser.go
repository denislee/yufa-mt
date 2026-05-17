package server

import (
	"os"

	"github.com/denislee/yufa-mt/internal/gemini"
)

// Aliases preserve the legacy main-package names while the implementation
// lives in internal/gemini.
type GeminiTradeItem = gemini.TradeItem
type GeminiTradeResult = gemini.TradeResult

// parseTradeMessageWithGemini is a compatibility shim. Prefer constructing
// a *gemini.Client once and calling its ParseTradeMessage.
func parseTradeMessageWithGemini(message string) (*GeminiTradeResult, error) {
	c := gemini.New(os.Getenv("GEMINI_API_KEY"))
	return c.ParseTradeMessage(message)
}
