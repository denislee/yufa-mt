package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/google/generative-ai-go/genai"
	"google.golang.org/api/option"
)

// parseTradeMessageWithGemini sends a trade message to the Gemini API for analysis.
// It expects the API to return a JSON object that can be unmarshalled into a GeminiTradeResult struct.
func parseTradeMessageWithGemini(message string) (*GeminiTradeResult, error) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("GEMINI_API_KEY environment variable not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := genai.NewClient(ctx, option.WithAPIKey(apiKey))
	if err != nil {
		return nil, fmt.Errorf("failed to create genai client: %w", err)
	}
	defer client.Close()

	// Configure the model to expect a JSON response.
	model := client.GenerativeModel("gemini-flash-lite-latest")
	model.GenerationConfig.ResponseMIMEType = "application/json"

	prompt := fmt.Sprintf(`You are an expert at parsing trade messages for the game Ragnarok Online.
Analyze the following message and extract the trade information.
The user's intent will be either "buying" or "selling".
For each item, extract its base name, refinement level, any attached cards, quantity, price, and currency.

- The base "name" should be the item's name WITHOUT any refinement prefix (e.g., +7) or cards. For "+9 Jur [3] [Mummy Card]", the name is "Jur".
- "refinement" is the number after a '+'. If not present, it is 0.
- "card1", "card2", "card3", "card4" are the names of the cards in the item. If not present, the value should be an empty string "". Extract the card name, like "Mummy Card".
- If "quantity" is not mentioned, assume 1.
- If "price" is not mentioned, use 1.
- Prices can be written with 'k' for thousands (e.g., 500k = 500000) or 'kk' for millions (e.g., 1.5kk = 1500000). Convert all prices to a raw integer.
- "V>", "vendo", or "V>endo" means the action is "selling". "C>", "compro", or "C>ompro" means the action is "buying".
- If the user mentions "RMT" or "$", the "currency" should be "rmt". Otherwise, it is "zeny".
- The item name can be in portuguese or english.
- Try to fix the name of the item if there is typo, still considering the context of Ragnarok Online

Provide the output *only* as a single, minified JSON object. Do not wrap it in markdown backticks or any other text.
The JSON object must have two keys: "action" (string: "buying" or "selling") and "items" (an array of objects).
Each item object in the array must have these keys: "name" (string), "quantity" (integer), "price" (integer), "currency" (string: "zeny" or "rmt"), "refinement" (integer), "card1" (string), "card2" (string), "card3" (string), "card4" (string).

Here is the message to parse:
---
%s
---`, message)

	log.Println("🤖 Sending request to Gemini API...")
	resp, err := model.GenerateContent(ctx, genai.Text(prompt))
	if err != nil {
		return nil, fmt.Errorf("failed to generate content from Gemini: %w", err)
	}

	if len(resp.Candidates) == 0 || resp.Candidates[0].Content == nil || len(resp.Candidates[0].Content.Parts) == 0 {
		return nil, fmt.Errorf("received an empty or invalid response from Gemini API")
	}

	rawJSON := fmt.Sprintf("%s", resp.Candidates[0].Content.Parts[0])

	// Clean up potential markdown formatting from the response, just in case.
	re := regexp.MustCompile("(?s)```json(.*)```")
	matches := re.FindStringSubmatch(rawJSON)
	if len(matches) > 1 {
		rawJSON = matches[1]
	}
	rawJSON = strings.TrimSpace(rawJSON)

	log.Printf("🤖 Received JSON response from Gemini: %s", rawJSON)

	var result GeminiTradeResult
	if err := json.Unmarshal([]byte(rawJSON), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON from Gemini: %w. Raw response: %s", err, rawJSON)
	}

	return &result, nil
}
