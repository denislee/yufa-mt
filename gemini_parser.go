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
	model := client.GenerativeModel("gemini-flash-latest")
	model.GenerationConfig.ResponseMIMEType = "application/json"

	prompt := fmt.Sprintf(`You are an expert at parsing trade messages for the game Ragnarok Online.
Analyze the following message and extract the trade information.
The user's intent will be either "buying" or "selling".
For each item, extract its base name, refinement level, number of slots, any attached cards, quantity, price, and currency.

- The base "name" should be the item's name WITHOUT any refinement prefix (e.g., +7), slots (e.g., [3]), or cards. For "+9 Jur [3] [Mummy Card]", the name is "Jur".
- "refinement" is the number after a '+'. If not present, it is 0.
- "slots" is the number inside square brackets, like [3]. If it's a card name like [Mummy Card], slots should be 0. If not present, it is 0.
- "card1", "card2", "card3", "card4" are the names of the cards in the item. If not present, the value should be an empty string "". Extract the card name, like "Mummy Card".
- If "quantity" is not mentioned, assume 1.
- If "price" is not mentioned, use 0.
- Prices can be written with 'k' for thousands (e.g., 500k = 500000) or 'kk' for millions (e.g., 1.5kk = 1500000). Convert all prices to a raw integer.
- "V>", "vendo", or "V>endo" means the action is "selling". "C>", "compro", or "C>ompro" means the action is "buying".
- If the user mentions "RMT" or "$", the "currency" should be "rmt". Otherwise, it is "zeny". This "currency" refers to the listed "price".
- **NEW**: Analyze the accepted payment types. This is separate from the listed price's currency.
- If the user *only* mentions zeny (k, kk), "payment_methods" should be "zeny".
- If the user *only* mentions RMT ($), "payment_methods" should be "rmt".
- If the user mentions *both* (e.g., "vendo por 2kk ou 10 reais", "aceito zeny ou rmt"), "payment_methods" should be "both".
- If no payment type is specified, default "payment_methods" to "zeny".
- Each item name can be in portuguese or english, and can be mixed in the same message.
- Try to fix typos, but do consider the context of Ragnarok Online game words.
- Keep the words "Carta" and "Card", if "Carta" exists, put on the begining of the item name; if "Card" exists, put on the end of the item name.
- replace the word: "peco peco" to "PecoPeco", "cavalo marinho" to "Cavalo-Marinho", "louva a deus" to "Louva-a-Deus"
- if the item is "thara", it is "Thara Frog Card"
- if the item is "louva-a-deus", it is "Carta Louva-a-Deus"
- if the item has slots, keep the slots on the name of the item, but always a space between the item name and the slots

Provide the output *only* as a single, minified JSON object. Do not wrap it in markdown backticks or any other text.
The JSON object must have two keys: "action" (string: "buying" or "selling") and "items" (an array of objects).
Each item object in the array must have these keys: "name" (string), "quantity" (integer), "price" (integer), "currency" (string: "zeny" or "rmt"), "payment_methods" (string: "zeny", "rmt", or "both"), "refinement" (integer), "slots" (integer), "card1" (string), "card2" (string), "card3" (string), "card4" (string).

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
