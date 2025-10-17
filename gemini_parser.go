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

	model := client.GenerativeModel("gemini-flash-lite-latest")
	// Configure the model to expect a JSON response.
	model.GenerationConfig.ResponseMIMEType = "application/json"

	prompt := fmt.Sprintf(`You are an expert at parsing trade messages for the game Ragnarok Online.
Analyze the following message and extract the trade information.
The user's intent will be either "buying" or "selling".
For each item, extract its name, quantity, and price in zeny.
- If quantity is not mentioned, assume 1.
- If price is not mentioned, use 1.
- Prices can be written with 'k' for thousands (e.g., 500k = 500000) or 'kk' for millions (e.g., 1.5kk = 1500000).
- Convert all prices to a raw integer. Remove any periods or commas. For example, "1.500.000" should become 1500000.
- the string "V>" means the user wants to sell and the string "C>" means the user wants to buy
- if users uses "RMT" means that he wants the transaction using real money, in this case, Reais (Brazilian)
- the item name can be in portuguese or in english	

Provide the output *only* as a single, minified JSON object. Do not wrap it in markdown backticks or any other text.
The JSON object must have two keys: "action" (string, must be "buying" or "selling") and "items" (an array of objects).
Each item object in the array must have three keys: "name" (string), "quantity" (integer), and "price" (integer).

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
