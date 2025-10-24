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

	model := client.GenerativeModel("gemini-flash-latest")

	model.GenerationConfig.ResponseMIMEType = "application/json"

	prompt := fmt.Sprintf(`You are an expert at parsing trade messages for the game Ragnarok Online.
Analyze the following message and extract the trade information.
For each item, extract its base name, refinement level, number of slots, any attached cards, quantity, Zeny price, RMT price, and action.

- "V>" or "vendo" means the "action" for that item is "selling".
- "C>" or "compro" means the "action" for that item is "buying".
- A single message can contain both buying and selling items. Assign the correct "action" to each item.
- The structure of each item will be "V> name of the item to be sell" or "C> name of the item to be buyed"
- The base "name" should be the item's name WITHOUT any refinement prefix (e.g., +7), slots (e.g., [3]), or cards. For "+9 Jur [3] [Mummy Card]", the name is "Jur".
- "refinement" is the number after a '+'. If not present, it is 0.
- "slots" is the number inside square brackets, like [3]. If it's a card name like [Mummy Card], slots should be 0. If not present, it is 0.
- "card1", "card2", "card3", "card4" are the names of the cards in the item. If not present, the value should be an empty string "". Extract the card name, like "Mummy Card".
- If "quantity" is not mentioned, assume 1.
- An item can have a Zeny price (e.g., 500k, 1.5kk) and/or an RMT price (e.g., $10, 50 reais).
- Extract Zeny prices into "price_zeny" (as an integer). Convert 'k' (500k = 500000) and 'kk' (1.5kk = 1500000).
- Extract RMT prices into "price_rmt" (as an integer).
- If a specific price type (Zeny or RMT) is not mentioned for an item, its value should be 0.
- Example: "V> +7 Jur [3] 2kk or $5" -> name: "Jur", refinement: 7, slots: 3, price_zeny: 2000000, price_rmt: 5, action: "selling".
- **NEW**: Analyze the accepted payment types. This is separate from the listed price.
- If in the name contains "slotado" or "com slot", means that the item contains one slot and can be replaced by "[1]"
- If the user *only* mentions zeny (k, kk), "payment_methods" should be "zeny".
- If the user *only* mentions RMT ($), "payment_methods" should be "rmt".
- If the user mentions *both* (e.g., "vendo por 2kk ou 10 reais", "aceito zeny ou rmt"), "payment_methods" should be "both".
- If no payment type is specified, default "payment_methods" to "zeny".
- Each item name can be in portuguese or english, and can be mixed in the same message.
- Try to fix typos, but do consider the context of Ragnarok Online game words.
- Keep the words "Carta" and "Card", if "Carta" exists, put on the begining of the item name; if "Card" exists, put on the end of the item name.
- replace the word: "peco peco" to "PecoPeco", "cavalo marinho" to "Cavalo-Marinho", "louva a deus" to "Louva-a-Deus", "Besouro Ladrão" to "Besouro-Ladrão"
- "solta ou na presilha", means that is the card and the card in the Clip [1], 2 items.
- "(0)" means that the item has no slot.

- If the item is "rosario" or "rosário", the name is "Rosary" and has one slot.
- If the item is "cartola", the name is "Magician Ha".
- If the item is "peliz", the name is "Sobrepeliz" and has one slot.
- If the item is "vemb" or "vembrassa", the name is  "Guard" and has one slot.
- If the item is "thara", it is "Thara Frog Card"
- If the item is "druid" or "druida", it is "Evil Druid Card"
- If the item is "esporo card", it is "Spore Card"
- If the item is "carta marc", it is "Marc Card"
- If the item is "louva-a-deus", it is "Carta Louva-a-Deus"
- If the item is "popohat" or "popo hat", it is "Poo Poo Hat"
- If the item is "ESPADA DE 2 MAOS", it is "Espada de Duas Mãos"
- If the item is "chave sub", it is "Chave para o Subterrâneo"
- If the item is "Sappo de Roda" or "sappo de roda", it is "Carta Sapo de Rodda"
- If the item is "composto", it is "Arco Composto"
- If the item is "presilha de cura", it is "Clip" with "vitata card" in it
- If the item name contains "Guilherme Tell" or is "AoA", it is "Apple of Archer"
- If the item is "Cota de Malha do Pânico", it is "Cota de Malha"
- If the item has "SW" in the name, it is "Skeleton Worker"
- If the item is "Gakk", it is "Gakkung Bow"
- If the item is "pesadelo", it is "Carta Pesadelo"

- An item only have card if the item has the number of slots equal or more than number of cards in the item
- If the item has slots, keep the slots on the name of the item, but always a space between the item name and the slots

- **SPECIAL CASE: Zeny for RMT Sales**:
- If the user is selling Zeny (kk) for RMT ($, reais), this is a special item. The "action" is "selling".
- **Unit Sale (e.g., "V>10kk 25 reais cada")**: "cada" implies "per 1kk".
    - "name": "Zeny (1kk pack)"
    - "quantity": 10 (the total number of kk's being sold)
    - "price_zeny": 0
    - "price_rmt": 25 (the RMT price per kk)
    - "payment_methods": "rmt"
    - "action": "selling"
    - refinement, slots, cards must be 0 or "".
- **Bulk Sale (e.g., "VENDO 13KK a 300$ no Pix")**: This is a single package.
    - "name": "Zeny (13kk pack)" (Note: the quantity is in the name)
    - "quantity": 1 (it's one pack)
    - "price_zeny": 0
    - "price_rmt": 300 (the total RMT price for the pack)
    - "payment_methods": "rmt"
    - "action": "selling"
    - refinement, slots, cards must be 0 or "".
- **General Sale (e.g., "V> KK", "V> Zeny")**: If the user lists "Zeny" or "KK" as an item they are *selling* ("V>"), but does not specify a price or amount (unlike the Unit or Bulk sale cases). This is still treated as a Zeny-for-RMT sale.
    - "name": "Zeny"
    - "quantity": 1 (default)
    - "price_zeny": 0
    - "price_rmt": 0
    - "currency": "rmt"
    - "payment_methods": "rmt"
    - "action": "selling"
    - refinement, slots, cards must be 0 or "".
- Do not parse regular items if the message is clearly just a Zeny for RMT sale (like the Unit or Bulk sale examples). If it's a mix (like "V> KK" and "V> Carta Hydra (RMT)"), parse all items.

Provide the output *only* as a single, minified JSON object. Do not wrap it in markdown backticks or any other text.
The JSON object must have one key: "items" (an array of objects).
Each item object in the array must have these keys: "name" (string), "action" (string: "buying" or "selling"), "quantity" (integer), "price_zeny" (integer), "price_rmt" (integer), "payment_methods" (string: "zeny", "rmt", or "both"), "refinement" (integer), "slots" (integer), "card1" (string), "card2" (string), "card3" (string), "card4" (string).

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
