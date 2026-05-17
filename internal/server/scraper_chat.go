package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// saveChatMessagesToDB inserts a batch of new messages in a single transaction.
func saveChatMessagesToDB(messages []ChatMessage) error {
	if len(messages) == 0 {
		return nil
	}

	// De-duplication logic
	// The key is the ChatMessage struct (Channel, CharacterName, Message).
	// This will now correctly de-duplicate retransmitted packets.
	seen := make(map[ChatMessage]struct{}, len(messages))
	dedupedMessages := make([]ChatMessage, 0, len(messages))

	for _, msg := range messages {
		if _, exists := seen[msg]; !exists {
			seen[msg] = struct{}{}
			dedupedMessages = append(dedupedMessages, msg)
		}
	}

	if len(dedupedMessages) == 0 {
		log.Printf("[D] [Scraper/Chat] Skipped saving batch of %d, all were duplicates.", len(messages))
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback() // Rollback on error

	// --- MODIFIED: SQL includes channel ---
	stmt, err := tx.Prepare("INSERT INTO chat (timestamp, channel, character_name, message) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	now := time.Now().Format(time.RFC3339)
	for _, msg := range dedupedMessages {
		// --- MODIFIED: Exec call includes msg.Channel ---
		if _, err := stmt.Exec(now, msg.Channel, msg.CharacterName, msg.Message); err != nil {
			log.Printf("[W] [Scraper/Chat] Failed to insert message from '%s' (%s): %v", msg.CharacterName, msg.Channel, err)
			// Continue inserting other messages
		}
	}

	log.Printf("[I] [Scraper/Chat] Saved %d new chat messages to DB (out of %d batched).", len(dedupedMessages), len(messages))
	return tx.Commit()
}

func logChatActivityPeriodically() {
	activityLogMutex.Lock()
	defer activityLogMutex.Unlock()

	now := time.Now()
	// Check if we've already logged an entry within the last minute
	if now.Sub(lastActivityLog) < 1*time.Minute {
		return // Already logged this minute
	}

	// It's a new minute (or the first log), so update the timestamp
	lastActivityLog = now

	// Store the timestamp truncated to the minute (e.g., 15:04:00)
	timestamp := now.Truncate(time.Minute).Format(time.RFC3339)

	// Use "INSERT OR IGNORE" to avoid errors on duplicate (which shouldn't
	// happen with the mutex, but it's safer)
	_, err := db.Exec("INSERT OR IGNORE INTO chat_activity_log (timestamp) VALUES (?)", timestamp)
	if err != nil {
		log.Printf("[E] [Scraper/Chat] Failed to log chat activity heartbeat: %v", err)
	} else if enableChatScraperDebugLogs {
		log.Printf("[D] [Scraper/Chat] Logged activity heartbeat for %s", timestamp)
	}
}

// logSystemStatusMessage injects a status message (Connect/Disconnect) into the chat DB.
func logSystemStatusMessage(message string) {
	// We use "System" as the channel so it appears in the "All" tab but is distinct from "Local".
	// We use "Status" as the character name.
	msg := ChatMessage{
		Timestamp:     time.Now().Format(time.RFC3339),
		Channel:       "System",
		CharacterName: "Status",
		Message:       message,
	}

	// Reuse the existing save function (wrapping single message in a slice)
	if err := saveChatMessagesToDB([]ChatMessage{msg}); err != nil {
		log.Printf("[E] [Scraper/Status] Failed to log system status: %v", err)
	} else {
		log.Printf("[I] [Scraper/Status] %s", message)
	}
}

// startChatPacketCapture is the new long-running service to replace processChatLogFile
func startChatPacketCapture(ctx context.Context) {
	log.Println("[I] [Scraper/Chat] Initializing live packet capture...")

	// --- 1. Find Network Device ---
	device := os.Getenv("CHAT_CAPTURE_DEVICE")
	if device == "" {
		// Use Go's standard 'net' package to find a suitable device.
		ifaces, err := net.Interfaces()
		if err != nil {
			log.Printf("[E] [Scraper/Chat] net.Interfaces() failed: %v. Chat capture disabled.", err)
			return
		}

		for _, i := range ifaces {
			// Check if interface is up and not a loopback
			isUp := (i.Flags & net.FlagUp) != 0
			isLoopback := (i.Flags & net.FlagLoopback) != 0

			if isUp && !isLoopback {
				// Check if it has a usable address
				addrs, err := i.Addrs()
				if err == nil && len(addrs) > 0 {
					device = i.Name
					log.Printf("[I] [Scraper/Chat] No CHAT_CAPTURE_DEVICE set. Auto-selected device: %s", device)
					break
				}
			}
		}

		if device == "" {
			log.Printf("[E] [Scraper/Chat] Could not find a suitable non-loopback network device. Please set CHAT_CAPTURE_DEVICE. Chat capture disabled.")
			return
		}
	}

	// --- 2. Get Port ---
	port := os.Getenv("CHAT_CAPTURE_PORT")
	if port == "" {
		port = "6121" // Default Ragnarok Online Char Server port
		log.Printf("[W] [Scraper/Chat] CHAT_CAPTURE_PORT not set. Defaulting to %s. This may not be correct.", port)
	}

	// --- 3. Open pcap Handle ---
	handle, err := pcap.OpenLive(device, 65536, true, pcap.BlockForever)
	if err != nil {
		log.Printf("[E] [Scraper/Chat] Failed to open pcap handle on %s: %v. (Do you have libpcap/Npcap installed and root/admin privileges?)", device, err)
		return
	}
	defer handle.Close()

	// --- 4. Set BPF Filter ---
	filter := fmt.Sprintf("tcp port %s", port)
	if err := handle.SetBPFFilter(filter); err != nil {
		log.Printf("[E] [Scraper/Chat] Failed to set BPF filter (%s): %v", filter, err)
		return
	}
	log.Printf("[I] [Scraper/Chat] Started packet capture on %s, filtering for %s.", device, filter)

	// --- 5. Start Packet Processing Loop ---
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	var newMessages []ChatMessage
	flushTicker := time.NewTicker(5 * time.Second) // Flush messages to DB every 5s
	defer flushTicker.Stop()

	// Status tracking
	isConnected := false
	const disconnectTimeout = 5 * time.Minute

	for {
		select {
		case <-ctx.Done():
			// Shutdown signal received
			log.Println("[I] [Scraper/Chat] Stopping packet capture...")
			if len(newMessages) > 0 {
				log.Println("[I] [Scraper/Chat] Flushing final batch of chat messages...")
				// Call and check error
				if err := saveChatMessagesToDB(newMessages); err != nil {
					log.Printf("[E] [Scraper/Chat] Error flushing final message batch to DB: %v", err)
				}
			}
			return

		case <-flushTicker.C:
			// 1. Periodic DB Flush
			if len(newMessages) > 0 {
				log.Printf("[I] [Scraper/Chat] Flushing %d batched messages to DB.", len(newMessages))
				// Call and check error
				if err := saveChatMessagesToDB(newMessages); err != nil {
					log.Printf("[E] [Scraper/Chat] Error flushing message batch to DB: %v", err)
				}
				newMessages = nil // Clear the batch
			}

			// 2. Watchdog: Check for Disconnection
			lastUnix := lastChatPacketTime.Load()
			if isConnected && lastUnix > 0 {
				lastPacketTime := time.Unix(lastUnix, 0)
				if time.Since(lastPacketTime) > disconnectTimeout {
					isConnected = false
					logSystemStatusMessage("🔴 Chat listener disconnected.")
				}
			}

		case packet := <-packetSource.Packets():

			// 1. Update the "last seen" time (for navbar and watchdog)
			lastChatPacketTime.Store(time.Now().Unix())

			// 2. Check for Reconnection
			if !isConnected {
				isConnected = true
				logSystemStatusMessage("🟢 Chat listener active/reconnected.")
			}

			// 3. Log this minute's activity for the graph
			logChatActivityPeriodically()

			if enableChatScraperDebugLogs {
				log.Printf("[D] [Scraper/Chat] Received packet. PktData size: %d", len(packet.Data()))
			}

			// We have a packet
			tcpLayer := packet.Layer(layers.LayerTypeTCP)
			if tcpLayer == nil {
				continue
			}
			tcp, _ := tcpLayer.(*layers.TCP)
			payload := tcp.Payload // This is the raw byte payload
			if len(payload) == 0 {
				continue
			}

			if enableChatScraperDebugLogs {
				log.Printf("[D] [Scraper/Chat] Found TCP packet. Payload size: %d bytes", len(payload))
				// Log the full payload in hex and as a sanitized string

				// Decode payload from Latin-1 for logging
				reader := transform.NewReader(bytes.NewReader(payload), charmap.ISO8859_1.NewDecoder())
				utf8Bytes, _ := io.ReadAll(reader) // Ignore error for logging

				sanitizedPayload := strings.Map(func(r rune) rune {
					if unicode.IsPrint(r) {
						return r
					}
					return '.' // Replace non-printable with a dot
				}, string(utf8Bytes)) // Use the decoded bytes

				log.Printf("[D] [Scraper/Chat] RAW PAYLOAD (HEX): %s", hex.EncodeToString(payload))
				log.Printf("[D] [Scraper/Chat] RAW PAYLOAD (STR): %s", sanitizedPayload)
			}

			// --- PARSING LOOP ---
			i := 0
			for i < len(payload) {
				firstPrefixIdx := -1
				var firstPacketDef chatPacketDefinition

				// Find the *closest* known prefix from our current position 'i'
				for _, packetDef := range knownChatPackets {
					idx := bytes.Index(payload[i:], packetDef.prefix)
					if idx != -1 { // Found this prefix
						if firstPrefixIdx == -1 || idx < firstPrefixIdx {
							firstPrefixIdx = idx
							firstPacketDef = packetDef
						}
					}
				}

				if firstPrefixIdx == -1 {
					break // No more known prefixes in this payload
				}

				absIdx := i + firstPrefixIdx // Absolute index in payload
				def := firstPacketDef        // The definition for the packet we found

				if enableChatScraperDebugLogs {
					log.Printf("[D] [Scraper/Chat] Matched prefix %s at index %d.", hex.EncodeToString(def.prefix), absIdx)
				}

				// Check if we have enough bytes to read the length (prefix + 2 bytes for length)
				if absIdx+4 > len(payload) {
					if enableChatScraperDebugLogs {
						log.Printf("[D] [Scraper/Chat] Fragmented header. Skipping.")
					}
					i = absIdx + 1 // Search after this partial prefix
					continue
				}

				// Read the packet length (2 bytes, little-endian)
				length := int(binary.LittleEndian.Uint16(payload[absIdx+2 : absIdx+4]))
				msgLen := length - def.headerLength // Use definition's header length
				msgEnd := absIdx + def.messageOffset + msgLen

				if enableChatScraperDebugLogs {
					log.Printf("[D] [Scraper/Chat] Parsed packet length: %d. Header: %d. Message length: %d. Required end index: %d", length, def.headerLength, msgLen, msgEnd)
				}

				if msgLen <= 0 {
					if enableChatScraperDebugLogs {
						log.Printf("[D] [Scraper/Chat] Invalid message length (%d). Skipping.", msgLen)
					}
					i = absIdx + 1
					continue
				}

				if msgEnd > len(payload) {
					if enableChatScraperDebugLogs {
						log.Printf("[D] [Scraper/Chat] Fragmented body. Need %d bytes, have %d. Skipping.", msgEnd, len(payload))
					}
					i = absIdx + 1 // Search after this partial prefix
					continue
				}

				// If we're here, we have a full message!
				// Use the definition's message offset
				msgBytes := payload[absIdx+def.messageOffset : msgEnd]
				if enableChatScraperDebugLogs {
					log.Printf("[D] [Scraper/Chat] Extracted message (raw hex): %s", hex.EncodeToString(msgBytes))
				}

				// Decode from Latin-1 (ISO-8859-1) to UTF-8
				reader := transform.NewReader(bytes.NewReader(msgBytes), charmap.ISO8859_1.NewDecoder())
				utf8Bytes, err := io.ReadAll(reader)
				if err != nil {
					log.Printf("[W] [Scraper/Chat] Failed to decode message from Latin-1: %v", err)
					// Fallback to the old method just in case
					utf8Bytes = msgBytes
				}

				// Trim NULL bytes *after* decoding
				message := string(bytes.Trim(utf8Bytes, "\x00"))

				// Sanitize
				message = strings.Map(func(r rune) rune {
					if unicode.IsPrint(r) {
						return r
					}
					return -1 // Discard
				}, message)
				message = strings.TrimSpace(message)

				if enableChatScraperDebugLogs && message != "" {
					log.Printf("[D] [Scraper/Chat] Sanitized message: '%s'", message)
				}

				// --- PARSING LOGIC ---
				if message != "" {
					var channel, charName, chatMsg string

					// Check for Drop Packet
					if bytes.Equal(def.prefix, []byte{0x9a, 0x00}) {
						channel = "Drop"
						// Check if it's the specific 0.01% drop message.
						if strings.Contains(message, "(chance: 0.01%)") && (strings.Contains(message, "got") || strings.Contains(message, "stole")) {
							// Parse for changelog
							dropMatches := dropMessageRegex.FindStringSubmatch(message)
							if len(dropMatches) == 4 {
								playerName := dropMatches[1]
								itemMsgFragment := dropMatches[3]

								// Now extract the item name from the fragment
								itemMatches := reItemFromDrop.FindStringSubmatch(itemMsgFragment)
								var itemName string
								if len(itemMatches) == 4 {
									if itemMatches[1] != "" {
										itemName = itemMatches[1]
									} else if itemMatches[2] != "" {
										itemName = itemMatches[2]
									} else if itemMatches[3] != "" {
										itemName = itemMatches[3]
									}
								}

								itemName = strings.TrimSpace(itemName)

								if playerName != "" && itemName != "" {
									go logDropToChangelog(time.Now().Format(time.RFC3339), playerName, itemName)
								}
							}

						} else {
							channel = "Announcement"
						}
						charName = "System"
						chatMsg = message
					} else if bytes.Equal(def.prefix, []byte{0xc3, 0x01}) {
						// This is a System/Event announcement packet (e.g., Invasion)
						channel = "Event"
						charName = "System"
						chatMsg = message
					} else if strings.HasPrefix(message, "[") && strings.Contains(message, "] ") {
						// Case: "[Global] golbin : bom dia!"
						channelPart, rest, _ := strings.Cut(message, "] ")
						channel = strings.TrimPrefix(channelPart, "[") // "Global"

						// Now parse the 'rest' for "char : msg"
						charNamePart, chatMsgPart, found := strings.Cut(rest, " : ")
						if found {
							// Standard: [Channel] Char : Msg
							charName = strings.TrimSpace(charNamePart) // "golbin"
							chatMsg = strings.TrimSpace(chatMsgPart)   // "bom dia!"
						} else {
							// No colon. Is it a system broadcast?
							if channel == "Notice" {
								charName = "System"
								chatMsg = strings.TrimSpace(rest)
							} else {
								// Discard non-chat messages (e.g. [Trade] M2LOKERO)
								chatMsg = ""
								if enableChatScraperDebugLogs {
									log.Printf("[D] [Scraper/Chat] Discarding non-chat message (no ' : ' in channel '%s'): '%s'", channel, message)
								}
							}
						}
					} else {
						// 2. No channel prefix, assume "Local"
						channel = "Local"

						// Case: "golbin : segunda aaa"
						charNamePart, chatMsgPart, found := strings.Cut(message, " : ")
						if found {
							// Standard: Char : Msg
							charName = strings.TrimSpace(charNamePart) // "golbin"
							chatMsg = strings.TrimSpace(chatMsgPart)   // "segunda aaa"
						} else {
							// No colon. Assume it's a local system broadcast.
							charName = "System"
							chatMsg = message
						}
					}

					// 3. Add to batch (if message is not empty)
					if chatMsg != "" {
						newMessages = append(newMessages, ChatMessage{
							Channel:       channel,
							CharacterName: charName,
							Message:       chatMsg,
						})
					} else if enableChatScraperDebugLogs {
						log.Printf("[D] [Scraper/Chat] Parsed an empty message. Discarding.")
					}
				}

				// Continue search *after* this full message
				i = msgEnd
			}
		}
	}
}

// logDropToChangelog inserts a drop event directly into the character_changelog table.
// This is called in real-time by the packet capture service.
func logDropToChangelog(timestamp, charName, itemName string) {
	if charName == "" || itemName == "" {
		return
	}

	activityDescription := "Dropped item: " + itemName

	// We perform a quick check to prevent duplicate entries if the packet is re-sent
	// and processed multiple times in a short window (e.g., 5 seconds).
	var exists int
	err := db.QueryRow(`
		SELECT 1 FROM character_changelog 
		WHERE character_name = ? 
		  AND activity_description = ? 
		  AND change_time > datetime(?, '-5 second') 
		LIMIT 1`,
		charName, activityDescription, timestamp,
	).Scan(&exists)

	// If it's not found (ErrNoRows) or another error, proceed with insert.
	// If it *is* found (err == nil), we skip.
	if err == nil {
		if enableChatScraperDebugLogs {
			log.Printf("[D] [Scraper/DropLog] Duplicate drop log detected for %s. Skipping.", charName)
		}
		return
	}

	// Insert the new drop log entry
	_, err = db.Exec(`
		INSERT INTO character_changelog (character_name, change_time, activity_description) 
		VALUES (?, ?, ?)`,
		charName, timestamp, activityDescription,
	)

	if err != nil {
		log.Printf("[E] [Scraper/DropLog] Failed to insert drop log for %s: %v", charName, err)
	} else if enableChatScraperDebugLogs {
		log.Printf("[D] [Scraper/DropLog] Successfully logged drop for %s: %s", charName, itemName)
	}
}
