// pinpad — solve Projeto Yufa's randomized character-select PIN keypad.
//
// The keypad is the stock rAthena "secure pincode" shuffle. Each time the PIN
// dialog appears, the char server (port 7121) sends HC_SECOND_PASSWD_LOGIN
// (opcode 0x08b9) in CLEARTEXT:
//
//	b9 08 | seed(uint32 LE) | AID(uint32 LE) | state(uint16 LE)
//	         state 0x0001 = "enter PIN"   state 0x0000 = "accepted"
//
// The client derives a digit permutation `tab` from `seed`, draws the 10 keypad
// buttons so that the button at slot i shows digit tab[i], and — when you click
// a button — sends that button's SLOT INDEX. The server then recovers the real
// digit as real = tab[slotIndexSent]. That is exactly char_pincode_decrypt()
// in rAthena/src/char/char.cpp.
//
// Consequences for automation:
//   - We can sniff `seed` off the wire (char server traffic is unencrypted; see
//     watch-seed.sh), with no need to break Gepard.
//   - Given seed + your real PIN we compute, for each PIN digit D, the slot to
//     click: slot = index i where tab[i] == D. Clicking those slots (in order)
//     plus OK enters the PIN. The slot->screen-pixel map is a one-time
//     calibration (see README.md); only the labels reshuffle each session.
//
// This tool has NO external dependencies and ships a self-test against 15 real
// captured sessions (all of which must decrypt to the same PIN). Run:
//
//	go run pinpad.go verify
//	go run pinpad.go solve --seed 0xdc00 --pin 9090
//	echo dc000000 | go run pinpad.go solve --pin 1122   # seed as raw LE hex on stdin
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// shuffleTab reproduces rAthena char_pincode_decrypt's permutation table.
// tab[i] is the digit displayed on keypad slot i for this session's seed.
func shuffleTab(seed uint32) [10]int {
	var tab [10]int
	for i := range tab {
		tab[i] = i
	}
	const multiplier, baseSeed = 0x3498, 0x881234
	s := seed
	for i := 1; i < 10; i++ {
		s = uint32(baseSeed + uint64(s)*multiplier) // wraps at 2^32, matching C uint32
		pos := int(s % uint32(i+1))
		if i != pos {
			tab[i], tab[pos] = tab[pos], tab[i]
		}
	}
	return tab
}

// decrypt maps what the client SENT (slot indices, as a digit string) back to
// the real PIN — i.e. the server side. real[k] = tab[sent[k]].
func decrypt(seed uint32, sent string) string {
	tab := shuffleTab(seed)
	var b strings.Builder
	for _, c := range sent {
		if c < '0' || c > '9' {
			return ""
		}
		b.WriteString(strconv.Itoa(tab[c-'0']))
	}
	return b.String()
}

// solve maps a real PIN to the slot indices to click (== the bytes the client
// will send in 0x08b8). slot s for digit D is the i where tab[i] == D.
func solve(seed uint32, pin string) (slots string, ok bool) {
	tab := shuffleTab(seed)
	inv := [10]int{}
	for i, d := range tab {
		inv[d] = i
	}
	var b strings.Builder
	for _, c := range pin {
		if c < '0' || c > '9' {
			return "", false
		}
		b.WriteString(strconv.Itoa(inv[c-'0']))
	}
	return b.String(), true
}

// parseSeed accepts 0x-hex (interpreted as the numeric seed), bare decimal, or
// raw little-endian byte hex like "dc000000" (as it appears on the wire).
func parseSeed(s string) (uint32, error) {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X"):
		v, err := strconv.ParseUint(s[2:], 16, 32)
		return uint32(v), err
	case len(s) == 8 && isHex(s): // 4-byte LE on-wire form
		raw, err := hex.DecodeString(s)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(raw), nil
	default:
		v, err := strconv.ParseUint(s, 10, 32)
		return uint32(v), err
	}
}

func isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

// testVectors are real (seed, sentSlots) pairs from ~/tmp/amigo/*.pcapng. The
// account's true PIN was 9090; every session must decrypt to it.
var testVectors = []struct {
	name string
	seed uint32
	sent string
}{
	{"amigo_01", 32125, "0101"}, {"amigo_02", 21247, "8686"},
	{"amigo_03", 34791, "4141"}, {"amigo_04", 46298, "6565"},
	{"amigo_05", 30632, "6161"}, {"amigo_06", 46740, "8181"},
	{"amigo_07", 36025, "0101"}, {"amigo_08", 31422, "2929"},
	{"amigo_09", 28050, "0101"}, {"amigo_10", 43980, "6767"},
	{"amigo_11", 22998, "0101"}, {"amigo_12", 48297, "2626"},
	{"amigo_13", 8571, "2626"}, {"amigo_14", 36894, "2525"},
	{"amigo_play", 220, "4545"},
}

const wantPIN = "9090"

func cmdVerify() int {
	fail := 0
	fmt.Printf("%-12s %8s %6s -> %s\n", "capture", "seed", "sent", "real")
	for _, tv := range testVectors {
		got := decrypt(tv.seed, tv.sent)
		mark := "ok"
		if got != wantPIN {
			mark, fail = "FAIL", fail+1
		}
		fmt.Printf("%-12s %8d %6s -> %s  %s\n", tv.name, tv.seed, tv.sent, got, mark)

		// Round-trip: solving wantPIN against this seed must reproduce tv.sent.
		if slots, ok := solve(tv.seed, wantPIN); !ok || slots != tv.sent {
			fmt.Printf("    round-trip MISMATCH: solve(seed,%s)=%q want %q\n", wantPIN, slots, tv.sent)
			fail++
		}
	}
	if fail == 0 {
		fmt.Println("\nPASS: all 15 sessions decrypt to", wantPIN, "and round-trip cleanly.")
		return 0
	}
	fmt.Printf("\nFAILED: %d mismatches\n", fail)
	return 1
}

func cmdSolve(args []string) int {
	var seedStr, pin string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--seed":
			i++
			if i < len(args) {
				seedStr = args[i]
			}
		case "--pin":
			i++
			if i < len(args) {
				pin = args[i]
			}
		}
	}
	if seedStr == "" { // allow seed on stdin (e.g. piped from watch-seed.sh)
		sc := bufio.NewScanner(os.Stdin)
		if sc.Scan() {
			seedStr = strings.TrimSpace(sc.Text())
		}
	}
	if seedStr == "" || pin == "" {
		fmt.Fprintln(os.Stderr, "usage: pinpad solve --seed <0xhex|dec|LEhex> --pin <digits>")
		return 2
	}
	seed, err := parseSeed(seedStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad seed:", err)
		return 2
	}
	tab := shuffleTab(seed)
	slots, ok := solve(seed, pin)
	if !ok {
		fmt.Fprintln(os.Stderr, "PIN must be digits only")
		return 2
	}
	fmt.Printf("seed            : %d (0x%x)\n", seed, seed)
	fmt.Printf("slot -> shows   : ")
	for i, d := range tab {
		fmt.Printf("[%d]=%d ", i, d)
	}
	fmt.Println()
	fmt.Printf("real PIN        : %s\n", pin)
	fmt.Printf("click slots     : %s   (click these slot positions in order, then OK)\n", spaced(slots))
	fmt.Printf("0x08b8 payload  : %s   (the digit-string the client sends)\n", slots)
	return 0
}

func spaced(s string) string {
	parts := make([]string, len(s))
	for i, c := range s {
		parts[i] = string(c)
	}
	return strings.Join(parts, " ")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: pinpad {verify|solve ...}")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "verify":
		os.Exit(cmdVerify())
	case "solve":
		os.Exit(cmdSolve(os.Args[2:]))
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", os.Args[1])
		os.Exit(2)
	}
}
