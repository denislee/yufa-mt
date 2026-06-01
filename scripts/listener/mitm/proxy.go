// yufa-mitm — a transparent local TCP proxy that auto-enters the character-select
// PIN by injecting a crafted packet, so the real client advances past the
// randomized keypad with NO mouse click and NO Gepard bypass.
//
// How it fits together (see ../GEPARD-PROTOCOL-FINDINGS.md and ../pinpad/):
//   - The char server (port 7121) speaks PLAINTEXT. When the PIN dialog opens it
//     sends HC_SECOND_PASSWD_LOGIN (0x08b9): b9 08 | seed(u32 LE) | AID(u32 LE) |
//     state(u16 LE), state 1 = "enter PIN".
//   - The client would normally show the shuffled keypad, you click, and it sends
//     CH_SECOND_PASSWD_ACK (0x08b8): b8 08 | AID(u32 LE) | <4 ascii slot digits>.
//   - This proxy sits on the char connection (via iptables REDIRECT; see
//     run-proxy.sh). It relays every byte untouched EXCEPT: on seeing 0x08b9
//     state=1 it reads the cleartext seed, computes the slot digits for your real
//     PIN (the rAthena pincode shuffle — identical to pinpad.go, self-tested
//     below), and writes a 0x08b8 to the server itself. The server replies
//     0x08b9 state=0 (OK), which is relayed to the client, and the client closes
//     the keypad and proceeds to char-select → map → zone on its own.
//
// Because the proxy terminates both sides, TCP stays consistent (unlike raw
// packet injection, which desyncs sequence numbers). The injected 0x08b8 never
// passes through the client's own send(), so Gepard (which hooks the client's
// socket) never sees an anomalous outbound packet. Login (7900) and zone (6121)
// are NOT proxied — only char (7121) is — so the client still does its own
// Gepard handshakes directly.
//
// Run via run-proxy.sh (sets up iptables and runs this as root). Config is env:
//
//	LISTEN_ADDR  (default 127.0.0.1:7799)  where REDIRECT delivers connections
//	CHAR_PORT    (default 7121)            only inject when orig dst port == this
//	PIN          (required to inject)      your real character-select PIN
//	UPSTREAM     (optional)                fixed dial target; if unset, use SO_ORIGINAL_DST
//	INJECT       (default 1)               set 0 for a pure transparent relay
//
// Subcommand: `proxy selftest` runs the shuffle against 15 captured sessions.
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const soOriginalDst = 80 // netfilter SO_ORIGINAL_DST (linux/netfilter_ipv4.h)

// ---- rAthena pincode shuffle (keep in sync with ../pinpad/pinpad.go; the
// algorithm is fixed in rAthena and is self-tested by `selftest`). -------------

func shuffleTab(seed uint32) [10]int {
	var tab [10]int
	for i := range tab {
		tab[i] = i
	}
	const multiplier, baseSeed = 0x3498, 0x881234
	s := seed
	for i := 1; i < 10; i++ {
		s = uint32(baseSeed + uint64(s)*multiplier)
		pos := int(s % uint32(i+1))
		if i != pos {
			tab[i], tab[pos] = tab[pos], tab[i]
		}
	}
	return tab
}

// solveSlots maps a real PIN to the slot indices the client would send (the
// digits inside 0x08b8). slot for digit D is the i where tab[i] == D.
func solveSlots(seed uint32, pin string) (string, error) {
	tab := shuffleTab(seed)
	var inv [10]int
	for i, d := range tab {
		inv[d] = i
	}
	var b strings.Builder
	for _, c := range pin {
		if c < '0' || c > '9' {
			return "", fmt.Errorf("PIN must be digits only")
		}
		b.WriteByte(byte('0' + inv[c-'0']))
	}
	return b.String(), nil
}

// ---- session state ----------------------------------------------------------

type session struct {
	id       int
	client   net.Conn
	server   net.Conn
	upMu     sync.Mutex // serializes writes to the server (relay + injection)
	pin      string
	charPort uint16
	dstPort  uint16

	aid     uint32
	haveAID bool
	pinDone bool
}

func (s *session) writeServer(b []byte) error {
	s.upMu.Lock()
	defer s.upMu.Unlock()
	_, err := s.server.Write(b)
	return err
}

// captureAID learns the account ID from the first CH_ENTER (0x0065) the client
// sends: 65 00 | AID(u32 LE) | ...  Needed to validate 0x08b9 and build 0x08b8.
func (s *session) captureAID(b []byte) {
	if s.haveAID || len(b) < 6 {
		return
	}
	if b[0] == 0x65 && b[1] == 0x00 {
		s.aid = binary.LittleEndian.Uint32(b[2:6])
		s.haveAID = true
		log.Printf("[%d] AID = %d (0x%x)", s.id, s.aid, s.aid)
	}
}

// findPinRequest scans server→client bytes for a valid 0x08b9 state=1 record and
// returns its seed. Validates the embedded AID against the session's to avoid
// false positives from 0xb9 0x08 appearing inside other packet data.
func (s *session) findPinRequest(buf []byte) (seed uint32, ok bool) {
	for i := 0; i+12 <= len(buf); i++ {
		if buf[i] != 0xb9 || buf[i+1] != 0x08 {
			continue
		}
		rec := buf[i : i+12]
		state := binary.LittleEndian.Uint16(rec[10:12])
		if state != 1 {
			continue
		}
		aid := binary.LittleEndian.Uint32(rec[6:10])
		if s.haveAID && aid != s.aid {
			continue
		}
		return binary.LittleEndian.Uint32(rec[2:6]), true
	}
	return 0, false
}

func buildPinAck(aid uint32, slots string) []byte {
	out := []byte{0xb8, 0x08}
	out = binary.LittleEndian.AppendUint32(out, aid)
	return append(out, []byte(slots)...)
}

// clientToServer relays C→S and snoops CH_ENTER for the AID.
func (s *session) clientToServer() {
	buf := make([]byte, 32*1024)
	for {
		n, err := s.client.Read(buf)
		if n > 0 {
			s.captureAID(buf[:n])
			if werr := s.writeServer(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	s.server.Close()
	s.client.Close()
}

// serverToClient relays S→C and injects the PIN ack on the first 0x08b9 state=1.
func (s *session) serverToClient(inject bool) {
	buf := make([]byte, 32*1024)
	var pend []byte // small rolling window to catch a record split across reads
	for {
		n, err := s.server.Read(buf)
		if n > 0 {
			if _, werr := s.client.Write(buf[:n]); werr != nil {
				break
			}
			if inject && !s.pinDone && s.dstPort == s.charPort {
				pend = append(pend, buf[:n]...)
				if seed, ok := s.findPinRequest(pend); ok {
					s.handlePin(seed)
					pend = nil
				} else if len(pend) > 16 {
					pend = pend[len(pend)-16:]
				}
			}
		}
		if err != nil {
			break
		}
	}
	s.server.Close()
	s.client.Close()
}

func (s *session) handlePin(seed uint32) {
	if !s.haveAID {
		log.Printf("[%d] PIN request (seed=%d) but AID unknown yet; skipping injection", s.id, seed)
		return
	}
	slots, err := solveSlots(seed, s.pin)
	if err != nil {
		log.Printf("[%d] cannot solve PIN: %v", s.id, err)
		return
	}
	pkt := buildPinAck(s.aid, slots)
	if werr := s.writeServer(pkt); werr != nil {
		log.Printf("[%d] failed to inject 0x08b8: %v", s.id, werr)
		return
	}
	s.pinDone = true
	log.Printf("[%d] PIN injected: seed=%d (0x%x) -> slots %q (0x08b8 %x)", s.id, seed, seed, slots, pkt)
}

// ---- transparent-proxy plumbing ---------------------------------------------

// origDst recovers the pre-REDIRECT destination via SO_ORIGINAL_DST.
func origDst(c *net.TCPConn) (netip.AddrPort, error) {
	sc, err := c.SyscallConn()
	if err != nil {
		return netip.AddrPort{}, err
	}
	var mreq *unix.IPv6Mreq
	var ge error
	cerr := sc.Control(func(fd uintptr) {
		mreq, ge = unix.GetsockoptIPv6Mreq(int(fd), unix.SOL_IP, soOriginalDst)
	})
	if cerr != nil {
		return netip.AddrPort{}, cerr
	}
	if ge != nil {
		return netip.AddrPort{}, ge
	}
	b := mreq.Multiaddr // sockaddr_in laid over the 16-byte field
	ip := netip.AddrFrom4([4]byte{b[4], b[5], b[6], b[7]})
	port := uint16(b[2])<<8 | uint16(b[3]) // network byte order
	return netip.AddrPortFrom(ip, port), nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "selftest" {
		os.Exit(selftest())
	}

	listenAddr := env("LISTEN_ADDR", "127.0.0.1:7799")
	pin := os.Getenv("PIN")
	if pin == "" {
		pin = os.Getenv("YUFA_PIN")
	}
	inject := env("INJECT", "1") != "0"
	charPort := uint16(atoiDefault(env("CHAR_PORT", "7121"), 7121))
	upstream := os.Getenv("UPSTREAM") // optional fallback

	if inject && pin == "" {
		log.Fatal("INJECT=1 but no PIN set (export PIN=<digits> or YUFA_PIN); use INJECT=0 for a pure relay")
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}
	log.Printf("yufa-mitm listening on %s  inject=%v charPort=%d", listenAddr, inject, charPort)
	if !inject {
		log.Printf("INJECT=0: pure transparent relay (no PIN injection)")
	}

	id := 0
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		id++
		go handle(id, c.(*net.TCPConn), pin, charPort, upstream, inject)
	}
}

func handle(id int, client *net.TCPConn, pin string, charPort uint16, upstream string, inject bool) {
	// UPSTREAM, when set, is a fixed dial target (simple mode + deterministic
	// tests). Otherwise recover the pre-REDIRECT destination via SO_ORIGINAL_DST.
	target := upstream
	if target == "" {
		dst, err := origDst(client)
		if err != nil {
			log.Printf("[%d] cannot determine upstream (SO_ORIGINAL_DST: %v, no UPSTREAM); closing", id, err)
			client.Close()
			return
		}
		target = dst.String()
	}

	server, err := net.Dial("tcp", target)
	if err != nil {
		log.Printf("[%d] dial upstream %s: %v", id, target, err)
		client.Close()
		return
	}
	dstPort := portOf(target)
	log.Printf("[%d] %s -> %s (port %d)", id, client.RemoteAddr(), target, dstPort)

	s := &session{
		id: id, client: client, server: server,
		pin: pin, charPort: charPort, dstPort: dstPort,
	}
	go s.clientToServer()
	s.serverToClient(inject)
}

// ---- helpers ----------------------------------------------------------------

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func atoiDefault(s string, def int) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	if s == "" {
		return def
	}
	return n
}

func portOf(hostport string) uint16 {
	if ap, err := netip.ParseAddrPort(hostport); err == nil {
		return ap.Port()
	}
	if _, p, err := net.SplitHostPort(hostport); err == nil {
		return uint16(atoiDefault(p, 0))
	}
	return 0
}

// ---- self-test against real captured sessions (all decrypt to PIN 9090) -----

func selftest() int {
	type tv struct {
		seed uint32
		sent string
	}
	vec := []tv{
		{32125, "0101"}, {21247, "8686"}, {34791, "4141"}, {46298, "6565"},
		{30632, "6161"}, {46740, "8181"}, {36025, "0101"}, {31422, "2929"},
		{28050, "0101"}, {43980, "6767"}, {22998, "0101"}, {48297, "2626"},
		{8571, "2626"}, {36894, "2525"}, {220, "4545"},
	}
	const want = "9090"
	fail := 0
	for _, t := range vec {
		// solveSlots(seed, want) must reproduce the captured sent bytes, and the
		// build must match the 0x08b8 layout (b8 08 | aid | slots).
		got, err := solveSlots(t.seed, want)
		if err != nil || got != t.sent {
			fmt.Printf("FAIL seed=%d want sent %q got %q (%v)\n", t.seed, t.sent, got, err)
			fail++
		}
	}
	// spot-check the packet builder against the amigo_play 0x08b8.
	pkt := buildPinAck(0x001ea65d, "4545")
	wantPkt := []byte{0xb8, 0x08, 0x5d, 0xa6, 0x1e, 0x00, '4', '5', '4', '5'}
	if string(pkt) != string(wantPkt) {
		fmt.Printf("FAIL buildPinAck = %x want %x\n", pkt, wantPkt)
		fail++
	}
	if fail == 0 {
		fmt.Printf("PASS: 15/15 sessions reproduce sent slots for PIN %s; 0x08b8 builder matches capture.\n", want)
		return 0
	}
	fmt.Printf("FAILED: %d\n", fail)
	return 1
}
