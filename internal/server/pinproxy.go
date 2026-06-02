//go:build linux

// pinproxy.go — an in-process transparent TCP proxy that auto-enters the
// character-select PIN by injecting a crafted packet, so the real game client
// advances past the randomized keypad with NO mouse click and NO Gepard bypass.
//
// This is the standalone scripts/listener/mitm/proxy.go ported into the main
// app: it starts as a background service (see startBackgroundJobs) when
// PIN_PROXY=1, installs its own iptables REDIRECT, and tears it down on
// shutdown. Running it here — instead of as the old root yufa-mitm-proxy
// service — means the proxy is already up by the time the listener logs the
// client in.
//
// How it fits together (see scripts/listener/GEPARD-PROTOCOL-FINDINGS.md and
// scripts/listener/pinpad/):
//   - The char server (PinProxyCharPort, default 7121) speaks PLAINTEXT. When
//     the PIN dialog opens it sends HC_SECOND_PASSWD_LOGIN (0x08b9):
//     b9 08 | seed(u32 LE) | AID(u32 LE) | state(u16 LE), state 1 = "enter PIN".
//   - The client would normally show the shuffled keypad, you click, and it
//     sends CH_SECOND_PASSWD_ACK (0x08b8): b8 08 | AID(u32 LE) | <4 slot digits>.
//   - This proxy sits on the char connection (via iptables REDIRECT). It relays
//     every byte untouched EXCEPT: on seeing 0x08b9 state=1 it reads the
//     cleartext seed, computes the slot digits for the real PIN (the rAthena
//     pincode shuffle, self-tested in the standalone proxy), and writes a
//     0x08b8 to the server itself. The server replies 0x08b9 state=0 (OK),
//     which is relayed to the client, and the client closes the keypad and
//     proceeds to char-select → map → zone on its own.
//
// Because the proxy terminates both sides, TCP stays consistent (unlike raw
// packet injection). The injected 0x08b8 never passes through the client's own
// send(), so Gepard (which hooks the client's socket) never sees an anomalous
// outbound packet. Login (7900) and zone (6121) are NOT proxied — only char
// (7121) is — so the client still does its own Gepard handshakes directly.
//
// Loop avoidance: the standalone proxy ran as root and excluded its own onward
// connection with `-m owner --uid-owner 0 -j RETURN`. In-process we share the
// desktop user's uid with the game client, so that uid trick can't tell them
// apart. Instead we tag the proxy's onward sockets with an fwmark (ppFwMark)
// and RETURN on `-m mark`, which needs CAP_NET_ADMIN — the same capability the
// REDIRECT install needs, and the one the binary already carries.
package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	ppSoOriginalDst = 80 // netfilter SO_ORIGINAL_DST (linux/netfilter_ipv4.h)

	// ppFwMark tags the proxy's own onward (upstream) connections so the
	// iptables REDIRECT skips them. Any value not used by other fwmark
	// consumers on the box works; 0x59554641 spells "YUFA".
	ppFwMark = 0x59554641
)

// Injection progress on the char connection.
const (
	ppStepWantPIN    = iota // waiting for 0x08b9 state=1 → inject 0x08b8 (PIN)
	ppStepWantSelect        // waiting for 0x08b9 state=0 → inject 0x0066 (char select)
	ppStepDone
)

// ---- rAthena pincode shuffle (mirrors scripts/listener/pinpad/pinpad.go; the
// algorithm is fixed in rAthena and self-tested by `proxy selftest`). ---------

func ppShuffleTab(seed uint32) [10]int {
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

// ppSolveSlots maps a real PIN to the slot indices the client would send (the
// digits inside 0x08b8). The slot for digit D is the i where tab[i] == D.
func ppSolveSlots(seed uint32, pin string) (string, error) {
	tab := ppShuffleTab(seed)
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

// CH_SECOND_PASSWD_ACK: b8 08 | AID(u32 LE) | <ascii slot digits>
func ppBuildPinAck(aid uint32, slots string) []byte {
	out := []byte{0xb8, 0x08}
	out = binary.LittleEndian.AppendUint32(out, aid)
	return append(out, []byte(slots)...)
}

// CH_SELECT_CHAR: 66 00 | slot(u8)
func ppBuildCharSelect(slot int) []byte {
	return []byte{0x66, 0x00, byte(slot)}
}

// ---- session state ----------------------------------------------------------

type pinSession struct {
	id       int
	client   net.Conn
	server   net.Conn
	upMu     sync.Mutex // serializes writes to the server (relay + injection)
	pin      string
	charPort uint16
	dstPort  uint16

	aid        uint32
	haveAID    bool
	step       int
	selectSlot int // CH_SELECT_CHAR slot to inject after the PIN; <0 = don't
}

func (s *pinSession) writeServer(b []byte) error {
	s.upMu.Lock()
	defer s.upMu.Unlock()
	_, err := s.server.Write(b)
	return err
}

// captureAID learns the account ID from the first CH_ENTER (0x0065) the client
// sends: 65 00 | AID(u32 LE) | ...  Needed to validate 0x08b9 and build 0x08b8.
func (s *pinSession) captureAID(b []byte) {
	if s.haveAID || len(b) < 6 {
		return
	}
	if b[0] == 0x65 && b[1] == 0x00 {
		s.aid = binary.LittleEndian.Uint32(b[2:6])
		s.haveAID = true
		slog.Info("PIN proxy: captured AID", "session", s.id, "aid", s.aid)
	}
}

// findPin scans server→client bytes for a valid 0x08b9 record (b9 08 | seed |
// AID | state) and returns its seed + state. Validates the embedded AID against
// the session's to avoid false positives from 0xb9 0x08 inside other packet data.
func (s *pinSession) findPin(buf []byte) (seed uint32, state uint16, ok bool) {
	for i := 0; i+12 <= len(buf); i++ {
		if buf[i] != 0xb9 || buf[i+1] != 0x08 {
			continue
		}
		rec := buf[i : i+12]
		aid := binary.LittleEndian.Uint32(rec[6:10])
		if s.haveAID && aid != s.aid {
			continue
		}
		return binary.LittleEndian.Uint32(rec[2:6]), binary.LittleEndian.Uint16(rec[10:12]), true
	}
	return 0, 0, false
}

// clientToServer relays C→S and snoops CH_ENTER for the AID.
func (s *pinSession) clientToServer() {
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

// serverToClient relays S→C and drives the injection state machine: inject the
// PIN ack on 0x08b9 state=1, then (if selectSlot >= 0) the char-select on the
// following 0x08b9 state=0.
func (s *pinSession) serverToClient(inject bool) {
	buf := make([]byte, 32*1024)
	var pend []byte // small rolling window to catch a record split across reads
	for {
		n, err := s.server.Read(buf)
		if n > 0 {
			if _, werr := s.client.Write(buf[:n]); werr != nil {
				break
			}
			if inject && s.step != ppStepDone && s.dstPort == s.charPort {
				pend = append(pend, buf[:n]...)
				if seed, state, ok := s.findPin(pend); ok {
					s.advance(seed, state)
					pend = nil // consume; look fresh for the next record
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

// advance reacts to a 0x08b9 record according to the current step.
func (s *pinSession) advance(seed uint32, state uint16) {
	switch s.step {
	case ppStepWantPIN:
		if state != 1 { // not the "enter PIN" prompt yet
			return
		}
		if !s.haveAID {
			slog.Warn("PIN proxy: PIN prompt but AID unknown; skipping", "session", s.id, "seed", seed)
			return
		}
		slots, err := ppSolveSlots(seed, s.pin)
		if err != nil {
			slog.Error("PIN proxy: cannot solve PIN", "session", s.id, "error", err)
			s.step = ppStepDone
			return
		}
		pkt := ppBuildPinAck(s.aid, slots)
		if werr := s.writeServer(pkt); werr != nil {
			slog.Error("PIN proxy: failed to inject 0x08b8", "session", s.id, "error", werr)
			s.step = ppStepDone
			return
		}
		slog.Info("PIN proxy: PIN injected", "session", s.id, "seed", seed, "slots", slots)
		if s.selectSlot < 0 {
			s.step = ppStepDone
		} else {
			s.step = ppStepWantSelect
		}

	case ppStepWantSelect:
		switch state {
		case 0: // PIN accepted → select the character
			pkt := ppBuildCharSelect(s.selectSlot)
			if werr := s.writeServer(pkt); werr != nil {
				slog.Error("PIN proxy: failed to inject 0x0066", "session", s.id, "error", werr)
			} else {
				slog.Info("PIN proxy: PIN accepted, char-select injected", "session", s.id, "slot", s.selectSlot)
			}
			s.step = ppStepDone
		case 1: // still prompting — ignore (likely a retransmit)
		default: // 8 = wrong, etc.
			slog.Warn("PIN proxy: PIN not accepted; not selecting char", "session", s.id, "state", state)
			s.step = ppStepDone
		}
	}
}

// ---- transparent-proxy plumbing ---------------------------------------------

// ppOrigDst recovers the pre-REDIRECT destination via SO_ORIGINAL_DST.
func ppOrigDst(c *net.TCPConn) (netip.AddrPort, error) {
	sc, err := c.SyscallConn()
	if err != nil {
		return netip.AddrPort{}, err
	}
	var mreq *unix.IPv6Mreq
	var ge error
	cerr := sc.Control(func(fd uintptr) {
		mreq, ge = unix.GetsockoptIPv6Mreq(int(fd), unix.SOL_IP, ppSoOriginalDst)
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

// ppDial connects to the upstream char server with the proxy's fwmark set on
// the socket, so the REDIRECT rule's `-m mark ... RETURN` skips it (no loop).
func ppDial(target string) (net.Conn, error) {
	d := net.Dialer{
		Control: func(_, _ string, c syscall.RawConn) error {
			var serr error
			if cerr := c.Control(func(fd uintptr) {
				serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_MARK, ppFwMark)
			}); cerr != nil {
				return cerr
			}
			return serr
		},
	}
	return d.Dial("tcp", target)
}

func ppHandle(id int, client *net.TCPConn, pin string, charPort uint16, inject bool, selectSlot int) {
	dst, err := ppOrigDst(client)
	if err != nil {
		slog.Warn("PIN proxy: cannot determine upstream (SO_ORIGINAL_DST); closing", "session", id, "error", err)
		client.Close()
		return
	}
	target := dst.String()
	server, err := ppDial(target)
	if err != nil {
		slog.Warn("PIN proxy: dial upstream failed", "session", id, "target", target, "error", err)
		client.Close()
		return
	}
	slog.Info("PIN proxy: new connection", "session", id, "from", client.RemoteAddr().String(), "to", target)

	s := &pinSession{
		id: id, client: client, server: server,
		pin: pin, charPort: charPort, dstPort: dst.Port(),
		step: ppStepWantPIN, selectSlot: selectSlot,
	}
	go s.clientToServer()
	s.serverToClient(inject)
}

// ---- iptables management -----------------------------------------------------

func ppIptables(args ...string) error {
	// -w makes iptables wait for the xtables lock instead of failing with
	// "Resource temporarily unavailable". The PIN proxy and zone proxy install
	// their REDIRECT rules from concurrent goroutines (see startBackgroundJobs),
	// and a restart can briefly overlap an old process's rule teardown, so the
	// lock is genuinely contended; -w (bounded) serializes rather than races.
	full := append([]string{"-w", "5"}, args...)
	out, err := exec.Command("iptables", full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("iptables %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ppRuleBodies returns the match+target portions of the two NAT OUTPUT rules
// (the chain verb -A/-D is prepended by the caller). Order matters: the RETURN
// for our marked onward traffic must precede the REDIRECT.
func ppRuleBodies(charPort, listenPort, serverIP string) [][]string {
	mark := []string{"-p", "tcp", "--dport", charPort,
		"-m", "mark", "--mark", fmt.Sprintf("0x%x", ppFwMark), "-j", "RETURN"}
	redir := []string{"-p", "tcp"}
	if serverIP != "" {
		redir = append(redir, "-d", serverIP)
	}
	redir = append(redir, "--dport", charPort, "-j", "REDIRECT", "--to-ports", listenPort)
	return [][]string{mark, redir}
}

func ppInstallRules(charPort, listenPort, serverIP string) error {
	bodies := ppRuleBodies(charPort, listenPort, serverIP)
	// Clear any stale copies left by a previous hard crash (best-effort; a
	// failing -D just means the rule wasn't there).
	for _, b := range bodies {
		for range 5 {
			if err := ppIptables(append([]string{"-t", "nat", "-D", "OUTPUT"}, b...)...); err != nil {
				break
			}
		}
	}
	for i, b := range bodies {
		if err := ppIptables(append([]string{"-t", "nat", "-A", "OUTPUT"}, b...)...); err != nil {
			// Roll back any rule already added before reporting failure.
			for _, done := range bodies[:i] {
				_ = ppIptables(append([]string{"-t", "nat", "-D", "OUTPUT"}, done...)...)
			}
			return err
		}
	}
	return nil
}

func ppRemoveRules(charPort, listenPort, serverIP string) {
	for _, b := range ppRuleBodies(charPort, listenPort, serverIP) {
		if err := ppIptables(append([]string{"-t", "nat", "-D", "OUTPUT"}, b...)...); err != nil {
			slog.Warn("PIN proxy: failed to remove iptables rule (may need manual cleanup)", "error", err)
		}
	}
	slog.Info("PIN proxy: iptables rules removed")
}

// ---- service entrypoint ------------------------------------------------------

// startPinProxy installs the char-port REDIRECT, runs the injection proxy, and
// blocks until ctx is cancelled, at which point it removes the rules. It is a
// no-op unless PIN_PROXY is enabled (the caller already checks, but we guard
// here too).
func startPinProxy(ctx context.Context) {
	cfg := appConfig
	if cfg == nil || !cfg.PinProxyEnabled {
		return
	}

	charPort := cfg.PinProxyCharPort
	listenPort := cfg.PinProxyListenPort
	serverIP := cfg.PinProxyServerIP
	inject := cfg.PinProxyInject
	pin := cfg.PinProxyPIN
	selectSlot := cfg.PinProxySelectSlot

	charPortNum, err := strconv.ParseUint(charPort, 10, 16)
	if err != nil {
		slog.Error("PIN proxy: invalid char port; not starting", "value", charPort, "error", err)
		return
	}
	if inject && pin == "" {
		// Injection is the whole point; without a PIN, don't install any
		// iptables rules — just skip the proxy so chat capture proceeds.
		slog.Error("PIN proxy: enabled with injection but no PIN; not starting",
			"hint", "set YUFA_PIN, or PIN_PROXY_INJECT=0 for a relay-only test")
		return
	}

	if err := ppInstallRules(charPort, listenPort, serverIP); err != nil {
		slog.Error("PIN proxy: failed to install iptables REDIRECT; not started", "error", err,
			"hint", "needs CAP_NET_ADMIN — run: sudo setcap cap_net_raw,cap_net_admin=eip <binary>")
		return
	}
	defer ppRemoveRules(charPort, listenPort, serverIP)

	listenAddr := net.JoinHostPort("127.0.0.1", listenPort)
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		slog.Error("PIN proxy: listen failed", "addr", listenAddr, "error", err)
		return
	}
	slog.Info("PIN proxy listening", "addr", listenAddr, "inject", inject,
		"charPort", charPort, "selectSlot", selectSlot, "serverIP", orAny(serverIP))

	// Close the listener on shutdown so Accept unblocks.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	id := 0
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				slog.Info("PIN proxy shutting down")
				return
			default:
				slog.Warn("PIN proxy accept error", "error", err)
				continue
			}
		}
		id++
		go ppHandle(id, c.(*net.TCPConn), pin, uint16(charPortNum), inject, selectSlot)
	}
}

func orAny(s string) string {
	if s == "" {
		return "<any>"
	}
	return s
}
