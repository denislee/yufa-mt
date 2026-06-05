//go:build linux

// zoneproxy.go — an in-process transparent TCP proxy on the ZONE/map server
// port, the sibling of pinproxy.go (which proxies the CHAR port). It exists so
// the app can INJECT chat/atcommand packets into the live, authenticated game
// connection — specifically `@mobinfo <id>` — to scrape the running server's
// (modified) monster database. See mobscrape.go for the sweep driver and
// mobinfo_parser.go for parsing the replies.
//
// Why a proxy and not raw packet injection: the zone connection is an
// authenticated TCP stream; injecting a raw packet would desync seq/ack. By
// terminating both sides (exactly like pinproxy) we get a clean write handle to
// the server and TCP stays consistent. The injected 0x00f3 bypasses the
// client's own send() so a client-side anti-cheat never sees an anomalous
// outbound packet (same reasoning as the PIN injection).
//
// On this server the zone protocol is PLAINTEXT (the passive chat capture reads
// it by literal packet-id prefix, which only works without obfuscation), so the
// chat packet is hand-buildable:
//
//	CZ_REQUEST_CHAT (0x00f3): f3 00 | len(u16 LE) | "<charname> : <message>"
//
// len is the total packet length (4-byte header + text); there is NO trailing
// null on this client (confirmed from a live capture). The server validates the
// "<charname>" prefix against the sender's character, so we learn the real name
// by snooping the client's own outgoing 0x00f3 packets (falling back to the
// configured ZoneProxyCharName).
//
// Reuses pinproxy.go's netfilter helpers (ppInstallRules / ppRemoveRules /
// ppDial / ppOrigDst / ppFwMark / orAny) — same package, same linux build tag.
package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// zoneSession is one live client<->zone connection flowing through the proxy.
type zoneSession struct {
	id     int
	client net.Conn
	server net.Conn
	upMu   sync.Mutex // serializes writes to the server (relay + injection)
}

func (s *zoneSession) writeServer(b []byte) error {
	s.upMu.Lock()
	defer s.upMu.Unlock()
	_, err := s.server.Write(b)
	return err
}

var (
	// activeZone is the most recent live zone session; injection targets it.
	activeZone atomic.Pointer[zoneSession]

	// Zone proxy diagnostics — surfaced by zoneProxyDiag() so a "zone proxy not
	// ready" failure names its concrete cause instead of a bare active=false.
	// These distinguish "relay never started" (ZONE_PROXY=0 / iptables install
	// failed) from "relay up, but no client connection was ever redirected into
	// it" (wrong ZONE_PROXY_PORT / ZONE_PROXY_SERVER_IP, or the client is off-box).
	zoneProxyListening     atomic.Bool            // REDIRECT installed + listener accepting
	zoneProxyEverConnected atomic.Bool            // any client session relayed since startup
	zoneProxyLastTarget    atomic.Pointer[string] // upstream addr ("ip:port") of the most recent session
	lastZoneDiagLog        atomic.Int64           // unix-nanos throttle for logZoneProxyDiag

	zoneNameMu   sync.RWMutex
	zoneCharName string // learned from the client's own outgoing chat
)

func setZoneCharName(n string) {
	zoneNameMu.Lock()
	zoneCharName = n
	zoneNameMu.Unlock()
}

func learnedZoneCharName() string {
	zoneNameMu.RLock()
	defer zoneNameMu.RUnlock()
	return zoneCharName
}

// effectiveCharName returns the learned name, falling back to the configured one.
func effectiveCharName() string {
	if n := learnedZoneCharName(); n != "" {
		return n
	}
	if appConfig != nil {
		return appConfig.ZoneProxyCharName
	}
	return ""
}

// parseOutgoingChatName scans a client→server buffer for a CZ_REQUEST_CHAT
// (0x00f3) packet and extracts the "<charname>" prefix before " : ". Returns ""
// if none is found.
func parseOutgoingChatName(payload []byte) string {
	for i := 0; i+4 <= len(payload); i++ {
		if payload[i] != 0xf3 || payload[i+1] != 0x00 {
			continue
		}
		length := int(binary.LittleEndian.Uint16(payload[i+2 : i+4]))
		if length < 5 || i+length > len(payload) {
			continue
		}
		msgBytes := payload[i+4 : i+length]
		utf8Bytes, _, err := transform.Bytes(charmap.ISO8859_1.NewDecoder(), msgBytes)
		if err != nil {
			utf8Bytes = msgBytes
		}
		s := strings.TrimRight(string(utf8Bytes), "\x00")
		if name, _, ok := strings.Cut(s, " : "); ok {
			if name = strings.TrimSpace(name); name != "" {
				return name
			}
		}
	}
	return ""
}

// buildChatPacket frames a CZ_REQUEST_CHAT carrying "<name> : <msg>" in Latin-1.
func buildChatPacket(name, msg string) ([]byte, error) {
	text := name + " : " + msg
	enc, _, err := transform.Bytes(charmap.ISO8859_1.NewEncoder(), []byte(text))
	if err != nil {
		enc = []byte(text)
	}
	total := len(enc) + 4 // 2 (id) + 2 (len) + text
	if total > 0xffff {
		return nil, fmt.Errorf("chat message too long (%d bytes)", total)
	}
	out := []byte{0xf3, 0x00}
	out = binary.LittleEndian.AppendUint16(out, uint16(total))
	return append(out, enc...), nil
}

// injectChatCommandLocal sends one chat/atcommand line over the live zone
// session held in THIS process. It is the implementation used in ModeAll (and
// inside the proxy process itself); ModeApp instead routes through the IPC
// client (see proxy_inject.go / proxy_ipc_client.go).
func injectChatCommandLocal(msg string) error {
	s := activeZone.Load()
	if s == nil {
		return fmt.Errorf("no active zone connection (client logged in? ZONE_PROXY enabled?)")
	}
	name := effectiveCharName()
	if name == "" {
		return fmt.Errorf("character name unknown; send any in-game chat once, or set ZONE_PROXY_CHAR_NAME")
	}
	pkt, err := buildChatPacket(name, msg)
	if err != nil {
		return err
	}
	return s.writeServer(pkt)
}

// buildUserCountRequest frames a CZ_REQ_USER_COUNT (0x00c1) — the packet the
// client emits for the player-facing "/who" command. It is a bare 2-byte header
// with no payload; the server answers with ZC_USER_COUNT (0x00c2) carrying the
// online total (see scanUserCountPacket in players_ingame.go). Unlike @users it
// needs no GM privilege, which is why it is the preferred in-game count source.
func buildUserCountRequest() []byte {
	return []byte{0xc1, 0x00}
}

// injectRawPacketLocal writes a pre-framed packet straight to the live zone
// server connection held in THIS process — the raw-bytes sibling of
// injectChatCommandLocal (which frames a chat string first). It exists for
// fixed binary requests like CZ_REQ_USER_COUNT that are not chat text and so
// carry no "<charname>" prefix. ModeApp routes through the IPC client.
func injectRawPacketLocal(pkt []byte) error {
	if len(pkt) == 0 {
		return fmt.Errorf("refusing to inject empty packet")
	}
	s := activeZone.Load()
	if s == nil {
		return fmt.Errorf("no active zone connection (client logged in? ZONE_PROXY enabled?)")
	}
	return s.writeServer(pkt)
}

// zoneProxyReadyLocal reports whether a session is live in THIS process and
// which char name would be used for injection. ModeAll/proxy use it directly;
// ModeApp routes through the IPC client (see proxy_inject.go). When not ready it
// logs a throttled diagnostic naming the concrete cause, so a failed
// players-ingame / @mobinfo run leaves a breadcrumb in the proxy logs.
func zoneProxyReadyLocal() (bool, string) {
	ready := activeZone.Load() != nil
	name := effectiveCharName()
	if !ready {
		logZoneProxyDiag()
	}
	return ready, name
}

// logZoneProxyDiag emits a one-line summary of the relay's state at WARN, gated
// to once per minute. Readiness is polled roughly that often (the players-ingame
// job + the admin status page), so without the throttle a persistently-down
// relay would flood the log with an identical line every poll.
func logZoneProxyDiag() {
	now := time.Now().UnixNano()
	last := lastZoneDiagLog.Load()
	if last != 0 && now-last < int64(time.Minute) {
		return
	}
	if !lastZoneDiagLog.CompareAndSwap(last, now) {
		return // another goroutine just logged it
	}
	slog.Warn("zone proxy not ready", "diag", zoneProxyDiag())
}

// zoneProxyDiag renders the relay's diagnostic state as a single line, ending in
// a "CAUSE:"/"LIKELY CAUSE:" hint so the operator does not have to interpret the
// raw flags. It reads only atomics + config, so it is cheap and lock-free.
func zoneProxyDiag() string {
	cfg := appConfig
	var enabled bool
	var zonePort, serverIP, chatPort string
	if cfg != nil {
		enabled = cfg.ZoneProxyEnabled
		zonePort = cfg.ZoneProxyZonePort
		serverIP = orAny(cfg.ZoneProxyServerIP)
		chatPort = cfg.ChatCapturePort
	}
	last := "(none yet)"
	if p := zoneProxyLastTarget.Load(); p != nil {
		last = *p
	}
	return fmt.Sprintf("enabled=%v listening=%v active=%v everConnected=%v lastTarget=%s zonePort=%s serverIP=%s chatCapturePort=%s charName=%q%s",
		enabled, zoneProxyListening.Load(), activeZone.Load() != nil,
		zoneProxyEverConnected.Load(), last, zonePort, serverIP, chatPort,
		effectiveCharName(), zoneProxyCauseHint(enabled, chatPort, zonePort))
}

// zoneProxyCauseHint maps the diagnostic state to the single most likely reason
// the relay has no live session, in the order the failures cascade.
func zoneProxyCauseHint(enabled bool, chatPort, zonePort string) string {
	switch {
	case !enabled:
		return " — CAUSE: relay disabled (ZONE_PROXY=0); set ZONE_PROXY=1 on the proxy process and re-log the client"
	case !zoneProxyListening.Load():
		return " — CAUSE: relay not listening; look earlier for an iptables REDIRECT install failure (needs CAP_NET_ADMIN)"
	case !zoneProxyEverConnected.Load():
		if chatPort != "" && zonePort != "" && chatPort != zonePort {
			return fmt.Sprintf(" — LIKELY CAUSE: no connection ever redirected AND ZONE_PROXY_PORT (%s) != CHAT_CAPTURE_PORT (%s); the REDIRECT is probably matching the wrong port", zonePort, chatPort)
		}
		return " — CAUSE: relay up but no client connection has been redirected into it; re-log the game client and verify ZONE_PROXY_PORT / ZONE_PROXY_SERVER_IP match the live map server"
	default:
		return " — CAUSE: a session was relayed earlier but is closed now (client disconnected / changed maps); re-log or wait for the watchdog to reconnect"
	}
}

func (s *zoneSession) clientToServer() {
	buf := make([]byte, 32*1024)
	for {
		n, err := s.client.Read(buf)
		if n > 0 {
			if name := parseOutgoingChatName(buf[:n]); name != "" {
				setZoneCharName(name)
			}
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

func (s *zoneSession) serverToClient() {
	buf := make([]byte, 32*1024)
	for {
		n, err := s.server.Read(buf)
		if n > 0 {
			if _, werr := s.client.Write(buf[:n]); werr != nil {
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

func zpHandle(id int, client *net.TCPConn) {
	dst, err := ppOrigDst(client)
	if err != nil {
		slog.Warn("zone proxy: cannot determine upstream (SO_ORIGINAL_DST); closing", "session", id, "error", err)
		client.Close()
		return
	}
	target := dst.String()
	server, err := ppDial(target)
	if err != nil {
		slog.Warn("zone proxy: dial upstream failed", "session", id, "target", target, "error", err)
		client.Close()
		return
	}
	slog.Info("zone proxy: new connection", "session", id, "from", client.RemoteAddr().String(), "to", target)

	s := &zoneSession{id: id, client: client, server: server}
	activeZone.Store(s)
	zoneProxyEverConnected.Store(true)
	targetCopy := target // record what the client actually connected to (for diag)
	zoneProxyLastTarget.Store(&targetCopy)
	go s.clientToServer()
	s.serverToClient() // blocks until the connection ends

	activeZone.CompareAndSwap(s, nil) // clear only if still the current session
	slog.Info("zone proxy: connection closed", "session", id)
}

// startZoneProxy installs the zone-port REDIRECT, runs the relay/injection
// proxy, and blocks until ctx is cancelled. No-op unless ZoneProxyEnabled.
func startZoneProxy(ctx context.Context) {
	cfg := appConfig
	if cfg == nil || !cfg.ZoneProxyEnabled {
		slog.Warn("zone proxy disabled (ZONE_PROXY=0): in-game player count (/who, @users) and @mobinfo/@whereis injection are unavailable until enabled; passive chat capture is unaffected")
		return
	}

	zonePort := cfg.ZoneProxyZonePort
	listenPort := cfg.ZoneProxyListenPort
	serverIP := cfg.ZoneProxyServerIP

	if _, err := strconv.ParseUint(zonePort, 10, 16); err != nil {
		slog.Error("zone proxy: invalid zone port; not starting", "value", zonePort, "error", err)
		return
	}

	if err := ppInstallRules(zonePort, listenPort, serverIP); err != nil {
		slog.Error("zone proxy: failed to install iptables REDIRECT; not started", "error", err,
			"hint", "needs CAP_NET_ADMIN — run: sudo setcap cap_net_raw,cap_net_admin=eip <binary>")
		return
	}
	defer ppRemoveRules(zonePort, listenPort, serverIP)

	listenAddr := net.JoinHostPort("127.0.0.1", listenPort)
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		slog.Error("zone proxy: listen failed", "addr", listenAddr, "error", err)
		return
	}
	slog.Info("zone proxy listening", "addr", listenAddr, "zonePort", zonePort,
		"serverIP", orAny(serverIP), "listenPort", listenPort, "chatCapturePort", cfg.ChatCapturePort)
	// A relay that REDIRECTs a different port than the passive chat sniffer
	// watches is the classic "chat works but player count says not ready" trap:
	// the sniffer still sees chat while the relay never catches the connection.
	if cfg.ChatCapturePort != "" && cfg.ChatCapturePort != zonePort {
		slog.Warn("zone proxy: ZONE_PROXY_PORT differs from CHAT_CAPTURE_PORT — passive chat may capture while the relay's REDIRECT misses the connection, so in-game player count / injection will report 'not ready'",
			"zonePort", zonePort, "chatCapturePort", cfg.ChatCapturePort)
	}
	zoneProxyListening.Store(true)
	defer zoneProxyListening.Store(false)

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
				slog.Info("zone proxy shutting down")
				return
			default:
				slog.Warn("zone proxy accept error", "error", err)
				continue
			}
		}
		id++
		go zpHandle(id, c.(*net.TCPConn))
	}
}
