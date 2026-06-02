package server

// proxy_ipc.go — the IPC server run by the proxy process (ModeProxy). It lets
// the app process (ModeApp) ask the proxy to inject chat (@mobinfo …) and report
// whether a live zone session exists. Transport is newline-delimited JSON over a
// unix-domain socket (one request line → one response line). See
// docs/two-process-split-plan.md §5.
//
// CRITICAL INVARIANT (§4): handling an IPC request must never block the proxy's
// relay loops. The handlers below only call injectChatCommandLocal /
// zoneProxyReadyLocal, which touch the live session via its own upMu — the same
// brief lock relay writes already take — and never wait on the relay goroutines.

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"path/filepath"
)

// ipcProtocolVersion is the IPC contract version. Bumping it requires
// restarting the proxy unit (a deliberate, rare disconnect); the app rejects a
// mismatched proxy rather than sending malformed injects.
const ipcProtocolVersion = 1

// ipcRequest is one app→proxy command line.
type ipcRequest struct {
	V    int    `json:"v"`
	Op   string `json:"op"`             // "status" | "inject" | "logs"
	Msg  string `json:"msg,omitempty"`  // for "inject": the raw chat/atcommand text
	Tail int    `json:"tail,omitempty"` // for "logs": max lines to return (0 = all)
}

// ipcResponse is the proxy→app reply line.
type ipcResponse struct {
	OK       bool     `json:"ok"`
	Ready    bool     `json:"ready,omitempty"`    // for "status"
	CharName string   `json:"charName,omitempty"` // for "status"
	Lines    []string `json:"lines,omitempty"`    // for "logs": recent proxy log lines
	Err      string   `json:"err,omitempty"`
}

// startProxyIPCServer listens on the unix socket and serves inject/status
// requests until ctx is cancelled. It is started from the proxy run path.
func startProxyIPCServer(ctx context.Context, socketPath string) {
	if socketPath == "" {
		slog.Error("proxy IPC: empty socket path; not starting")
		return
	}
	// The systemd RuntimeDirectory normally creates the parent, but create it
	// best-effort so a non-systemd run (tests, manual) works too.
	if dir := filepath.Dir(socketPath); dir != "" {
		_ = os.MkdirAll(dir, 0o750)
	}
	// Remove a stale socket left by a previous hard crash; a fresh bind would
	// otherwise fail with "address already in use".
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("proxy IPC: could not remove stale socket", "path", socketPath, "error", err)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "unix", socketPath)
	if err != nil {
		slog.Error("proxy IPC: listen failed", "path", socketPath, "error", err)
		return
	}
	// App runs as the same user; 0660 keeps the socket off-limits to others.
	if err := os.Chmod(socketPath, 0o660); err != nil {
		slog.Warn("proxy IPC: chmod socket failed", "path", socketPath, "error", err)
	}
	slog.Info("proxy IPC server listening", "path", socketPath)

	go func() {
		<-ctx.Done()
		ln.Close()
		_ = os.Remove(socketPath)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				slog.Info("proxy IPC server shutting down")
				return
			default:
				slog.Warn("proxy IPC accept error", "error", err)
				continue
			}
		}
		go handleProxyIPCConn(conn)
	}
}

// handleProxyIPCConn serves request lines on one connection until it closes.
// The app dials per request, so this is usually one round-trip, but we loop to
// support a long-lived client too.
func handleProxyIPCConn(conn net.Conn) {
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 4096), 1<<20) // inject messages are short
	enc := json.NewEncoder(conn)
	for sc.Scan() {
		var req ipcRequest
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			_ = enc.Encode(ipcResponse{OK: false, Err: "bad request: " + err.Error()})
			continue
		}
		_ = enc.Encode(serveProxyIPCRequest(req))
	}
	if err := sc.Err(); err != nil {
		slog.Debug("proxy IPC: connection read ended", "error", err)
	}
}

// serveProxyIPCRequest dispatches one request to the local proxy state.
func serveProxyIPCRequest(req ipcRequest) ipcResponse {
	if req.V != ipcProtocolVersion {
		return ipcResponse{OK: false, Err: "unsupported IPC version"}
	}
	switch req.Op {
	case "status":
		ready, name := zoneProxyReadyLocal()
		return ipcResponse{OK: true, Ready: ready, CharName: name}
	case "inject":
		if err := injectChatCommandLocal(req.Msg); err != nil {
			return ipcResponse{OK: false, Err: err.Error()}
		}
		return ipcResponse{OK: true}
	case "logs":
		// Serve this process's own in-memory log ring (the proxy's stdout
		// capture) so the app's admin panel can show proxy-side logs.
		return ipcResponse{OK: true, Lines: logBuffer.Lines(req.Tail)}
	default:
		return ipcResponse{OK: false, Err: "unknown op: " + req.Op}
	}
}
