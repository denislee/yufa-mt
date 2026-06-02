package server

// proxy_ipc_client.go — the app-side (ModeApp) client for the proxy IPC server
// in proxy_ipc.go. It dials the unix socket per request: requests are rare
// (a paced @mobinfo sweep) and per-call dialing makes the client resilient to
// the proxy restarting underneath it — no stale long-lived connection to heal.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// proxyIPCClient talks to the proxy process over its unix socket.
type proxyIPCClient struct {
	socketPath string
}

func newProxyIPCClient(socketPath string) *proxyIPCClient {
	return &proxyIPCClient{socketPath: socketPath}
}

// roundTrip dials, sends one request line, and reads one response line.
func (c *proxyIPCClient) roundTrip(req ipcRequest) (ipcResponse, error) {
	req.V = ipcProtocolVersion
	conn, err := net.DialTimeout("unix", c.socketPath, 3*time.Second)
	if err != nil {
		return ipcResponse{}, fmt.Errorf("proxy IPC dial %s: %w (is the proxy unit running?)", c.socketPath, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	line, err := json.Marshal(req)
	if err != nil {
		return ipcResponse{}, err
	}
	if _, err := conn.Write(append(line, '\n')); err != nil {
		return ipcResponse{}, fmt.Errorf("proxy IPC write: %w", err)
	}

	sc := bufio.NewScanner(conn)
	// Responses are usually tiny, but a "logs" reply carries up to the full
	// 2000-line ring as one JSON line — allow up to 8 MiB.
	sc.Buffer(make([]byte, 0, 4096), 8<<20)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return ipcResponse{}, fmt.Errorf("proxy IPC read: %w", err)
		}
		return ipcResponse{}, fmt.Errorf("proxy IPC: no response (connection closed)")
	}
	var resp ipcResponse
	if err := json.Unmarshal(sc.Bytes(), &resp); err != nil {
		return ipcResponse{}, fmt.Errorf("proxy IPC: bad response: %w", err)
	}
	return resp, nil
}

// inject forwards a chat/atcommand line to the proxy for injection. Matches the
// injectChatCommand signature (see proxy_inject.go).
func (c *proxyIPCClient) inject(msg string) error {
	resp, err := c.roundTrip(ipcRequest{Op: "inject", Msg: msg})
	if err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("proxy refused inject: %s", resp.Err)
	}
	return nil
}

// ready asks the proxy whether a live zone session exists and the char name it
// would use. On any IPC error it reports not-ready (the caller logs and skips),
// matching the zoneProxyReady signature.
func (c *proxyIPCClient) ready() (bool, string) {
	resp, err := c.roundTrip(ipcRequest{Op: "status"})
	if err != nil || !resp.OK {
		return false, ""
	}
	return resp.Ready, resp.CharName
}

// proxyLogs fetches the proxy process's most recent log lines (its stdout ring
// buffer) so the app's admin panel can display proxy-side logs.
func (c *proxyIPCClient) proxyLogs(tail int) ([]string, error) {
	resp, err := c.roundTrip(ipcRequest{Op: "logs", Tail: tail})
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("proxy refused logs: %s", resp.Err)
	}
	return resp.Lines, nil
}
