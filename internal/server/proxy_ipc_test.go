package server

import (
	"bufio"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// waitForSocket polls until the IPC server's socket is dialable or the deadline
// passes, so the test doesn't race the listener coming up.
func waitForSocket(t *testing.T, c *proxyIPCClient) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.roundTrip(ipcRequest{Op: "status"}); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("IPC server socket never became dialable")
}

func TestProxyIPCRoundTrip(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "proxy.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go startProxyIPCServer(ctx, sock)

	c := newProxyIPCClient(sock)
	waitForSocket(t, c)

	// status: no zone session is active in the test process, so ready must be
	// false but the round-trip itself must succeed (OK path, no transport err).
	ready, name := c.ready()
	if ready {
		t.Errorf("ready() = true; want false with no active zone session")
	}
	if name != "" {
		t.Errorf("ready() name = %q; want empty", name)
	}

	// inject with no active session must surface a refusal (OK=false) as an
	// error, not a transport failure.
	if err := c.inject("@mobinfo 1002"); err == nil {
		t.Error("inject() with no active session = nil error; want a refusal")
	}
}

func TestServeProxyIPCRequestVersionAndOp(t *testing.T) {
	// Wrong protocol version is rejected.
	if resp := serveProxyIPCRequest(ipcRequest{V: ipcProtocolVersion + 1, Op: "status"}); resp.OK {
		t.Error("mismatched version accepted; want rejected")
	}
	// Unknown op is rejected.
	if resp := serveProxyIPCRequest(ipcRequest{V: ipcProtocolVersion, Op: "bogus"}); resp.OK {
		t.Error("unknown op accepted; want rejected")
	}
	// status is served (OK true) even with no session.
	if resp := serveProxyIPCRequest(ipcRequest{V: ipcProtocolVersion, Op: "status"}); !resp.OK {
		t.Errorf("status not served: %s", resp.Err)
	}
}

// TestUseProxyIPCRoutesOverWire proves that ModeApp wiring (useProxyIPC) actually
// repoints injectChatCommand/zoneProxyReady at the IPC client: a fake server that
// replies OK makes injectChatCommand succeed, whereas the in-process *Local impl
// would fail with "no active zone session". So a nil error here can ONLY come
// from the wire path.
func TestUseProxyIPCRoutesOverWire(t *testing.T) {
	// useProxyIPC mutates package globals; restore them so other tests see locals.
	origInject, origReady := injectChatCommand, zoneProxyReady
	t.Cleanup(func() { injectChatCommand, zoneProxyReady = origInject, origReady })

	sock := filepath.Join(t.TempDir(), "fake.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				sc := bufio.NewScanner(conn)
				for sc.Scan() {
					// Reply success for inject, ready+name for status.
					_, _ = conn.Write([]byte(`{"ok":true,"ready":true,"charName":"fakehero"}` + "\n"))
				}
			}()
		}
	}()

	useProxyIPC(sock)

	if err := injectChatCommand("@mobinfo 1002"); err != nil {
		t.Errorf("injectChatCommand over IPC = %v; want nil (proves wire routing)", err)
	}
	ready, name := zoneProxyReady()
	if !ready || name != "fakehero" {
		t.Errorf("zoneProxyReady over IPC = (%v, %q); want (true, \"fakehero\")", ready, name)
	}
}

// TestProxyIPCClientDialError confirms the client reports a clear error (not a
// hang) when no proxy is listening — the app-mode "proxy unit down" path.
func TestProxyIPCClientDialError(t *testing.T) {
	c := newProxyIPCClient(filepath.Join(t.TempDir(), "nonexistent.sock"))
	if err := c.inject("@mobinfo 1"); err == nil {
		t.Error("inject() to missing socket = nil; want dial error")
	}
	if ready, _ := c.ready(); ready {
		t.Error("ready() to missing socket = true; want false")
	}
}
