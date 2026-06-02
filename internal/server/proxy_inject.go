package server

import "fmt"

// proxy_inject.go wires the two cross-process calls — chat injection and
// proxy-readiness — behind package-level function variables so the same call
// sites (mobscrape.go, admin_scheduler.go) work in every --mode:
//
//   - ModeAll / ModeProxy: the vars point at the in-process implementations
//     (injectChatCommandLocal / zoneProxyReadyLocal in zoneproxy.go), which
//     act on the live zone session held by THIS process.
//   - ModeApp: useProxyIPC() repoints them at the IPC client, which forwards
//     the request to the proxy process over the unix socket.
//
// The vars are assigned once at startup (before any background goroutine
// starts), so no synchronization is needed on the variables themselves.
var (
	injectChatCommand = injectChatCommandLocal
	zoneProxyReady    = zoneProxyReadyLocal

	// fetchProxyLogs returns the proxy's recent log lines. By default (ModeAll /
	// ModeProxy) the proxy logs live in THIS process, so it reads the local ring
	// buffer. In ModeApp useProxyIPC repoints it at the IPC client.
	fetchProxyLogs = func(tail int) ([]string, error) { return logBuffer.Lines(tail), nil }

	// proxyLogsRemote is true only in ModeApp, where the proxy runs in a separate
	// process. In ModeAll/Proxy the "app" and "proxy" log streams are the same
	// single buffer, so the admin panel must not merge/duplicate them.
	proxyLogsRemote = false

	// requestProxyRestart asks the proxy process to restart (for the admin
	// "Update Proxy" button). Only meaningful in ModeApp; the default errors so
	// the single-process modes can't misfire it.
	requestProxyRestart = func() error {
		return fmt.Errorf("proxy restart only applies in the split deployment (app mode)")
	}
)

// useProxyIPC repoints the injection/readiness/logs calls at the IPC client for
// the given socket path. Called from Run when starting in ModeApp.
func useProxyIPC(socketPath string) {
	c := newProxyIPCClient(socketPath)
	injectChatCommand = c.inject
	zoneProxyReady = c.ready
	fetchProxyLogs = c.proxyLogs
	proxyLogsRemote = true
	requestProxyRestart = c.restart
}
