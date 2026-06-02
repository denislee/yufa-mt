package server

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
)

// useProxyIPC repoints the injection/readiness calls at the IPC client for the
// given socket path. Called from Run when starting in ModeApp.
func useProxyIPC(socketPath string) {
	c := newProxyIPCClient(socketPath)
	injectChatCommand = c.inject
	zoneProxyReady = c.ready
}
