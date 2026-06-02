# Two-Process Split: zero-disconnect deploys

**Status:** 🟢 Implemented & locally verified (Phases 0–2 + UI) — **Phase 3 (live x230 verify) pending**
**Owner:** Denis Lee
**Created:** 2026-06-02
**Last updated:** 2026-06-02
**Goal:** Let routine deploys (web UI, scrapers, chat parsing, Discord) restart
**without dropping the live game-client connection**, by moving the connection-owning
proxies into a separate long-lived process.

> **Handoff note for a fresh Claude instance:** read this whole file first, then the
> "Current architecture" and "Target architecture" sections against the live code. Update
> the **Task checklist** statuses and the **Changelog** at the bottom as you make progress.
> Don't trust line numbers blindly — re-grep, code drifts.

---

## 1. Why

Today everything runs in **one process / one systemd unit** (`deploy/yufa-mt.service`,
`Restart=always`). The "Pull, Rebuild & Restart" button (`internal/server/selfupdate.go`)
does `git pull` → `go build` → swap binary → **SIGTERM self**. systemd respawns onto the
new binary.

The same process owns the **live game TCP connection**: the zone proxy
(`internal/server/zoneproxy.go`) and PIN proxy (`internal/server/pinproxy.go`) terminate
both sides of the client↔server connection (via iptables `REDIRECT`). So **every deploy
kills the game connection.** The listener watchdog (`scripts/listener/yufa-listener.sh`)
auto-recovers it, but only after a 90–120 s grace window → ~2–3 min of bot downtime per
deploy.

**Insight:** the only code that *must not* restart is the part holding the sockets
(relay + PIN injection + iptables). Everything we actually churn on (web, scrapers, chat
parsing, Discord) touches no socket. Split along that line and ~all deploys become
zero-disconnect.

### Alternative considered & rejected
**FD hand-off / graceful restart** (pass live socket FDs to the new process via
`SCM_RIGHTS`, nginx/tableflip style). Rejected because: (a) must also transfer the
per-session injection state machine, (b) gives no failure isolation — a bug in the new
binary still rides the live connection, (c) still restarts everything each deploy. The
split matches our actual change pattern (churn is in the app, not the proxy).

---

## 2. Current architecture (as of 2026-06-02)

Single process, started by `cmd/server/main.go` → `server.Run(cfg)`
(`internal/server/server.go:175`). Background jobs launched in
`startBackgroundJobs` (`internal/server/scraper_jobs.go:14`):

| Component | File | Owns a game socket? | Needs caps | Churns often? |
|---|---|---|---|---|
| PIN proxy (char port 7121) | `pinproxy.go` | **YES** (relay+inject) | CAP_NET_ADMIN (iptables, SO_MARK) | no — stable |
| Zone proxy (zone port 6121) | `zoneproxy.go` | **YES** (relay+inject) | CAP_NET_ADMIN | no — stable |
| Chat capture (libpcap sniff) | `scraper_chat.go:149` | **NO** — passive pcap on the wire | CAP_NET_RAW | yes (parsers) |
| Scrapers / scheduler | `scheduler.go`, `mobscrape.go`, … | no | none | yes |
| Web UI / admin | `server.go`, `web/` | no | none | yes |
| Discord bot, visitor logger | — | no | none | yes |
| Self-update | `selfupdate.go` | no | none | — |

### Coupling between "proxy world" and "app world" (the entire IPC surface)
Only **two** call sites cross the boundary, both app→proxy:

1. `injectChatCommand(msg string) error` — `mobscrape.go:127` sends `@mobinfo <id>`.
   Impl: `zoneproxy.go:138` writes a framed `CZ_REQUEST_CHAT (0x00f3)` to the active
   zone session's server socket.
2. `zoneProxyReady() (bool, string)` — `mobscrape.go:80`, `admin_scheduler.go:132`.
   Impl: `zoneproxy.go:156` reports `(session live?, char name)`.

Supporting proxy-side state that must travel **with** the proxy (not the app):
- `activeZone atomic.Pointer[zoneSession]` (`zoneproxy.go:65`) — current live session.
- Char-name learning: `parseOutgoingChatName` + `setZoneCharName` (`zoneproxy.go:97,71`),
  fed from the relay loop `clientToServer` (`zoneproxy.go:165`). Fallback
  `ZoneProxyCharName` config.
- `buildChatPacket` (`zoneproxy.go:122`) — Latin-1 framing.

Non-Linux stubs: `zoneproxy_stub.go`, `pinproxy_stub.go`.

### Key fact that drives the design
**Chat capture is NOT in the relay path.** It opens its own libpcap handle
(`pcap.OpenLive`, `scraper_chat.go:192`) filtering `tcp port 6121` and sniffs the wire
passively. It works whether or not the proxy is relaying, and it only needs `CAP_NET_RAW`.
→ **Chat capture stays in the APP** (its parsers change often; we want it redeployable).
The app therefore keeps `CAP_NET_RAW`; only `CAP_NET_ADMIN` (iptables) moves out to the
proxy.

---

## 3. Target architecture

Two **processes from one binary**, selected by a `--mode` flag (no second build target).

```
                         ┌─────────────────────────────────────────┐
 game client ──REDIRECT──▶  yufa-mt --mode=proxy   (rarely restarts) │
                         │   • PIN proxy (char 7121) relay+inject    │
                         │   • zone proxy (zone 6121) relay+inject   │
                         │   • iptables REDIRECT install/teardown    │
                         │   • char-name learning, activeZone        │
                         │   • IPC server on unix socket             │
                         │   caps: CAP_NET_ADMIN                     │
                         └──────────────▲────────────────────────────┘
                                        │ unix socket
                                        │ /run/yufa-mt/proxy.sock
                                        │ (INJECT / STATUS)
                         ┌──────────────┴────────────────────────────┐
                         │  yufa-mt --mode=app   (redeploys freely)   │
                         │   • web UI / admin / self-update           │
                         │   • scrapers, scheduler, mobscrape driver  │
                         │   • chat capture (libpcap) + parsers       │
                         │   • Discord bot, visitor logger, DB        │
                         │   caps: CAP_NET_RAW (pcap)                 │
                         └────────────────────────────────────────────┘
```

**`--mode` values:**
- `proxy` — only the two proxies + iptables + IPC server. No DB, no web, no scrapers.
- `app` — everything else. `injectChatCommand`/`zoneProxyReady` become IPC client calls.
  No iptables, no relay.
- `all` (**default**) — current single-process behavior; in-process injection (no IPC).
  Keeps `make run` / dev / non-Linux unchanged.

**Deploy flow (new):** self-update rebuilds the one binary, then restarts **only the app
unit**. The proxy unit keeps executing its old inode (the `mv` over the binary doesn't
disturb a running process) → game connection survives.

### What still causes a disconnect (be honest)
- Changing the **proxy** code (relay/PIN/inject logic) or the **IPC protocol** → you must
  restart the proxy unit = one disconnect. These are rare. Treat the IPC protocol as a
  stable, versioned contract (see §5).

### Privilege change
- App: `CAP_NET_ADMIN` removed (no longer runs iptables); keeps `CAP_NET_RAW` for pcap.
- Proxy: `CAP_NET_ADMIN` only. (SO_MARK fwmark + iptables REDIRECT both need it; relay
  uses plain TCP. It does **not** need `CAP_NET_RAW`.)

---

## 4. THE critical invariant

> **The proxy's relay loops (`clientToServer`/`serverToClient`) must NEVER block on the
> app IPC.**

When the app is mid-restart, the IPC client disconnects. The proxy must keep relaying
game bytes perfectly and simply have nowhere to send `INJECT` results / no one asking for
`STATUS`. Implementation rules:
- IPC server accept/handle runs on its **own goroutines**, fully decoupled from relay.
- An `INJECT` request from a (re)connected app just calls the existing
  `activeZone.Load().writeServer(pkt)` — same as today, synchronous to the *IPC* goroutine,
  never touching the relay goroutine's control flow.
- If no app is connected, nothing happens to the game link. Mob sweeps just fail/retry
  (mobscrape already handles inject errors: `mobscrape.go:127`).
- No shared lock between relay and IPC except the already-existing `zoneSession.upMu`
  (`zoneproxy.go:53`), which serializes writes to the server socket — that's correct and
  must be kept (relay writes and injected writes both take it).

Getting this wrong = an app restart stalls the game socket, defeating the whole project.

---

## 5. IPC protocol (keep it tiny & versioned)

- **Transport:** unix domain socket, path `${RUNTIME_DIR}/proxy.sock`, default
  `/run/yufa-mt/proxy.sock` (the `RuntimeDirectory=yufa-mt` already exists for both units;
  see §7). Configurable via `PROXY_IPC_SOCKET`.
- **Roles:** proxy = server (long-lived, listens). app = client (reconnects each app
  start, with retry/backoff since the proxy may briefly be down too).
- **Framing:** newline-delimited JSON (one request per line, one response per line). Small,
  human-debuggable with `socat`/`nc`. No external dep.
- **Versioning:** every request carries `"v":1`. Proxy rejects unknown major version with
  an error response. Bumping `v` ⇒ a proxy deploy (one disconnect) — acceptable & rare.

**Requests (app→proxy):**
```jsonc
{"v":1,"op":"status"}
  → {"ok":true,"ready":true,"charName":"golbin"}
{"v":1,"op":"inject","msg":"@mobinfo 1002"}
  → {"ok":true}                       // proxy frames with learned/cfg char name & writes
  → {"ok":false,"err":"no active zone connection"}
```

**App-side abstraction:** introduce indirection so the same call sites work in every mode:
```go
// set once at startup based on --mode
var injectChatCommand func(string) error
var zoneProxyReady    func() (bool, string)
```
- mode `all`/`proxy`: bind to the local impls currently in `zoneproxy.go`.
- mode `app`: bind to IPC-client impls (dial socket, send op, parse reply).

Keep `buildChatPacket` + char-name learning **proxy-side**; the app sends only the raw
command text (`@mobinfo 1002`) — the proxy owns framing & name.

---

## 6. Code changes (by file)

- `cmd/server/main.go` — parse `--mode` (flag or `YUFA_MODE` env; default `all`). Dispatch
  to `server.RunApp` / `server.RunProxy` / `server.Run` (all).
- `internal/server/server.go` — refactor `Run` so the background-job set is mode-aware.
  Extract proxy-only startup vs app-only startup. `Run`(all) keeps current behavior.
- `internal/server/scraper_jobs.go` — split `startBackgroundJobs`: proxies only in
  proxy/all; scrapers+chat-capture+scheduler only in app/all.
- **New** `internal/server/proxy_ipc.go` (`//go:build linux`) — IPC server (proxy side):
  listen on unix socket, handle `status`/`inject` on their own goroutines, call existing
  `injectChatCommand`/`zoneProxyReady` locals.
- **New** `internal/server/proxy_ipc_client.go` — IPC client (app side): `injectChatCommandIPC`,
  `zoneProxyReadyIPC` with dial+retry.
- `internal/server/zoneproxy.go` — rename the existing package-level funcs to locals
  (e.g. `injectChatCommandLocal`) so the `var injectChatCommand` indirection can point at
  either. Or keep names and wire via a small interface. (Pick one; note it in Changelog.)
- `internal/server/zoneproxy_stub.go` / `pinproxy_stub.go` — keep non-Linux behavior;
  mode `all` off-Linux must still build (dev on mac).
- `internal/server/selfupdate.go` — on restart, SIGTERM self **only** (app is its own
  unit now, so this restarts just the app). Add a guard/log clarifying the proxy is left
  running. Optionally expose a separate "Restart proxy (will disconnect)" admin action.
- `internal/config/config.go` — add `Mode` (or read in main), `ProxyIPCSocket`
  (`PROXY_IPC_SOCKET`).
- `web/templates/admin.html` — relabel the update button / add note that it no longer
  disconnects the game; optionally surface proxy-unit status separately.

---

## 7. systemd: two units

Replace the single `deploy/yufa-mt.service` with two (keep old one for `all`-mode boxes if
any). Both share `WorkingDirectory`, `EnvironmentFile`, `RuntimeDirectory=yufa-mt` (so the
socket path + xtables lock are co-located and user-owned).

**`yufa-mt-proxy.service`** (rarely restarted):
- `ExecStart=/home/dns/git/yufa-mt/yufa-mt --mode=proxy`
- `AmbientCapabilities=CAP_NET_ADMIN`, `CapabilityBoundingSet=CAP_NET_ADMIN`
- `Environment=XTABLES_LOCKFILE=/run/yufa-mt/xtables.lock`
- `RuntimeDirectory=yufa-mt` (owns it; see ordering note)
- `Restart=always`, `RestartSec=3`

**`yufa-mt-app.service`** (redeployed by the button):
- `ExecStart=/home/dns/git/yufa-mt/yufa-mt --mode=app`
- `AmbientCapabilities=CAP_NET_RAW`, `CapabilityBoundingSet=CAP_NET_RAW`
- `After=yufa-mt-proxy.service`, `Wants=yufa-mt-proxy.service` (app needs the socket;
  but app must also **tolerate the proxy being absent** and retry — don't hard-`Requires`)
- `Restart=always`, `RestartSec=3`

**RuntimeDirectory ownership gotcha:** if both units declare `RuntimeDirectory=yufa-mt`,
systemd may remove `/run/yufa-mt` when *either* stops (`RuntimeDirectoryPreserve=yes` to
avoid). Pick one owner (proxy) with `RuntimeDirectoryPreserve=yes`, or give them distinct
dirs and put the socket somewhere both can reach. **Decide & document in Changelog.**

---

## 8. Task checklist

Legend: ☐ todo · ◐ in progress · ☑ done · ✗ dropped

### Phase 0 — scaffolding (no behavior change)
- ☑ Add `--mode` flag/env to `cmd/server/main.go`; default `all` dispatches to current `Run`.
      (`--mode` sets `YUFA_MODE` before `config.Load` so validation matches the role.)
- ☑ Add `ProxyIPCSocket` config + default (`/run/yufa-mt/proxy.sock`, `PROXY_IPC_SOCKET`);
      `Mode` field + `ModeAll/App/Proxy` consts; mode validation in `config.Load`.
- ☑ Make `startBackgroundJobs` mode-aware (proxies only in `ModeAll`; scrapers+chat in app/all).
- ☑ `go build -tags fts5 ./...` ✔, full `go test ./...` ✔, `go vet ./...` ✔. Default-mode boot
      verified identical (no proxy/IPC banner, serves `/health/zone`). ← gate PASSED

### Phase 1 — IPC layer
- ☑ Indirection vars (`injectChatCommand`, `zoneProxyReady`) in `proxy_inject.go`; locals
      renamed `*Local` in `zoneproxy.go` + `zoneproxy_stub.go`.
- ☑ `proxy_ipc.go`: newline-JSON unix-socket IPC server (status/inject), own goroutines,
      version gate, bounded buffers. **§4 honored** — handlers only call the `*Local` funcs.
- ☑ `proxy_ipc_client.go`: app-side client, dials per request (resilient to proxy restart),
      3s dial / 5s rw timeouts, graceful error → not-ready.
- ☑ `useProxyIPC()` repoints the vars in `ModeApp` (server.go); `all`/`proxy` keep locals.
- ☑ Tests (`proxy_ipc_test.go`): socket round-trip, version/op rejection, dial-error path —
      all green. Cross-process check via python over the real binary's socket — status,
      inject-refusal, bad-version, unknown-op, malformed-line all behave.

### Phase 2 — self-update & systemd
- ☑ `deploy/yufa-mt-proxy.service` (`--mode=proxy`, `CAP_NET_ADMIN`, owns `/run/yufa-mt`
      with `RuntimeDirectoryPreserve=yes`) + `deploy/yufa-mt-app.service` (`--mode=app`,
      `CAP_NET_RAW`, `Wants=`/`After=` proxy). RuntimeDirectory question resolved: **proxy
      owns it, preserve=yes; app does not declare it.** `systemd-analyze verify` clean
      (only the expected "binary not at /home/dns/git" note on this /tmp copy).
- ☑ `selfupdate.go`: doc + restart log now mode-aware (logs "app process only — proxy keeps
      the game connection up" in `ModeApp`). SIGTERM-self is correct for both unit layouts;
      no behavioral branch needed. `SELF_UPDATE=1` app boot verified.
- ☑ README: new "Split deployment" section — install both units, migrate off the single
      unit, rollback note.

### Phase 3 — verify on the x230 box  ⚠️ REQUIRES THE LIVE BOX (not doable from this repo copy)
- ☐ Copy units to `/etc/systemd/system/`, `daemon-reload`, `disable --now yufa-mt`,
      `enable --now yufa-mt-proxy yufa-mt-app`.
- ☐ Deploy proxy unit; confirm game client connects & PIN auto-enters (proxy alone).
- ☐ Deploy app unit; confirm web up, chat capture flowing, `@mobinfo` sweep injects via IPC
      (check proxy journal for "proxy IPC server listening" + an inject from the app).
- ☐ **Acceptance test:** with a client logged in & in-game, press "Pull, Rebuild & Restart".
      Confirm: app PID changes, **game connection stays up** (no watchdog relaunch, no
      char-name re-learn, `activeZone` unchanged, chat capture only briefly gaps). Watch
      `watch -n1 'ss -tnp | grep 6121'` and `/health/zone age` across the restart.
- ☐ Confirm repeated deploys don't accumulate iptables rules or leak sockets.
- ☐ Use the `verify-deploy` skill against x230 as the final gate.

### Phase 4 — polish
- ☑ Admin UI: update card now shows a green "split deployment — client NOT disconnected"
      banner in `ModeApp`, or a yellow "single-process — WILL disconnect" warning otherwise
      (`SplitDeployment` view field; `admin_handlers.go` + `admin.html`). Verified rendering
      in an app-mode boot (HTTP 200, banner present).
- ☐ Optional separate "restart proxy" admin action (deferred — not required; use
      `systemctl restart yufa-mt-proxy` for the rare proxy redeploy).
- ☐ Update `MEMORY.md` pointers if/when Phase 3 lands on x230 (behavior on the live box
      changes from single-unit to two-unit).

---

## 9. Verification plan (detail)

- **IPC round-trip (local, no game):** terminal A `--mode=proxy`, terminal B `--mode=app`;
  trigger a mob sweep from admin → expect inject errors only because no live zone session,
  but `status` must report `ready:false` cleanly (not a crash/hang).
- **Zero-disconnect (x230, the whole point):** keep a `watch -n1 'ss -tnp | grep 6121'`
  (or the `/health/zone` age) running while pressing the deploy button. The ESTABLISHED
  zone connection must persist across the app PID change; `/health/zone` `age` must keep
  ticking near zero (chat capture in app gaps a few seconds — acceptable).
- **Proxy restart still works (rare path):** `systemctl restart yufa-mt-proxy` → expect the
  one disconnect + watchdog auto-recovery, exactly like today.

## 10. Rollback
The single `yufa-mt.service` + `--mode=all` default is fully preserved. To roll back:
`systemctl disable --now yufa-mt-app yufa-mt-proxy`, re-enable `yufa-mt.service`. No DB or
binary changes are mode-specific. Keep the old unit file in `deploy/` until Phase 3 passes.

## 11. Open questions
- ☐ RuntimeDirectory ownership / socket path between the two units (§7) — final decision.
- ☐ Should chat capture ever move to the proxy for capture continuity across app restarts?
  (Default: NO — keep it in app; a few seconds' gap is fine and parsers churn.)
- ☐ Does the app need to *start* the proxy or assume systemd manages it? (Assume systemd.)
- ☐ Any other cross-boundary calls beyond `injectChatCommand`/`zoneProxyReady`? Re-grep
  before Phase 1 sign-off: `grep -rn 'activeZone\|injectChat\|zoneProxyReady\|setZoneCharName'`.

---

## 12. Changelog (append as you work — for handoff)
- 2026-06-02 — Plan created. Architecture surveyed: confirmed chat capture is pcap-based
  and independent of the relay (stays in app); only `injectChatCommand` + `zoneProxyReady`
  cross the boundary. No code changed yet.
- 2026-06-02 — **Implemented Phases 0–2 + Phase 4 UI.** Files added: `proxy_inject.go`
  (dispatch vars), `proxy_ipc.go` (IPC server + protocol), `proxy_ipc_client.go` (app
  client), `proxy_ipc_test.go`, `deploy/yufa-mt-proxy.service`, `deploy/yufa-mt-app.service`.
  Files edited: `cmd/server/main.go` (`--mode`), `internal/config/config.go` (`Mode`,
  `ProxyIPCSocket`, consts, validation), `server.go` (mode dispatch + `runProxyMode` +
  `useProxyIPC`), `scraper_jobs.go` (proxies only in `ModeAll`), `zoneproxy.go` +
  `zoneproxy_stub.go` (locals → `*Local`), `selfupdate.go` (mode-aware log/doc),
  `models.go` + `admin_handlers.go` + `admin.html` (split banner), `README.md` (deploy
  section). Decision: chat injection char-name framing stays proxy-side; app sends raw
  command text. Decision: per-request dial in the IPC client (no long-lived conn to heal).
  Verified locally: build/vet/test all green; `--mode=proxy` (IPC-only, no iptables),
  `--mode=app` (no iptables, serves, IPC client wired), default `--mode=all` (in-process)
  all boot & shut down cleanly; cross-process IPC round-trip over the real binary.
  Pre-existing gofmt quirks in models.go:727 / admin_handlers.go:455 left untouched (not
  mine — confirmed via git diff). **NOT done:** Phase 3 live x230 verification (needs SSH
  to the box + a running game client; this is a `/home/dns/tmp` copy). Next instance:
  deploy the two units on x230 and run the Phase 3 acceptance test.
