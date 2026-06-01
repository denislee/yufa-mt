# Projeto Yufa chat listener

Keeps a Projeto Yufa client connected and alive so yufa-mt's passive packet
sniffer always has live chat traffic to read. The client does all the talking;
we never bypass Gepard.

## Two modes

### `RUN_MODE=real` — the working mode

Launches the client on your **real X display** (`:0`) via lutris (your normal,
GPU-accelerated setup, with the remembered `yufayufa` account) and keeps it
alive in a self-restarting loop. **You log in once by hand** (password + PIN
`1122`) with your real keyboard; RO sessions last hours, so it's infrequent. On
crash it relaunches and you log in again.

A **zone-connection watchdog** (`ZONE_WATCHDOG=1`, default on) covers the case a
bare process check misses: if the client loses its in-game connection for
`DISCONNECT_GRACE` seconds (default 90) — kicked back to login, AFK timeout, or a
mistimed auto-login — while the `.exe` keeps running, the loop kills and
relaunches it. With `AUTO_LOGIN=1` (+ the PIN proxy) that relogin is hands-free.
Tune via `MONITOR_INTERVAL` / `DISCONNECT_GRACE` / `ZONE_PORT` in `config.env`.

This is the mode in `~/.config/yufa-listener/config.env`. Start it inside your
desktop session:

```bash
scripts/listener/yufa-listener.sh run
```

> **Wayland/Sway:** the client and `xdotool` are X11-only — they cannot talk to
> a native Wayland compositor. On a Sway (or any Wayland) desktop, **Xwayland
> must be running** and `REAL_DISPLAY` must point at its X display (typically
> `:0` or `:1`, i.e. the value of `$DISPLAY` inside the session). The default
> `REAL_DISPLAY=${DISPLAY:-:0}` resolves to this automatically when the script
> runs inside the Sway session; if it's empty or wrong, set `REAL_DISPLAY`
> explicitly in `config.env`. Check with `echo $DISPLAY` (must be non-empty) and
> `xdotool getdisplaygeometry` (must succeed) from within the session.

or let it autostart at login — `~/.config/autostart/yufa-listener.desktop` is
installed. Stop with Ctrl+C (foreground) or `yufa-listener.sh stop`.

The **sniffer** (yufa-mt chat capture) is a separate keep-alive wrapper,
`run-sniffer.sh`, also autostarted via `~/.config/autostart/yufa-sniffer.desktop`.
It runs the binary in chat-only mode (`CHAT_CAPTURE_ONLY=1`) and logs to
`~/.local/state/yufa-listener/sniffer.log`. The binary needs `CAP_NET_RAW` (for
packet capture) and `CAP_NET_ADMIN` (for the in-app PIN proxy's iptables rule):

```bash
sudo setcap cap_net_raw,cap_net_admin=eip <repo>/yufa-mt
```

The same binary also runs the **character-select PIN proxy in-process**:
`run-sniffer.sh` sets `PIN_PROXY=1` automatically when `YUFA_PIN` is configured,
so on startup yufa-mt installs an iptables REDIRECT of the char port and
auto-enters the PIN — no mouse click, no separate service. See [mitm/](mitm/).

## Portability — running on another computer

Almost everything **auto-detects**, so the scripts are not tied to this machine:

| Auto-detected | From |
|---|---|
| `LUTRIS_GAME_ID` | `lutris -l -j`, matched to the RO game by slug |
| `GAME_DIR` / `GAME_EXE` / `WINEPREFIX_DIR` | the Lutris game's `.yml` |
| `GAME_PROC` | exe basename |
| `PROTONPATH` | newest `GE-Proton*` in `compatibilitytools.d` |
| `UMU_RUN` | lutris runtime path or `$PATH` |
| `REAL_DISPLAY` | `$DISPLAY` (fallback `:0`) |
| `CHAT_CAPTURE_DEVICE` | default-route NIC (in `run-sniffer.sh`) |
| repo path | derived from the script's own location |

What you still set **per machine** (none are guessable):

1. `credentials.env` — `YUFA_PASS`, `YUFA_PIN` (and a one-time manual login as the
   account so "Salvar Login" remembers it in that wine prefix). Setting `YUFA_PIN`
   auto-enables the in-app PIN proxy (`PIN_PROXY=1`); set `PIN_PROXY=0` in
   `config.env` to opt out.
2. `sudo setcap …` on the `yufa-mt` binary (one command — `cap_net_raw` for packet
   capture, `cap_net_admin` for the PIN proxy's iptables rule).
3. `CHAT_CAPTURE_PORT` in `config.env` — **on Projeto Yufa chat flows on the
   zone/map port `6121`, which is already the default.** The server applies a
   +1000 offset to the *standard* RO ports — login 6900→7900, char 6121→7121,
   map/zone 5121→6121 — so chat is on `6121` and `7121` is the char server (see
   "Wire protocol" below). Verify in-game with `ss -tn | grep <exe>` → the foreign port.

If detection guesses wrong, override any value in `config.env`
(see `config.env.example`). Match a different RO client with `GAME_EXE_PATTERN`.

### Automated login — SOLVED except the PIN keypad click (2026-06-01)

The earlier headless experiments left an open question — was the login blocker the
**headless environment** or the **input automation**? Running `ydotool` on the
**real GPU display** (the same environment as the working manual login) settled
it. Result: **automated login works end-to-end, all the way to character select.**

What works, driven entirely over SSH against the live Sway seat (`ydotoold` as
root + `grim` screenshots as the feedback loop):

1. Launch via `lutris lutris:rungame/ragnarok-online` on the real output (force
   the panel on if the lid's closed: `swaymsg output LVDS-1 enable; … power on`).
2. `ydotool key 28:1 28:0` dismisses the recurring Lua error
   (`[string "buf"]:2: attempt to index global 'HatEFID'`).
3. Enter selects the proxy (`Proxy 1-Loki`).
4. The ID is pre-filled (`yufayufa`, "Salvar Login") and the **password field is
   auto-focused** — `ydotool type "$YUFA_PASS"` + Enter. **Account auth succeeds.**
5. Enter selects the `Yufa` server → the **character-select screen renders.**

So the old "Gepard rejects the automated/headless client at the handshake"
conclusion was **wrong** — that was the headless (Xvfb/wined3d) environment. On the
real display the genuine client connects fine and Gepard is happy. **Keyboard
automation is fully solved.**

**The one remaining wall is the character-select PIN keypad — and it needs a mouse
*click*.** It's a randomized anti-keylogger pad (`"Clique em uma sequência de 4
números"`, e.g. `4 0 9 / 1 8 3 / 5 6 7 / 2 Reset`; our PIN is `1122`). Measured
behaviour:

- Mouse **motion** via `ydotool` reaches the client — hovering a cell highlights
  it and the hand cursor lands correctly. (This refutes the old blanket "mouse
  never registers".)
- Mouse **button clicks do NOT register.** Confirmed the click *is* emitted at the
  kernel: reading the ydotoold evdev node `/dev/input/event15` shows
  `EV_KEY code=272 (BTN_LEFT) val=1/0`. So the loss is **above the OS**, in the
  Sway→Xwayland→Wine→Gepard delivery — almost certainly Gepard's mouse-button
  validation (it accepts motion but rejects the injected/synthetic button). No
  software inject (XTEST, uinput `BTN_LEFT`, held press) gets past it.
- The keypad refuses **keyboard** digits too (`type "1122"`+Enter does nothing).

#### Why faking the PIN packet does NOT work (investigated 2026-06-01)

Tempting, since most of the wire is cleartext — but a packet capture of the full
login proves it's a dead end:

- After the char list arrives, the char-server connection goes **silent except
  `0x0187` (CZ_PING) every 12s.** The genuine client is parked at its *local* PIN
  dialog, exchanging nothing. The goal requires *this genuine client* to be
  in-game; only it processing a real click advances its UI (send `CH_SECOND_PASSWD`
  / `CH_SELECT_CHAR` → map-server redirect → zone connect).
- Forging server-bound packets can satisfy the *server* but cannot drive the
  *client's* local state machine off the keypad — so the genuine client never
  enters the world, and there's no in-game client to read chat from.
- The only way to make "the client" advance without the real client is to *be* the
  client (full protocol MITM that speaks login + char + zone). That's OpenKore —
  exactly what Gepard blocks (and the login channel is encrypted anyway; see
  below). It also abandons the "one genuine client" premise.

The resolution sidesteps the click entirely: a transparent local proxy terminates
the char connection and, on the cleartext `0x08b9` PIN prompt, computes the slot
digits from the seed and injects the `0x08b8` answer to the server itself — then
relays the server's OK back so the genuine client closes its *own* keypad and
proceeds. No click, no Gepard bypass. This proxy now runs **in-process inside
`yufa-mt`** (`internal/server/pinproxy.go`), started by `run-sniffer.sh` with
`PIN_PROXY=1` — so it's already up before the client logs in. See [mitm/](mitm/)
(and [pinpad/](pinpad/) for the seed→slots math).

### Wire protocol — what's cleartext vs. encrypted (verified by capture)

Captured with a raw `AF_PACKET` Python sniffer (the box has no tcpdump/tshark):

- **Char server (`:7121`) is cleartext, standard RO** — clean opcodes
  `0x0065` (CH_ENTER), `0x082d` (char-list accept), `0x09a1` (charlist req),
  `0x0b72` (char-info page), `0x0187` (ping), AID in the clear, and two identical
  pings (no per-packet cipher).
- **Zone/map server (`:6121`) is also cleartext, and is where chat actually
  flows** — channel joins and ongoing messages, confirmed in full-gameplay
  captures. **This is why the sniffer works.** Capture on `CHAT_CAPTURE_PORT=6121`
  (the default). An earlier note placed chat on `7121`; that came from a capture
  stuck at the PIN that never opened the zone connection. See
  [GEPARD-PROTOCOL-FINDINGS.md](GEPARD-PROTOCOL-FINDINGS.md).
- **Login/account servers (`:6700`, `:7900`) are Gepard-encrypted** — high-entropy
  payloads, garbage opcodes (`0x3488`/`0x9264`/`0x8200`), and the `CA_LOGIN`
  (`0x0064`) username/password fields scrambled. (Account auth also touches
  `*:443` — Gepard's own backend validation.)
- Client config loads over **plain HTTP on `:8888`** (`POST /userconfig/load`).

## Why a client at all (not a bot)

Projeto Yufa runs **Gepard Shield 3.0**, which blocks OpenKore/third-party
clients and resists handshake replay / DLL emulation (only private/paid bypasses,
which break daily and risk a ban). The sniffer works because **chat is cleartext on
the wire** (confirmed — the zone server `:6121` speaks plain RO protocol), so we
just need one genuine client connected.

## Requirements

- `lutris` + GE-Proton (your existing setup) for real mode.
- On a **Wayland desktop (e.g. Sway)**, **Xwayland** so the X11 client and
  `xdotool` have an X display to bind to (see the Wayland/Sway note above).
- `ydotool` + `ydotoold` (and `ss` from iproute2) for the automated keyboard
  login; `grim` for the screenshot feedback loop.

## Minimum-resource notes

- `install.sh` sets ROExt `AutoFreeCPU=1` so the client idles its render loop
  while inactive.
- Park the character AFK in a busy town (Prontera) for the broadest chat feed
  (Global/Main/Trade/announcements; Local is position-limited).

## Verify it's working

- Persistent `ss -tn | grep ESTAB` to the game server = in-game and connected.
- New rows in yufa-mt's `chat` table, and the `🟢 Chat listener active` log line.
- Logs: `~/.local/state/yufa-listener/listener.log`.
