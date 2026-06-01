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

This is the mode in `~/.config/yufa-listener/config.env`. Start it inside your
desktop session:

```bash
scripts/listener/yufa-listener.sh run
```

or let it autostart at login — `~/.config/autostart/yufa-listener.desktop` is
installed. Stop with Ctrl+C (foreground) or `yufa-listener.sh stop`.

The **sniffer** (yufa-mt chat capture) is a separate keep-alive wrapper,
`run-sniffer.sh`, also autostarted via `~/.config/autostart/yufa-sniffer.desktop`.
It runs the binary in chat-only mode (`CHAT_CAPTURE_ONLY=1`) and logs to
`~/.local/state/yufa-listener/sniffer.log`. The binary needs `CAP_NET_RAW`:

```bash
sudo setcap cap_net_raw,cap_net_admin=eip <repo>/yufa-mt
```

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
   account so "Salvar Login" remembers it in that wine prefix).
2. `sudo setcap …` on the `yufa-mt` binary (one command, for packet capture).
3. `CHAT_CAPTURE_PORT` in `config.env` if the server's game port isn't `6121`
   (find it in-game: `ss -tn | grep <exe>` → the foreign port).

If detection guesses wrong, override any value in `config.env`
(see `config.env.example`). Match a different RO client with `GAME_EXE_PATTERN`.

### `RUN_MODE=headless` — does NOT work for login (kept for record)

Fully headless via Xvfb + openbox + umu-run (wined3d) reaches the login screen
and the rendering/launch pipeline all works — **but the client ignores synthetic
keyboard input** (it reads the keyboard via DirectInput / real hardware only), so
the password can't be typed. Mouse clicks work; keystrokes do not. `xdotool`
(XTEST) and `ydotool` (uinput, can't reach Xvfb) both fail for keys, even with
the window focused and activated by openbox. VNC wouldn't help either (it injects
via XTEST). So unattended headless login is not achievable with this client.

## Why a client at all (not a bot)

Projeto Yufa runs **Gepard Shield 3.0**, which blocks OpenKore/third-party
clients and resists handshake replay / DLL emulation (only private/paid bypasses,
which break daily and risk a ban). The sniffer works because chat is cleartext on
the wire, so we just need one genuine client connected.

## Requirements

- `lutris` + GE-Proton (your existing setup) for real mode.
- Headless-mode extras (Xvfb, `xdotool`, `openbox`, `import`, `umu-run`) are only
  needed if you experiment with `RUN_MODE=headless`.

## Minimum-resource notes

- `install.sh` sets ROExt `AutoFreeCPU=1` so the client idles its render loop
  while inactive.
- Park the character AFK in a busy town (Prontera) for the broadest chat feed
  (Global/Main/Trade/announcements; Local is position-limited).

## Verify it's working

- Persistent `ss -tn | grep ESTAB` to the game server = in-game and connected.
- New rows in yufa-mt's `chat` table, and the `🟢 Chat listener active` log line.
- Logs: `~/.local/state/yufa-listener/listener.log`.
