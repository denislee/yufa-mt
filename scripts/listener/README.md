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
