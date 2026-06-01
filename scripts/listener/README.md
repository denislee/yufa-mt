# Headless Projeto Yufa chat listener

Keeps **one** real (Gepard-protected) Projeto Yufa client connected, headless and
low-resource, so yufa-mt's passive packet sniffer always has live chat traffic to
read. The client does all the talking; we never try to bypass Gepard.

> **Why a full client and not a bot?** Projeto Yufa runs **Gepard Shield 3.0**,
> which blocks OpenKore/third-party clients (it hooks `send`/`recv`/`connect` and
> does integrity + handshake checks). There is no maintained public bypass — only
> private/paid ones that break daily and risk an account+HWID ban. But the sniffer
> works *because chat packets are cleartext on the wire on this server*, so all we
> need is one genuine client generating traffic. See the project notes for the full
> reasoning.

## How it works

```
systemd --user ──> yufa-listener.sh run
                      ├─ Xvfb :99 (640x480x16, software/Intel GPU)
                      ├─ lutris lutris:rungameid/4   (GE-Proton, wine_prefix_2)
                      ├─ xdotool auto-login (type user/pass, Enter, Enter)
                      └─ monitor client; exit if it dies -> systemd restarts
```

`yufa-mt` (separately) sniffs the chat off the NIC as it does today.

## Requirements

- `Xvfb`, `lutris`, ImageMagick `import` — already present on this box.
- `xdotool` — **must be installed**: `sudo pacman -S xdotool`.
  (It injects keystrokes via X11 `XTEST`, which Xvfb accepts. The existing
  `ydotool` works at the kernel `uinput` level and does **not** reach Xvfb.)

## Setup

```bash
cd scripts/listener
./install.sh                       # deps check + AutoFreeCPU + config + service
$EDITOR ~/.config/yufa-listener/credentials.env   # set YUFA_USER / YUFA_PASS
```

Use a **dedicated, low-value account** parked AFK in a busy town (Prontera) — an
idle character sees Global/Main/Trade/announcements; only *Local* chat is limited
to where it stands.

## Calibrate the login once (important)

The auto-login is timed/blind, so verify it before enabling the service:

```bash
# bring the client up and attempt login in the foreground
./yufa-listener.sh run &

# grab screenshots at each stage to see where it is:
./yufa-listener.sh shot ~/login1.png   # during SETTLE_AFTER_WINDOW -> should show ID/PW
./yufa-listener.sh shot ~/login2.png   # after Enter -> server select
./yufa-listener.sh shot ~/login3.png   # -> character select / in-game
```

Open the PNGs. Adjust `~/.config/yufa-listener/config.env`:

- Stuck on a black screen / no window → bump `SETTLE_AFTER_WINDOW`, and if it
  never renders set `EXTRA_ENV="PROTON_USE_WINED3D=1"` (software D3D9).
- Login screen visible but text not entered → the ID field wasn't focused; set
  `LOGIN_FIELD_CLICK="X,Y"` (read the coords off the screenshot).
- Character not entered → set `CHAR_SELECT_CLICK="X,Y"`.
- Wrong window targeted → check the title in the screenshot, set `WINDOW_NAME`.

Re-run just the key sequence without restarting the client:

```bash
./yufa-listener.sh login
```

When it logs in reliably, stop the test run (`./yufa-listener.sh stop`) and enable
the service.

## Run as a service

```bash
systemctl --user enable --now yufa-listener.service
loginctl enable-linger "$USER"          # keep running after logout
journalctl --user -u yufa-listener -f   # follow logs
```

## Minimum-resource notes

- `install.sh` sets ROExt **`AutoFreeCPU=1`** in `dinput.ini` — the client idles
  its render loop while the window is inactive (always, headless).
- 640x480x16 Xvfb keeps the framebuffer tiny.
- The service caps it: `CPUQuota=40%`, `Nice=10`, low `CPUWeight`/`IOWeight`.
  Lower `CPUQuota` in the unit if you want an even tighter leash.
- Disable in-game sound/BGM and effects in the client options to shave more.

## Troubleshooting

- `lutris -l -j` — list game ids if `LUTRIS_GAME_ID=4` isn't the right profile.
- Logs: `~/.local/state/yufa-listener/listener.log` and the journal.
- If the sniffer shows "disconnected", check the journal — the service restarts
  the client on crash, but a server-side kick needs a fresh login cycle (which the
  restart provides).
- Coordinate the sniffer's `CHAT_CAPTURE_PORT` with the actual **map server** port
  (chat flows on the zone/map server, not the char server).
