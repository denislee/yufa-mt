# Headless Projeto Yufa chat listener

Keeps **one** real (Gepard-protected) Projeto Yufa client connected, headless and
low-resource, so yufa-mt's passive packet sniffer always has live chat traffic to
read. The client does all the talking; we never try to bypass Gepard.

> **Why a full client and not a bot?** Projeto Yufa runs **Gepard Shield 3.0**,
> which blocks OpenKore/third-party clients and resists handshake replay or DLL
> emulation (only private/paid bypasses exist; they break daily and risk a ban).
> But the sniffer works *because chat packets are cleartext on the wire on this
> server*, so all we need is one genuine client generating traffic.

## How it works

```
systemd --user ──> yufa-listener.sh run
   ├─ Xvfb :99 (640x480x16, software OpenGL/llvmpipe)
   ├─ umu-run  (GE-Proton, wine_prefix_2, PROTON_USE_WINED3D=1)   ← NOT lutris
   ├─ poll-dismiss the startup Lua popup until the login window appears
   ├─ pin window to 0,0; (saved-login) type password; click Login
   ├─ enter character-select PIN; select character
   └─ monitor; exit if the client dies -> systemd restarts
```

Key environment facts discovered on this box (see `config.env.example`):

- **Launch via `umu-run` directly, not `lutris`** — lutris resets `DISPLAY` back
  to the real `:0` and drops our env, so the client never lands on `:99`.
- **`PROTON_USE_WINED3D=1` is required** — Xvfb's Intel Vulkan exposes no X11
  presentable surface, so DXVK can't create a window; wined3d (OpenGL/llvmpipe)
  renders fine.
- The client shows a **startup Lua popup** (`HatEFID nil`) that blocks the login
  window; we poll-dismiss it.
- **"Salvar Login" locks the ID field** to the remembered account, so we use
  *saved-login mode* (below) and only type the password + PIN.

## Requirements

- `Xvfb`, ImageMagick `import`, `umu-run`, GE-Proton — present on this box.
- `xdotool` (`sudo pacman -S xdotool`) — injects via X11 `XTEST`, which Xvfb
  accepts; `ydotool` works at kernel uinput level and can't reach Xvfb.

## One-time setup

```bash
cd scripts/listener
./install.sh                                   # deps + AutoFreeCPU + service
$EDITOR ~/.config/yufa-listener/credentials.env   # YUFA_PASS and YUFA_PIN
```

### Make the listener account the *remembered* one (required for saved-login)

Because the ID field is locked by "Salvar Login", the automation does **not**
type the username — it logs in as whatever account the client last saved in
`wine_prefix_2`. So once, by hand, log into that prefix as the listener account
with **"Salvar Login" checked**, e.g.:

```bash
lutris lutris:rungameid/4     # opens the same prefix on your desktop
# log in as the listener account, tick "Salvar Login", then quit
```

After that, the headless run only needs the password (+ PIN), which is the part
that automates reliably. (Set `USE_SAVED_LOGIN=0` to type the ID instead, but the
locked field makes that unreliable.)

## Calibrate once

```bash
./yufa-listener.sh run &
./yufa-listener.sh shot ~/s.png     # inspect each stage; magnify the form region
./yufa-listener.sh stop
```

The window is pinned to `0,0`, so click coordinates are stable. If the form sits
elsewhere (different `SCREEN_GEOMETRY`), re-read coords off a `shot` and set
`LOGIN_PW_CLICK` / `LOGIN_BUTTON_CLICK` / `DIALOG_OK_CLICK` / `PIN_CLICK` /
`CHAR_SELECT_CLICK` in `~/.config/yufa-listener/config.env`.

## Run as a service

```bash
systemctl --user enable --now yufa-listener.service
loginctl enable-linger "$USER"
journalctl --user -u yufa-listener -f
```

## Minimum-resource notes

- `install.sh` sets ROExt **`AutoFreeCPU=1`** so the client idles its render loop
  while the window is inactive (always, headless).
- 640x480x16 Xvfb + software GL; the service caps it (`CPUQuota=40%`, `Nice=10`).
- Disable in-game sound/BGM/effects to shave more.

## Troubleshooting

- Black `shot` / no window → bump `SETTLE_AFTER_WINDOW`; confirm
  `EXTRA_ENV="PROTON_USE_WINED3D=1"`.
- Stuck on the Lua popup → check `DIALOG_OK_CLICK` against a magnified `shot`.
- Logged in as the wrong account → redo the one-time saved-login step above.
- Verify it's really in-game: a persistent `ss -tn | grep ESTAB` to the game
  server, and new rows in yufa-mt's `chat` table.
- Logs: `~/.local/state/yufa-listener/listener.log` + the journal.
