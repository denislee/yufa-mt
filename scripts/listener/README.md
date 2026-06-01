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

### `RUN_MODE=headless` — still does NOT work for login, but the reason changed

The rendering/launch pipeline works fully headless and reaches the login screen.
Earlier this section claimed the blocker was synthetic keyboard input. That was
only half right — and only true for XTEST/Xvfb. The real findings:

**Keyboard input CAN be injected** — on the real Wayland (sway) seat, not Xvfb.
`ydotool` writes to the kernel `uinput`/evdev layer, so its keystrokes look like
real hardware to libinput → the compositor → Xwayland → the client, and the
client accepts them. (`xdotool`/XTEST keystrokes are rejected; that's the
limitation originally documented.) Verified end-to-end: launching the client on a
runtime `swaymsg create_output` headless output and driving the whole login UI —
server-select, password field, dialogs — entirely with `ydotool key`/`type`.
Requires `ydotoold` running as root (`/dev/uinput` is root-only):
`sudo ydotoold -p /tmp/.ydotool_socket -o 1000:1000`.

**Mouse clicks do NOT register** via any method tried — `xdotool` (XTEST),
`ydotool` (uinput `BTN_LEFT`), or `swaymsg seat - cursor press`. The client
reads the mouse through DirectInput bound to the physical device and ignores the
virtual one (it renders no cursor of its own). Login was still possible because
every screen is keyboard-drivable: `Enter` confirms dialogs, the password field
auto-focuses, and the saved account ("Salvar Login") pre-fills the ID — which is
itself stored in the wine registry under
`[Software\Wow6432Node\Gravity Soft\Ragnarok]` `"ID"=`, so a different remembered
account can be swapped in there (the client rewrites its token blob each run).

**The blocker is at the connection handshake (cause not yet confirmed — see the
`rtest` caveat below).** With a patched, up-to-date client (run `#Patch Projeto
Yufa - ABRA POR AQUI.exe` first — an outdated client fails to connect at all) and
correct credentials, the client *does* reach the login server (`ESTAB` to its
`:7900`), but the connection is immediately dropped and it shows *"Não foi
possível conectar-se ao servidor."*
A manual login with the laptop's real keyboard connects fine, so the genuine
client is accepted and only this automated/headless path is rejected. The
original guess was that Gepard detects the `ydotoold` virtual input device — but
that's now doubtful (see the caveat below): killing `ydotoold` at submit time
didn't help, and a Wine-hosted anti-cheat can't see Linux `/dev/uinput` anyway.
The headless environment itself is the more likely cause. `rtest` settles it.

So unattended headless login is **not yet achieved** — and which factor blocks it
(headless environment vs. input automation) is the open question `rtest` is
designed to answer. If it turns out to be a genuine Gepard rejection of the
client itself, getting past that needs the private/paid bypasses this project
deliberately avoids.

> **Caveat — the experiment confounded two variables.** The known-good login was
> *(real display + manual keyboard)*; the known-bad was *(headless + ydotool)*.
> Those differ in **both** the environment (real GPU vs. Xvfb/wined3d) **and** the
> input (hardware vs. automated), so we can't yet say which one Gepard rejects.
> And note: Gepard is a Windows DLL **inside Wine** — it can't read Linux
> `/dev/uinput`, and by the time input reaches the game Wine has already turned it
> into ordinary Windows messages, so a uinput keyboard and a real one look
> identical at the game layer. That makes "Gepard detected the virtual device"
> doubtful (consistent with: killing `ydotoold` at submit didn't help) and points
> at the **headless environment** as the more likely blocker. The `rtest` command
> isolates it — see below.

### Isolating the blocker: real-display ydotool test (`rtest`)

Change one variable at a time: drive the login with `ydotool` on the **real**
GPU-accelerated display — the same environment as the working manual login.

```bash
# 1. ydotoold needs root (/dev/uinput is root-only); -o makes the socket yours.
sudo ydotoold -p /tmp/.ydotool_socket -o "$(id -u):$(id -g)" &

# 2. Launch the client on the real display and bring it to the login screen.
scripts/listener/yufa-listener.sh run        # leave running; ID is pre-filled

# 3. In another terminal: focus the client's login window, then run the test.
#    (run with sudo to also see process names in the connection sample)
scripts/listener/yufa-listener.sh rtest
```

`rtest` types the password (+ PIN) via ydotool, then samples `ss` for ~15s and
prints a verdict guide:

- **Persistent `ESTAB` to the server** → login *worked* with automated input on the
  real display ⇒ the blocker was the **headless environment**, not input
  automation. A hardware HID device (the Pico) would **not** help; effort belongs
  on the GPU/display side (real Xorg/seat, GPU passthrough) instead.
- **`ESTAB` drops + the in-client "Não foi possível conectar-se" dialog** → input
  automation is implicated even on the real seat. Only *then* is the
  [Pico HID](pico-hid/) (genuine USB hardware) worth trying.

Either outcome decides whether the Pico is worth building.

## Why a client at all (not a bot)

Projeto Yufa runs **Gepard Shield 3.0**, which blocks OpenKore/third-party
clients and resists handshake replay / DLL emulation (only private/paid bypasses,
which break daily and risk a ban). The sniffer works because chat is cleartext on
the wire, so we just need one genuine client connected.

## Requirements

- `lutris` + GE-Proton (your existing setup) for real mode.
- On a **Wayland desktop (e.g. Sway)**, **Xwayland** so the X11 client and
  `xdotool` have an X display to bind to (see the Wayland/Sway note above).
- Headless-mode extras (Xvfb, `xdotool`, `openbox`, `import`, `umu-run`) are only
  needed if you experiment with `RUN_MODE=headless`.
- `ydotool` + `ydotoold` (and `ss` from iproute2) only for the `rtest`
  real-display experiment described above.

## Minimum-resource notes

- `install.sh` sets ROExt `AutoFreeCPU=1` so the client idles its render loop
  while inactive.
- Park the character AFK in a busy town (Prontera) for the broadest chat feed
  (Global/Main/Trade/announcements; Local is position-limited).

## Verify it's working

- Persistent `ss -tn | grep ESTAB` to the game server = in-game and connected.
- New rows in yufa-mt's `chat` table, and the `🟢 Chat listener active` log line.
- Logs: `~/.local/state/yufa-listener/listener.log`.
