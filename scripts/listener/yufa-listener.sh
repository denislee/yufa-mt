#!/usr/bin/env bash
#
# yufa-listener.sh — run the Projeto Yufa client headless (Xvfb) under
# Lutris/GE-Proton and auto-login, so yufa-mt's passive chat sniffer always
# has live traffic to read. The real (Gepard-protected) client does all the
# talking; we just keep one instance connected with minimum resources.
#
# Subcommands:
#   run     (default) start Xvfb, launch the client, auto-login, then monitor
#   login   run only the auto-login key sequence against the running display
#           (use this to re-calibrate timings without restarting the client)
#   shot    grab a PNG screenshot of the headless display (for calibration)
#   stop    kill the client and the Xvfb we started
#   env     print the resolved configuration and exit
#
# Config is loaded from (later overrides earlier):
#   1. the defaults below
#   2. ~/.config/yufa-listener/config.env        (optional overrides)
#   3. ~/.config/yufa-listener/credentials.env   (required: YUFA_USER/YUFA_PASS)
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Paths & user config -----------------------------------------------------
# Source user overrides FIRST so they win and we skip auto-detection for any
# value you set explicitly. Nothing below is hardcoded to one machine.
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/yufa-listener"
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/yufa-listener"
LOG_FILE="$STATE_DIR/listener.log"
XVFB_PID_FILE="$STATE_DIR/xvfb.pid"
WM_PID_FILE="$STATE_DIR/wm.pid"
[ -f "$CONFIG_DIR/config.env" ]      && . "$CONFIG_DIR/config.env"
[ -f "$CONFIG_DIR/credentials.env" ] && . "$CONFIG_DIR/credentials.env"

# --- Auto-detection (only fills values not set in config.env) ----------------
# Which Lutris game is the RO client (matched on the exe path in its .yml).
: "${GAME_EXE_PATTERN:=Projeto_Yufa|[Rr]agexe|[Rr]agnarok}"
_LUTRIS_GAMES_DIR="$HOME/.local/share/lutris/games"
_LUTRIS_SLUG=""

# Read GAME_EXE/WINEPREFIX_DIR/GAME_DIR + slug from the matching Lutris .yml.
detect_from_lutris_yml() {
  local f
  f="$(grep -lE "exe:.*(${GAME_EXE_PATTERN})" "$_LUTRIS_GAMES_DIR"/*.yml 2>/dev/null | xargs -r ls -t 2>/dev/null | head -1)"
  [ -z "$f" ] && return 1
  _yval() { sed -n "s/^[[:space:]]*$1:[[:space:]]*//p" "$f" | head -1; }
  [ -z "${GAME_EXE:-}" ]       && GAME_EXE="$(_yval exe)"
  [ -z "${WINEPREFIX_DIR:-}" ] && WINEPREFIX_DIR="$(_yval prefix)"
  [ -z "${GAME_DIR:-}" ]       && GAME_DIR="$(_yval working_dir)"
  _LUTRIS_SLUG="$(basename "$f" .yml | sed 's/-[0-9]\{6,\}$//')"
}
detect_lutris_id() {  # map slug -> numeric id via `lutris -l -j`
  command -v lutris >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1 || return 0
  lutris -l -j 2>/dev/null | python3 -c "import sys,json
try: print(next((g['id'] for g in json.load(sys.stdin) if g.get('slug')=='$_LUTRIS_SLUG'),''))
except Exception: pass" 2>/dev/null
}
detect_proton() { ls -d "$HOME/.local/share/Steam/compatibilitytools.d"/GE-Proton* 2>/dev/null | sort -V | tail -1; }
detect_umu() { local p; for p in "$HOME/.local/share/lutris/runtime/umu/umu-run" "$(command -v umu-run 2>/dev/null)"; do [ -n "$p" ] && [ -x "$p" ] && { echo "$p"; return; }; done; }
detect_nic() { ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1);exit}}'; }

detect_from_lutris_yml || true

# --- Run mode ----------------------------------------------------------------
# real     = launch the client on the real X display via lutris; you log in once
#            with your real keyboard (the working mode).
# headless = Xvfb + openbox + auto-login — does NOT work (client ignores
#            synthetic keyboard input); kept for record.
: "${RUN_MODE:=real}"
: "${REAL_DISPLAY:=${DISPLAY:-:0}}"     # real X display used in real mode
: "${LUTRIS_GAME_ID:=$(detect_lutris_id)}"

# --- Client identity (auto-detected from Lutris; override in config.env) ------
: "${GAME_DIR:=$HOME/Downloads/Projeto Yufa}"
: "${GAME_EXE:=$GAME_DIR/Projeto_Yufa.exe}"
: "${WINEPREFIX_DIR:=$GAME_DIR/wine_prefix_2}"
: "${GAME_PROC:=$(basename "$GAME_EXE")}"   # process name tracked for liveness
: "${WINDOW_NAME:=Gepard Shield}"           # xdotool window-name regex (headless)

# --- Proton / umu-run (auto-detected) ----------------------------------------
: "${UMU_RUN:=$(detect_umu)}"
: "${PROTONPATH:=$(detect_proton)}"

# --- Headless display ---------------------------------------------------------
: "${DISPLAY_NUM:=:99}"                 # virtual display to create
: "${SCREEN_GEOMETRY:=640x480x16}"      # Xvfb -screen 0 geometry (low = light)

# A minimal window manager (openbox) runs on the display so the client window
# gets *activated* — RO's text fields only accept keyboard input when the window
# is active, which is impossible without a WM. The rc keeps windows undecorated
# so click coordinates stay valid.
: "${USE_WM:=1}"
: "${WM_RC:=$SCRIPT_DIR/openbox-rc.xml}"

# Login timing knobs (seconds). Tune with the `login`/`shot` subcommands.
: "${WAIT_FOR_WINDOW:=180}"             # max time to wait for the window to appear
: "${SETTLE_AFTER_WINDOW:=25}"          # initial Gepard init wait before polling
: "${LOGIN_READY_TRIES:=40}"            # poll iterations (x2s) waiting for the login window
: "${WAIT_AFTER_LOGIN:=10}"             # login -> PIN/char-select screen
: "${WAIT_BEFORE_PIN:=4}"               # settle before typing the PIN
: "${WAIT_BEFORE_CHARSELECT:=6}"        # PIN -> character select
: "${WAIT_MAP_LOAD:=25}"                # character select -> in-game/map loaded
: "${KEY_DELAY_MS:=70}"                 # per-keystroke delay while typing

# The client shows a modal Lua popup on startup ("HatEFID nil") and may show
# others after login. We dismiss any non-main/non-IME window this many times.
: "${DISMISS_DIALOGS:=4}"

# OK button of the startup Lua popup (when window-centered at 640x480). Clicked
# as a fallback if focusing the dialog window doesn't dismiss it.
: "${DIALOG_OK_CLICK:=322,265}"

# The window is pinned to 0,0 before we touch the form so absolute click coords
# are deterministic (there is no window manager to place it). Coordinates below
# are window-relative-from-0,0 at 640x480; re-tune with `shot` if you change
# SCREEN_GEOMETRY. RO's custom UI ignores Tab/Ctrl+A, so we click each field and
# clear it with BackSpace. Empty value -> skip that click.
: "${WINDOW_PIN_XY:=0,0}"               # where to pin the client window (empty -> don't move)
: "${LOGIN_ID_CLICK:=281,305}"          # ID / account field
: "${LOGIN_PW_CLICK:=285,328}"          # password field
: "${LOGIN_BUTTON_CLICK:=420,308}"      # the big Login button (empty -> press Return)
: "${PIN_CLICK:=}"                     # PIN field, if it needs a focus click (optional)
: "${CHAR_SELECT_CLICK:=}"             # character slot before Enter (optional)

# Saved-login mode. The client's "Salvar Login" feature remembers the account
# and locks the ID field, so blind typing into it is ignored. With this on we
# DON'T touch the ID field — we rely on the remembered account and only fill the
# password (+ PIN). Requires logging in once manually as the desired account in
# this wine prefix with "Salvar Login" checked. Set to 0 to type the ID instead.
: "${USE_SAVED_LOGIN:=1}"

# Character-select PIN. Put the real value as YUFA_PIN in credentials.env.
: "${PIN_CODE:=${YUFA_PIN:-}}"

# Extra environment for the umu-run launch. PROTON_USE_WINED3D=1 is required
# headless: Xvfb's Intel Vulkan has no X11 presentable surface, so DXVK can't
# create a window; wined3d (D3D9 -> OpenGL -> llvmpipe) renders fine.
: "${EXTRA_ENV:=PROTON_USE_WINED3D=1}"

mkdir -p "$STATE_DIR"

log() { printf '%s [listener] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE" >&2; }
die() { log "FATAL: $*"; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1 (see scripts/listener/README.md)"; }

# --- Xvfb lifecycle ----------------------------------------------------------
# Reuse only a display that actually answers; a bare lock file means a dead
# server (reusing that is what cascaded the earlier failures).
xvfb_responding() { DISPLAY="$DISPLAY_NUM" xdotool getdisplaygeometry >/dev/null 2>&1; }

start_xvfb() {
  if xvfb_responding; then
    log "Xvfb already responding on $DISPLAY_NUM, reusing it."
    return 0
  fi
  local n="${DISPLAY_NUM#:}"
  if [ -e "/tmp/.X${n}-lock" ]; then
    log "Removing stale X lock for $DISPLAY_NUM (no live server)"
    rm -f "/tmp/.X${n}-lock" "/tmp/.X11-unix/X${n}" 2>/dev/null
  fi
  need Xvfb
  log "Starting Xvfb on $DISPLAY_NUM ($SCREEN_GEOMETRY)"
  Xvfb "$DISPLAY_NUM" -screen 0 "$SCREEN_GEOMETRY" -nolisten tcp -noreset >>"$LOG_FILE" 2>&1 &
  echo $! >"$XVFB_PID_FILE"
  # wait for it to accept connections
  local i
  for i in $(seq 1 20); do
    DISPLAY="$DISPLAY_NUM" xdotool getdisplaygeometry >/dev/null 2>&1 && { log "Xvfb is up."; return 0; }
    sleep 0.5
  done
  die "Xvfb did not come up on $DISPLAY_NUM"
}

stop_xvfb() {
  if [ -f "$XVFB_PID_FILE" ]; then
    local pid; pid="$(cat "$XVFB_PID_FILE" 2>/dev/null)"
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      log "Stopping Xvfb (pid $pid)"; kill "$pid" 2>/dev/null
    fi
    rm -f "$XVFB_PID_FILE"
  fi
}

# --- Window manager (openbox) ------------------------------------------------
start_wm() {
  [ "$USE_WM" = 1 ] || { log "WM disabled (USE_WM=0)"; return 0; }
  command -v openbox >/dev/null 2>&1 || die "openbox not installed (sudo pacman -S openbox), or set USE_WM=0"
  local cfgflag=""
  [ -f "$WM_RC" ] && cfgflag="--config-file $WM_RC"
  log "Starting openbox on $DISPLAY_NUM"
  # shellcheck disable=SC2086
  DISPLAY="$DISPLAY_NUM" openbox $cfgflag >>"$LOG_FILE" 2>&1 &
  echo $! >"$WM_PID_FILE"
  sleep 1.5
}

stop_wm() {
  if [ -f "$WM_PID_FILE" ]; then
    local pid; pid="$(cat "$WM_PID_FILE" 2>/dev/null)"
    [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null
    rm -f "$WM_PID_FILE"
  fi
}

# --- Client lifecycle --------------------------------------------------------
game_pid() { pgrep -f "$GAME_PROC" | head -1; }

launch_client() {
  [ -x "$UMU_RUN" ] || die "umu-run not found/executable at $UMU_RUN"
  [ -d "$PROTONPATH" ] || die "Proton not found at $PROTONPATH"
  [ -f "$GAME_EXE" ] || die "game exe not found at $GAME_EXE"
  log "Launching client via umu-run on $DISPLAY_NUM (wined3d)"
  (
    cd "$GAME_DIR" || exit 1
    # shellcheck disable=SC2086
    exec env DISPLAY="$DISPLAY_NUM" \
        WINEPREFIX="$WINEPREFIX_DIR" \
        GAMEID=0 STORE=none \
        PROTONPATH="$PROTONPATH" \
        $EXTRA_ENV \
        "$UMU_RUN" "$GAME_EXE"
  ) >>"$LOG_FILE" 2>&1 &
  LAUNCH_PID=$!
  log "umu launcher pid $LAUNCH_PID; waiting up to ${WAIT_FOR_WINDOW}s for '$GAME_PROC'"
  local i
  for i in $(seq 1 "$WAIT_FOR_WINDOW"); do
    [ -n "$(game_pid)" ] && { log "Client process up (pid $(game_pid))."; return 0; }
    sleep 1
  done
  die "client process '$GAME_PROC' never appeared (check $LOG_FILE)"
}

# Real-display launch: use lutris (the user's normal, GPU-accelerated setup) on
# the real X server. No auto-login — the user types the password by hand.
launch_client_lutris() {
  need lutris
  log "Launching client via lutris (game id $LUTRIS_GAME_ID) on $REAL_DISPLAY"
  DISPLAY="$REAL_DISPLAY" lutris "lutris:rungameid/$LUTRIS_GAME_ID" >>"$LOG_FILE" 2>&1 &
  LAUNCH_PID=$!
  log "lutris pid $LAUNCH_PID; waiting up to ${WAIT_FOR_WINDOW}s for '$GAME_PROC'"
  local i
  for i in $(seq 1 "$WAIT_FOR_WINDOW"); do
    [ -n "$(game_pid)" ] && { log "Client process up (pid $(game_pid))."; return 0; }
    sleep 1
  done
  die "client process '$GAME_PROC' never appeared (check $LOG_FILE)"
}

# Find the client's window id on the headless display.
find_window() {
  local wid
  wid="$(DISPLAY="$DISPLAY_NUM" xdotool search --name "$WINDOW_NAME" 2>/dev/null | tail -1)"
  [ -z "$wid" ] && wid="$(DISPLAY="$DISPLAY_NUM" xdotool search --onlyvisible --name '.' 2>/dev/null | tail -1)"
  echo "$wid"
}

# --- Auto-login --------------------------------------------------------------
xdo() { DISPLAY="$DISPLAY_NUM" xdotool "$@"; }

click_at() {  # "X,Y"
  local xy="$1"; [ -z "$xy" ] && return 0
  xdo mousemove --sync "${xy%,*}" "${xy#*,}"
  xdo click 1
  sleep 0.4
}

# Pin the client window to a known position so click coords are deterministic,
# and *activate* it (openbox provides _NET_ACTIVE_WINDOW) so its text fields
# accept keyboard input. Echoes the wid.
pin_window() {
  local wid; wid="$(find_window)"
  [ -z "$wid" ] && return 0
  if [ -n "$WINDOW_PIN_XY" ]; then
    xdo windowmove "$wid" "${WINDOW_PIN_XY%,*}" "${WINDOW_PIN_XY#*,}" 2>/dev/null
  fi
  xdo windowraise "$wid" 2>/dev/null
  xdo windowactivate --sync "$wid" 2>/dev/null
  xdo windowfocus "$wid" 2>/dev/null
  echo "$wid"
}

# Modal popups are mapped, named windows that are neither the main client
# window nor an IME/Input helper.
dialog_wids() {
  local w n
  for w in $(xdo search --all --maxdepth 99 --name '.' 2>/dev/null); do
    n="$(xdo getwindowname "$w" 2>/dev/null)"
    case "$n" in ''|*Gepard*|*IME*|Input) continue ;; esac
    echo "$w"
  done
}

# Dismiss any popups present right now (focus + Return, plus an OK-coord click
# fallback). Returns 0 if at least one popup was found.
dismiss_dialogs_once() {
  local any="" w
  for w in $(dialog_wids); do
    any=1
    xdo windowraise "$w" 2>/dev/null; xdo windowfocus "$w" 2>/dev/null
    xdo key --clearmodifiers Return; xdo key --clearmodifiers space
  done
  if [ -n "$any" ] && [ -n "$DIALOG_OK_CLICK" ]; then
    xdo mousemove --sync "${DIALOG_OK_CLICK%,*}" "${DIALOG_OK_CLICK#*,}"; xdo click 1
  fi
  [ -n "$any" ]
}

# Poll until the main login window appears, clearing the startup popup(s) that
# block it. The Gepard window only maps after the Lua popup is dismissed, so its
# presence is our "login screen ready" signal. Echoes the wid (empty on timeout).
wait_login_ready() {
  local t wid
  for t in $(seq 1 "$LOGIN_READY_TRIES"); do
    wid="$(find_window)"
    [ -n "$wid" ] && { echo "$wid"; return 0; }
    dismiss_dialogs_once
    sleep 2
  done
  echo ""
}

clear_field() {  # RO custom fields ignore Ctrl+A; clear with End + BackSpace
  xdo key --clearmodifiers End
  local i; for i in $(seq 1 28); do xdo key --clearmodifiers BackSpace; done
}

type_field() {  # "X,Y" "text" — click field, clear it, type
  local xy="$1" text="$2"; [ -z "$xy" ] && return 0
  xdo mousemove --sync "${xy%,*}" "${xy#*,}"; xdo click 1; sleep 0.3
  clear_field
  xdo type --clearmodifiers --delay "$KEY_DELAY_MS" "$text"; sleep 0.3
}

auto_login() {
  need xdotool
  [ -n "${YUFA_USER:-}" ] || die "YUFA_USER not set (put it in $CONFIG_DIR/credentials.env)"
  [ -n "${YUFA_PASS:-}" ] || die "YUFA_PASS not set (put it in $CONFIG_DIR/credentials.env)"

  log "Initial settle ${SETTLE_AFTER_WINDOW}s, then polling for the login window"
  sleep "$SETTLE_AFTER_WINDOW"

  # Dismiss the startup Lua modal ("HatEFID nil") — which blocks and delays the
  # main window — until the login window actually appears.
  local wid; wid="$(wait_login_ready)"
  if [ -n "$wid" ]; then
    log "Login window ready ($wid)"
    pin_window >/dev/null
  else
    log "WARN: login window never appeared; acting on the focused surface"
  fi

  if [ "$USE_SAVED_LOGIN" = 1 ]; then
    log "Saved-login mode: using remembered account, entering password only"
  else
    log "Entering account id '$YUFA_USER'"
    type_field "$LOGIN_ID_CLICK" "$YUFA_USER"
  fi
  type_field "$LOGIN_PW_CLICK" "$YUFA_PASS"

  if [ -n "$LOGIN_BUTTON_CLICK" ]; then
    log "Clicking Login ($LOGIN_BUTTON_CLICK)"
    xdo mousemove --sync "${LOGIN_BUTTON_CLICK%,*}" "${LOGIN_BUTTON_CLICK#*,}"; xdo click 1
  else
    xdo key --clearmodifiers Return
  fi

  log "Login sent; waiting ${WAIT_AFTER_LOGIN}s for PIN/char-select"
  sleep "$WAIT_AFTER_LOGIN"
  dismiss_dialogs_once       # clear any post-login info popup

  # PIN entry (appears over character selection on this server).
  if [ -n "$PIN_CODE" ]; then
    sleep "$WAIT_BEFORE_PIN"
    log "Entering character-select PIN"
    pin_window >/dev/null
    click_at "$PIN_CLICK"
    xdo type --clearmodifiers --delay "$KEY_DELAY_MS" "$PIN_CODE"
    xdo key --clearmodifiers Return
  fi

  log "Waiting ${WAIT_BEFORE_CHARSELECT}s for character select"
  sleep "$WAIT_BEFORE_CHARSELECT"
  pin_window >/dev/null
  click_at "$CHAR_SELECT_CLICK"
  xdo key --clearmodifiers Return            # enter selected character

  log "Waiting ${WAIT_MAP_LOAD}s for map load"
  sleep "$WAIT_MAP_LOAD"
  log "Auto-login sequence complete."
}

# --- Monitor / cleanup -------------------------------------------------------
cleanup() {
  log "Cleaning up..."
  pkill -f "$GAME_PROC" 2>/dev/null
  # let Gepard/wine close the socket cleanly
  sleep 3
  pkill -9 -f "$GAME_PROC" 2>/dev/null
  [ -n "${LAUNCH_PID:-}" ] && kill "$LAUNCH_PID" 2>/dev/null
  stop_wm
  stop_xvfb
}

monitor() {
  log "Monitoring client liveness; will exit (and let systemd restart) if it dies."
  while [ -n "$(game_pid)" ]; do sleep 15; done
  log "Client process exited."
}

cmd_run() {
  trap 'cleanup; exit 0' INT TERM
  trap 'cleanup' EXIT
  if [ "$RUN_MODE" = real ]; then
    [ -n "${XAUTHORITY:-}" ] || export XAUTHORITY="$HOME/.Xauthority"
    log "Real-display mode on $REAL_DISPLAY — launch + keep-alive; log in manually with your keyboard."
    # Self-restarting loop so it survives client crashes without depending on
    # systemd env for the :0 session. Run it inside your graphical session.
    while true; do
      launch_client_lutris
      log "Client up on $REAL_DISPLAY. Log in (password + PIN) with your keyboard; it stays connected."
      monitor
      log "Client exited; relaunching in 10s (you'll need to log in again)."
      sleep 10
    done
  fi
  start_xvfb
  start_wm
  launch_client
  auto_login
  monitor
  # falling out of monitor means the client died -> non-zero so the service restarts
  exit 1
}

cmd_login() { auto_login; }

cmd_shot() {
  need import
  local out="${1:-$STATE_DIR/screen-$(date +%H%M%S).png}"
  DISPLAY="$DISPLAY_NUM" import -window root "$out" && log "Saved screenshot: $out"
}

cmd_stop() { cleanup; }

cmd_env() {
  for v in RUN_MODE REAL_DISPLAY LUTRIS_GAME_ID DISPLAY_NUM SCREEN_GEOMETRY GAME_PROC WINDOW_NAME GAME_DIR \
           UMU_RUN PROTONPATH WINEPREFIX_DIR GAME_EXE \
           WAIT_FOR_WINDOW SETTLE_AFTER_WINDOW WAIT_AFTER_LOGIN WAIT_BEFORE_PIN \
           WAIT_BEFORE_CHARSELECT WAIT_MAP_LOAD KEY_DELAY_MS DISMISS_DIALOGS \
           WINDOW_PIN_XY LOGIN_ID_CLICK LOGIN_PW_CLICK LOGIN_BUTTON_CLICK \
           PIN_CLICK CHAR_SELECT_CLICK EXTRA_ENV; do
    printf '%-22s = %s\n' "$v" "${!v}"
  done
  printf '%-22s = %s\n' "YUFA_USER" "${YUFA_USER:+<set>}"
  printf '%-22s = %s\n' "YUFA_PASS" "${YUFA_PASS:+<set>}"
  printf '%-22s = %s\n' "PIN_CODE" "${PIN_CODE:+<set>}"
}

case "${1:-run}" in
  run)   cmd_run ;;
  login) cmd_login ;;
  shot)  shift; cmd_shot "${1:-}" ;;
  stop)  cmd_stop ;;
  env)   cmd_env ;;
  *) echo "usage: $0 {run|login|shot [file]|stop|env}" >&2; exit 2 ;;
esac
