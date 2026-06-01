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

# --- Defaults (override in ~/.config/yufa-listener/config.env) ---------------
: "${DISPLAY_NUM:=:99}"                 # virtual display to create
: "${SCREEN_GEOMETRY:=640x480x16}"      # Xvfb -screen 0 geometry (low = light)
: "${LUTRIS_GAME_ID:=4}"                # `lutris -l -j` id (4 = ragnarok-online-1, wine_prefix_2)
: "${GAME_PROC:=Projeto_Yufa.exe}"      # process name to track for liveness
: "${WINDOW_NAME:=Ragnarok}"            # xdotool --name regex for the client window
: "${GAME_DIR:=/home/dns/Downloads/Projeto Yufa}"

# Login timing knobs (seconds). Tune with the `login`/`shot` subcommands.
: "${WAIT_FOR_WINDOW:=180}"             # max time to wait for the window to appear
: "${SETTLE_AFTER_WINDOW:=45}"          # Gepard init + reach the login screen
: "${WAIT_AFTER_LOGIN:=12}"             # login server -> server select
: "${WAIT_BEFORE_CHARSELECT:=6}"        # server select -> character select
: "${WAIT_MAP_LOAD:=25}"                # character select -> in-game/map loaded
: "${KEY_DELAY_MS:=60}"                 # per-keystroke delay while typing

# Optional mouse clicks (set "X,Y" to enable). Most classic clients focus the
# ID field on launch and accept Enter on server/char select, so these are off
# by default. Use the calibration screenshot to find coordinates if needed.
: "${LOGIN_FIELD_CLICK:=}"              # e.g. 320,210  (click ID field before typing)
: "${CHAR_SELECT_CLICK:=}"             # e.g. 110,180  (click the character slot)

# Extra environment passed to the Lutris launch (rendering fallbacks, etc.).
# If the client fails to render under Xvfb, try: EXTRA_ENV="PROTON_USE_WINED3D=1"
: "${EXTRA_ENV:=}"

# --- Paths -------------------------------------------------------------------
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/yufa-listener"
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/yufa-listener"
LOG_FILE="$STATE_DIR/listener.log"
XVFB_PID_FILE="$STATE_DIR/xvfb.pid"

[ -f "$CONFIG_DIR/config.env" ]      && . "$CONFIG_DIR/config.env"
[ -f "$CONFIG_DIR/credentials.env" ] && . "$CONFIG_DIR/credentials.env"

mkdir -p "$STATE_DIR"

log() { printf '%s [listener] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE" >&2; }
die() { log "FATAL: $*"; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1 (see scripts/listener/README.md)"; }

# --- Xvfb lifecycle ----------------------------------------------------------
xvfb_running() {
  local n="${DISPLAY_NUM#:}"
  [ -e "/tmp/.X${n}-lock" ] && return 0
  return 1
}

start_xvfb() {
  if xvfb_running; then
    log "Xvfb already present on $DISPLAY_NUM, reusing it."
    return 0
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

# --- Client lifecycle --------------------------------------------------------
game_pid() { pgrep -f "$GAME_PROC" | head -1; }

launch_client() {
  need lutris
  log "Launching client via Lutris (game id $LUTRIS_GAME_ID) on $DISPLAY_NUM"
  # shellcheck disable=SC2086
  env DISPLAY="$DISPLAY_NUM" $EXTRA_ENV \
      lutris "lutris:rungameid/$LUTRIS_GAME_ID" >>"$LOG_FILE" 2>&1 &
  LUTRIS_PID=$!
  log "Lutris launcher pid $LUTRIS_PID; waiting up to ${WAIT_FOR_WINDOW}s for '$GAME_PROC'"
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

auto_login() {
  need xdotool
  [ -n "${YUFA_USER:-}" ] || die "YUFA_USER not set (put it in $CONFIG_DIR/credentials.env)"
  [ -n "${YUFA_PASS:-}" ] || die "YUFA_PASS not set (put it in $CONFIG_DIR/credentials.env)"

  log "Waiting ${SETTLE_AFTER_WINDOW}s for Gepard init + login screen"
  sleep "$SETTLE_AFTER_WINDOW"

  local wid; wid="$(find_window)"
  if [ -n "$wid" ]; then
    log "Focusing client window $wid"
    xdo windowactivate --sync "$wid" 2>/dev/null
    xdo windowfocus "$wid" 2>/dev/null
    # a click in the window guarantees Wine gives it input focus (no WM running)
    xdo windowsize "$wid" >/dev/null 2>&1
    local geo w h; geo="$(xdo getdisplaygeometry)"; w="${geo% *}"; h="${geo#* }"
    xdo mousemove --sync $((w/2)) $((h/2)); xdo click 1; sleep 0.5
  else
    log "WARN: no window found by name; typing blind into the focused surface"
  fi

  click_at "$LOGIN_FIELD_CLICK"

  log "Entering credentials for user '$YUFA_USER'"
  xdo type --clearmodifiers --delay "$KEY_DELAY_MS" "$YUFA_USER"
  xdo key --clearmodifiers Tab
  xdo type --clearmodifiers --delay "$KEY_DELAY_MS" "$YUFA_PASS"
  xdo key --clearmodifiers Return

  log "Login sent; waiting ${WAIT_AFTER_LOGIN}s for server select"
  sleep "$WAIT_AFTER_LOGIN"
  xdo key --clearmodifiers Return            # accept service/server

  log "Waiting ${WAIT_BEFORE_CHARSELECT}s for character select"
  sleep "$WAIT_BEFORE_CHARSELECT"
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
  [ -n "${LUTRIS_PID:-}" ] && kill "$LUTRIS_PID" 2>/dev/null
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
  start_xvfb
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
  for v in DISPLAY_NUM SCREEN_GEOMETRY LUTRIS_GAME_ID GAME_PROC WINDOW_NAME GAME_DIR \
           WAIT_FOR_WINDOW SETTLE_AFTER_WINDOW WAIT_AFTER_LOGIN WAIT_BEFORE_CHARSELECT \
           WAIT_MAP_LOAD KEY_DELAY_MS LOGIN_FIELD_CLICK CHAR_SELECT_CLICK EXTRA_ENV; do
    printf '%-22s = %s\n' "$v" "${!v}"
  done
  printf '%-22s = %s\n' "YUFA_USER" "${YUFA_USER:+<set>}"
  printf '%-22s = %s\n' "YUFA_PASS" "${YUFA_PASS:+<set>}"
}

case "${1:-run}" in
  run)   cmd_run ;;
  login) cmd_login ;;
  shot)  shift; cmd_shot "${1:-}" ;;
  stop)  cmd_stop ;;
  env)   cmd_env ;;
  *) echo "usage: $0 {run|login|shot [file]|stop|env}" >&2; exit 2 ;;
esac
