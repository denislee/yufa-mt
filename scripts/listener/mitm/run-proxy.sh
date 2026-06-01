#!/usr/bin/env bash
#
# run-proxy.sh — set up the transparent redirect and run the MITM PIN proxy.
#
# Redirects the client's char-server connection (port 7121) to a local proxy
# (proxy.go) that auto-injects the character-select PIN, so login needs no mouse
# click on the randomized keypad. Login (7900) and zone (6121) are untouched, so
# the client still performs its own Gepard handshakes. See README.md.
#
# MUST run as root: it edits iptables and runs the proxy as root so the proxy's
# own onward connection (also root) is excluded from the redirect (no loop).
# The game client must run as a NORMAL user (not root) for the redirect to catch
# it — which is the usual case under lutris/wine.
#
# Usage:
#   sudo PIN=1122 ./run-proxy.sh            # or put YUFA_PIN in credentials.env
#   sudo ./run-proxy.sh                     # INJECT=0-style dry run if no PIN (relay only)
#   sudo SERVER_IP=99.83.172.22 ./run-proxy.sh
#
# Stop with Ctrl+C — iptables rules are always removed on exit.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/yufa-listener"
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/yufa-listener"
# When run via sudo, $HOME is root's; prefer the invoking user's config.
if [ -n "${SUDO_USER:-}" ]; then
  USER_HOME="$(getent passwd "$SUDO_USER" | cut -d: -f6)"
  CONFIG_DIR="${USER_HOME}/.config/yufa-listener"
  STATE_DIR="${USER_HOME}/.local/state/yufa-listener"
fi
[ -f "$CONFIG_DIR/credentials.env" ] && . "$CONFIG_DIR/credentials.env"
[ -f "$CONFIG_DIR/config.env" ] && . "$CONFIG_DIR/config.env"

: "${PROXY_PORT:=7799}"          # local port REDIRECT delivers char traffic to
: "${CHAR_PORT:=7121}"           # char-server port (carries the cleartext PIN packets)
: "${SERVER_IP:=}"               # optional: narrow the redirect to one server IP
: "${PIN:=${YUFA_PIN:-}}"        # your real PIN (enables injection)
: "${INJECT:=1}"
: "${SELECT_SLOT:=}"             # also auto-pick this character slot after the PIN (empty = don't)
: "${BIN:=$STATE_DIR/yufa-mitm}"

[ "$(id -u)" -eq 0 ] || { echo "must run as root (iptables): sudo $0" >&2; exit 1; }
command -v iptables >/dev/null 2>&1 || { echo "iptables required" >&2; exit 1; }

if [ "$INJECT" = 1 ] && [ -z "$PIN" ]; then
  echo "no PIN set (export PIN=... or YUFA_PIN in credentials.env). Use INJECT=0 for a relay-only test." >&2
  exit 1
fi

# --- Build the proxy (as the invoking user so it uses their Go module cache) ---
mkdir -p "$STATE_DIR"
if [ ! -x "$BIN" ] || [ "$SCRIPT_DIR/proxy.go" -nt "$BIN" ]; then
  echo "building proxy -> $BIN"
  if [ -n "${SUDO_USER:-}" ]; then
    sudo -u "$SUDO_USER" sh -c "cd '$SCRIPT_DIR' && go build -o '$BIN' proxy.go" \
      || { echo "build failed" >&2; exit 1; }
  else
    ( cd "$SCRIPT_DIR" && go build -o "$BIN" proxy.go ) || { echo "build failed" >&2; exit 1; }
  fi
fi

# Try to discover the server IP from a live char connection if not given (nicer
# logging + a tighter rule). Falls back to matching the port on any destination.
if [ -z "$SERVER_IP" ]; then
  SERVER_IP="$(ss -tn 2>/dev/null | awk -v p=":$CHAR_PORT" '$0 ~ p {split($5,a,":"); print a[1]; exit}')"
fi

DST_MATCH=()
[ -n "$SERVER_IP" ] && DST_MATCH=(-d "$SERVER_IP")
echo "char port=$CHAR_PORT  proxy=127.0.0.1:$PROXY_PORT  server_ip=${SERVER_IP:-<any>}  inject=$INJECT  select_slot=${SELECT_SLOT:-<off>}"

# --- iptables: exclude our own (root) traffic, then redirect the client's -----
add_rules() {
  iptables -t nat -A OUTPUT -p tcp --dport "$CHAR_PORT" -m owner --uid-owner 0 -j RETURN
  iptables -t nat -A OUTPUT -p tcp "${DST_MATCH[@]}" --dport "$CHAR_PORT" -j REDIRECT --to-ports "$PROXY_PORT"
}
del_rules() {
  iptables -t nat -D OUTPUT -p tcp --dport "$CHAR_PORT" -m owner --uid-owner 0 -j RETURN 2>/dev/null
  iptables -t nat -D OUTPUT -p tcp "${DST_MATCH[@]}" --dport "$CHAR_PORT" -j REDIRECT --to-ports "$PROXY_PORT" 2>/dev/null
}

cleanup() {
  echo; echo "removing iptables rules…"
  del_rules
  [ -n "${PROXY_PID:-}" ] && kill "$PROXY_PID" 2>/dev/null
}
trap cleanup EXIT INT TERM

del_rules            # clear any stale copies from a previous crash
add_rules
echo "redirect installed. Launch/relogin the client now; the PIN will auto-enter."

LISTEN_ADDR="127.0.0.1:$PROXY_PORT" CHAR_PORT="$CHAR_PORT" PIN="$PIN" INJECT="$INJECT" \
  SELECT_SLOT="$SELECT_SLOT" "$BIN" &
PROXY_PID=$!
wait "$PROXY_PID"
