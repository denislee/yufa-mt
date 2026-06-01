#!/usr/bin/env bash
#
# run-sniffer.sh — run the yufa-mt chat sniffer (chat-only mode) in a
# keep-alive loop. Pairs with yufa-listener.sh (which keeps a logged-in client
# connected). The binary must have CAP_NET_RAW:
#   sudo setcap cap_net_raw,cap_net_admin=eip <repo>/yufa-mt
#
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/yufa-listener"
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/yufa-listener"
LOG="$STATE_DIR/sniffer.log"

[ -f "$CONFIG_DIR/config.env" ]      && . "$CONFIG_DIR/config.env"
[ -f "$CONFIG_DIR/credentials.env" ] && . "$CONFIG_DIR/credentials.env"  # for YUFA_PIN

# Repo root = two levels up from scripts/listener (portable; no hardcoded path).
: "${YUFA_REPO:=$(cd "$SCRIPT_DIR/../.." && pwd)}"

# NIC carrying internet traffic = the default-route interface (auto-detected).
# Leave CHAT_CAPTURE_DEVICE empty to let yufa-mt auto-pick instead.
if [ -z "${CHAT_CAPTURE_DEVICE:-}" ]; then
  CHAT_CAPTURE_DEVICE="$(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')"
fi
: "${CHAT_CAPTURE_PORT:=6121}"          # game/zone server port (chat flows here)
: "${CHAT_CAPTURE_ONLY:=1}"             # chat capture only, no market scrapers

# --- In-app character-select PIN proxy ---------------------------------------
# yufa-mt now runs the PIN-injection proxy in-process (replacing the old root
# yufa-mitm-proxy.service): on startup it installs an iptables REDIRECT of the
# char-server port to a local listener and auto-enters the PIN, so the client
# clears the randomized keypad with no mouse click. Auto-enable it when a PIN
# is configured; set PIN_PROXY=0 in config.env to opt out.
if [ -z "${PIN_PROXY:-}" ] && [ -n "${YUFA_PIN:-}" ]; then
  PIN_PROXY=1
fi
: "${PIN_PROXY:=0}"
: "${PIN_PROXY_CHAR_PORT:=7121}"        # char server port (carries the cleartext PIN packets)
: "${PIN_PROXY_LISTEN_PORT:=7799}"      # local port the REDIRECT delivers to
: "${PIN_PROXY_SELECT_SLOT:=0}"         # also auto-pick this character slot (set empty to disable)
: "${PIN_PROXY_INJECT:=1}"
: "${PIN_PROXY_SERVER_IP:=}"            # optional: narrow the redirect to one server IP

mkdir -p "$STATE_DIR"
cd "$YUFA_REPO" || { echo "repo not found: $YUFA_REPO" >&2; exit 1; }

[ -x ./yufa-mt ] || { echo "$(date '+%F %T') yufa-mt binary missing; build with 'make build'" >>"$LOG"; exit 1; }
_caps="$(getcap ./yufa-mt 2>/dev/null)"
case "$_caps" in
  *cap_net_raw*) ;;
  *) echo "$(date '+%F %T') WARN: ./yufa-mt lacks cap_net_raw; packet capture will fail. Run: sudo setcap cap_net_raw,cap_net_admin=eip $YUFA_REPO/yufa-mt" >>"$LOG" ;;
esac
# The in-app PIN proxy edits iptables, which needs cap_net_admin (not root).
if [ "$PIN_PROXY" = 1 ]; then
  case "$_caps" in
    *cap_net_admin*) ;;
    *) echo "$(date '+%F %T') WARN: PIN_PROXY=1 but ./yufa-mt lacks cap_net_admin; the iptables REDIRECT will fail. Run: sudo setcap cap_net_raw,cap_net_admin=eip $YUFA_REPO/yufa-mt" >>"$LOG" ;;
  esac
fi

export CHAT_CAPTURE_DEVICE CHAT_CAPTURE_PORT CHAT_CAPTURE_ONLY
export PIN_PROXY PIN_PROXY_CHAR_PORT PIN_PROXY_LISTEN_PORT PIN_PROXY_SELECT_SLOT PIN_PROXY_INJECT PIN_PROXY_SERVER_IP YUFA_PIN

trap 'echo "$(date "+%F %T") sniffer wrapper stopping" >>"$LOG"; exit 0' INT TERM

while true; do
  echo "$(date '+%F %T') starting yufa-mt sniffer (repo=$YUFA_REPO dev=${CHAT_CAPTURE_DEVICE:-auto} port=$CHAT_CAPTURE_PORT chat_only=$CHAT_CAPTURE_ONLY pin_proxy=$PIN_PROXY)" >>"$LOG"
  ./yufa-mt >>"$LOG" 2>&1
  echo "$(date '+%F %T') yufa-mt exited (code $?); restarting in 15s" >>"$LOG"
  sleep 15
done
