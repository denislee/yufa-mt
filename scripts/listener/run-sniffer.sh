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

[ -f "$CONFIG_DIR/config.env" ] && . "$CONFIG_DIR/config.env"

# Repo root = two levels up from scripts/listener (portable; no hardcoded path).
: "${YUFA_REPO:=$(cd "$SCRIPT_DIR/../.." && pwd)}"

# NIC carrying internet traffic = the default-route interface (auto-detected).
# Leave CHAT_CAPTURE_DEVICE empty to let yufa-mt auto-pick instead.
if [ -z "${CHAT_CAPTURE_DEVICE:-}" ]; then
  CHAT_CAPTURE_DEVICE="$(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')"
fi
: "${CHAT_CAPTURE_PORT:=6121}"          # game/zone server port (chat flows here)
: "${CHAT_CAPTURE_ONLY:=1}"             # chat capture only, no market scrapers

mkdir -p "$STATE_DIR"
cd "$YUFA_REPO" || { echo "repo not found: $YUFA_REPO" >&2; exit 1; }

[ -x ./yufa-mt ] || { echo "$(date '+%F %T') yufa-mt binary missing; build with 'make build'" >>"$LOG"; exit 1; }
if ! getcap ./yufa-mt 2>/dev/null | grep -q cap_net_raw; then
  echo "$(date '+%F %T') WARN: ./yufa-mt lacks cap_net_raw; packet capture will fail. Run: sudo setcap cap_net_raw,cap_net_admin=eip $YUFA_REPO/yufa-mt" >>"$LOG"
fi

export CHAT_CAPTURE_DEVICE CHAT_CAPTURE_PORT CHAT_CAPTURE_ONLY

trap 'echo "$(date "+%F %T") sniffer wrapper stopping" >>"$LOG"; exit 0' INT TERM

while true; do
  echo "$(date '+%F %T') starting yufa-mt sniffer (repo=$YUFA_REPO dev=${CHAT_CAPTURE_DEVICE:-auto} port=$CHAT_CAPTURE_PORT chat_only=$CHAT_CAPTURE_ONLY)" >>"$LOG"
  ./yufa-mt >>"$LOG" 2>&1
  echo "$(date '+%F %T') yufa-mt exited (code $?); restarting in 15s" >>"$LOG"
  sleep 15
done
