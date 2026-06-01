#!/usr/bin/env bash
#
# watch-seed.sh — live-sniff the char server for the PIN keypad seed.
#
# When the character-select PIN dialog appears, the char server sends
# HC_SECOND_PASSWD_LOGIN (0x08b9) in cleartext:
#     b9 08 | seed(4 LE) | AID(4 LE) | state(2 LE)
# state 0x0001 = "enter PIN". We extract `seed` and (if PIN is set) print the
# slot-click sequence via pinpad.go. This needs NO Gepard bypass — char-server
# traffic is unencrypted on the wire.
#
# Usage:
#   sudo ./watch-seed.sh                       # just print seeds as they arrive
#   YUFA_PIN=1122 sudo ./watch-seed.sh         # also print the click sequence
#   CHAR_PORT=7121 IFACE=eth0 sudo ./watch-seed.sh
#
# Needs tshark (live capture => root or dumpcap capabilities) and Go (for pinpad).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/yufa-listener"
[ -f "$CONFIG_DIR/credentials.env" ] && . "$CONFIG_DIR/credentials.env"
[ -f "$CONFIG_DIR/config.env" ] && . "$CONFIG_DIR/config.env"

: "${CHAR_PORT:=7121}"                 # char server port (carries 0x08b9)
: "${PIN:=${YUFA_PIN:-}}"              # your real PIN (optional; enables solving)
: "${IFACE:=$(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1);exit}}')}"

command -v tshark >/dev/null 2>&1 || { echo "tshark required" >&2; exit 1; }
[ -n "$IFACE" ] || { echo "could not auto-detect IFACE; set IFACE=..." >&2; exit 1; }

echo "watching $IFACE tcp port $CHAR_PORT for PIN seed (0x08b9 state=1)…" >&2
[ -n "$PIN" ] && echo "PIN set → will print click slots" >&2 || echo "no PIN set → seeds only (set YUFA_PIN to solve)" >&2

# -l = line-buffered so we react immediately.
tshark -l -i "$IFACE" -f "tcp port $CHAR_PORT" -T fields -e tcp.payload 2>/dev/null \
| while read -r payload; do
    [ -z "$payload" ] && continue
    # Find every 0x08b9 record (12 bytes => 24 hex chars) in the payload.
    for rec in $(echo "$payload" | grep -oiE 'b908[0-9a-f]{20}'); do
      state="${rec:20:4}"
      [ "$state" = "0100" ] || continue        # only the "enter PIN" request
      seed_le="${rec:4:8}"                      # 4 seed bytes, on-wire (LE) order
      ts="$(date '+%H:%M:%S')"
      echo "[$ts] PIN seed (LE hex): $seed_le"
      if [ -n "$PIN" ]; then
        ( cd "$SCRIPT_DIR" && echo "$seed_le" | go run pinpad.go solve --pin "$PIN" )
      fi
    done
  done
