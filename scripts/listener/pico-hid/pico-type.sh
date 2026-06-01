#!/usr/bin/env bash
# pico-type.sh — send the Projeto Yufa login macro to the Pico HID device, which
# types it as genuine USB HID. The password/PIN stay here on the host (read from
# credentials.env); the Pico firmware holds no secret.
#
# Usage:
#   pico-type.sh                 # send the login macro (password + PIN)
#   pico-type.sh '{ENTER}'       # send an arbitrary macro (debug)
#
# Env:
#   PICO_PORT         serial device (default: first /dev/ttyACM*)
#   CREDENTIALS_ENV   path to credentials.env (default: ~/.config/yufa-listener/credentials.env)
set -euo pipefail

PORT="${PICO_PORT:-}"
if [ -z "$PORT" ]; then
  PORT="$(ls /dev/ttyACM* 2>/dev/null | head -1 || true)"
fi
[ -n "$PORT" ] && [ -e "$PORT" ] || {
  echo "No Pico serial port found. Set PICO_PORT=/dev/ttyACMx (is it plugged in?)" >&2
  exit 1
}

# 115200 raw, no echo, to match the sketch. NEVER 1200 baud — a 1200-baud open
# triggers the RP2040 bootloader (mass-storage flash mode), not the sketch.
stty -F "$PORT" 115200 raw -echo -echoe -echok -echoctl -echoke 2>/dev/null || true

if [ "$#" -ge 1 ]; then
  macro="$1"                                   # explicit macro passed in
else
  CRED="${CREDENTIALS_ENV:-$HOME/.config/yufa-listener/credentials.env}"
  # shellcheck disable=SC1090
  [ -r "$CRED" ] && . "$CRED"
  : "${YUFA_PASS:?set YUFA_PASS in $CRED}"
  : "${YUFA_PIN:=}"
  case "$YUFA_PASS$YUFA_PIN" in
    *['{}']*) echo "Warning: password/PIN contains { or }, which the macro parser treats as a token." >&2 ;;
  esac
  # RO login: password field auto-focuses -> type pass, Enter -> PIN dialog -> type PIN, Enter.
  macro="{DELAY 400}${YUFA_PASS}{ENTER}"
  [ -n "$YUFA_PIN" ] && macro="${macro}{DELAY 1800}${YUFA_PIN}{ENTER}"
fi

printf '%s\n' "$macro" > "$PORT"
echo "Sent macro to $PORT (the Pico is typing it as HID)."
