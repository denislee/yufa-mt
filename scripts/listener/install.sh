#!/usr/bin/env bash
#
# One-time setup for the headless Projeto Yufa chat listener:
#   - checks dependencies
#   - sets ROExt AutoFreeCPU=1 in the client's dinput.ini (idles CPU headless)
#   - scaffolds ~/.config/yufa-listener/{config.env,credentials.env}
#   - installs the systemd --user service
#
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/yufa-listener"
GAME_DIR="${GAME_DIR:-/home/dns/Downloads/Projeto Yufa}"

echo "==> Checking dependencies"
missing=()
for c in Xvfb import; do command -v "$c" >/dev/null 2>&1 || missing+=("$c"); done
command -v xdotool >/dev/null 2>&1 || missing+=("xdotool (sudo pacman -S xdotool)")
UMU="${UMU_RUN:-$HOME/.local/share/lutris/runtime/umu/umu-run}"
[ -x "$UMU" ] || missing+=("umu-run (expected at $UMU)")
command -v lutris >/dev/null 2>&1 || echo "   note: lutris not found (only needed for the one-time saved-login step)"
if [ "${#missing[@]}" -gt 0 ]; then
  printf '   MISSING: %s\n' "${missing[@]}"
  echo "   Install the missing packages, then re-run this script."
  echo "   (xdotool is required: it injects keystrokes via XTEST, which Xvfb accepts;"
  echo "    your existing ydotool cannot reach an Xvfb display.)"
  exit 1
fi
echo "   all present."

echo "==> Enabling ROExt AutoFreeCPU (releases CPU while the window is inactive)"
DINPUT="$GAME_DIR/dinput.ini"
if [ -f "$DINPUT" ]; then
  if grep -q '^AutoFreeCPU' "$DINPUT"; then
    cur="$(grep -m1 '^AutoFreeCPU' "$DINPUT")"
    if echo "$cur" | grep -q '=[[:space:]]*1'; then
      echo "   already enabled."
    else
      cp -n "$DINPUT" "$DINPUT.bak"
      sed -i 's/^AutoFreeCPU[[:space:]]*=.*/AutoFreeCPU =1/' "$DINPUT"
      echo "   set AutoFreeCPU =1 (backup: $DINPUT.bak)"
    fi
  else
    echo "   WARN: no AutoFreeCPU line in $DINPUT; leaving as-is."
  fi
else
  echo "   WARN: $DINPUT not found; skipping (set GAME_DIR=... to point at the client)."
fi

echo "==> Scaffolding $CONFIG_DIR"
install -m700 -d "$CONFIG_DIR"
[ -f "$CONFIG_DIR/config.env" ]      || install -m600 "$HERE/config.env.example"      "$CONFIG_DIR/config.env"
if [ ! -f "$CONFIG_DIR/credentials.env" ]; then
  install -m600 "$HERE/credentials.env.example" "$CONFIG_DIR/credentials.env"
  echo "   >>> EDIT $CONFIG_DIR/credentials.env and set YUFA_USER / YUFA_PASS <<<"
fi

echo "==> Installing systemd --user service"
install -m644 -D "$HERE/yufa-listener.service" "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/yufa-listener.service"
systemctl --user daemon-reload
echo "   installed. Enable with:"
echo "     systemctl --user enable --now yufa-listener.service"
echo "     loginctl enable-linger \"$USER\"   # survive logout"
echo
echo "Done. First, calibrate the login once (see README), then enable the service."
