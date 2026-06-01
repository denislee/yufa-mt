#!/usr/bin/env bash
#
# install-service.sh — install the MITM PIN-injection proxy as a root systemd
# service so it's always up (and survives reboots). The proxy needs root for
# iptables, so it can't be a user autostart; this is the supported way to make
# it persistent. Run with sudo.
#
#   sudo ./install-service.sh                 # SELECT_SLOT defaults to 0
#   sudo SELECT_SLOT=1 ./install-service.sh   # pick a different character slot
#   sudo ./install-service.sh uninstall
#
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT=/etc/systemd/system/yufa-mitm-proxy.service

[ "$(id -u)" -eq 0 ] || { echo "run as root: sudo $0" >&2; exit 1; }

if [ "${1:-}" = uninstall ]; then
  systemctl disable --now yufa-mitm-proxy.service 2>/dev/null
  rm -f "$UNIT"; systemctl daemon-reload
  echo "uninstalled yufa-mitm-proxy.service (iptables rules were removed by the service's stop handler)."
  exit 0
fi

: "${SELECT_SLOT:=0}"
: "${CHAR_PORT:=7121}"

# The proxy must read the *user's* PIN/config, not root's.
RUN_USER="${SUDO_USER:-root}"
USER_HOME="$(getent passwd "$RUN_USER" | cut -d: -f6)"
CONFIG_DIR="$USER_HOME/.config/yufa-listener"
STATE_DIR="$USER_HOME/.local/state/yufa-listener"
BIN="$STATE_DIR/yufa-mitm"

[ -f "$CONFIG_DIR/credentials.env" ] || { echo "missing $CONFIG_DIR/credentials.env (needs YUFA_PIN)" >&2; exit 1; }
command -v iptables >/dev/null 2>&1 || { echo "iptables required" >&2; exit 1; }

# Pre-build the binary as the user so the service never has to build as root
# (which would use root's empty Go module cache).
echo "building proxy as $RUN_USER -> $BIN"
mkdir -p "$STATE_DIR"; chown "$RUN_USER" "$STATE_DIR" 2>/dev/null || true
sudo -u "$RUN_USER" sh -c "cd '$SCRIPT_DIR' && go build -o '$BIN' proxy.go" \
  || { echo "build failed" >&2; exit 1; }

cat > "$UNIT" <<EOF
[Unit]
Description=Yufa MITM char-select PIN auto-injection proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
# CONFIG_DIR/STATE_DIR/BIN point run-proxy.sh at $RUN_USER's config + prebuilt binary.
Environment=CONFIG_DIR=$CONFIG_DIR
Environment=STATE_DIR=$STATE_DIR
Environment=BIN=$BIN
Environment=SELECT_SLOT=$SELECT_SLOT
Environment=CHAR_PORT=$CHAR_PORT
ExecStart=/bin/bash $SCRIPT_DIR/run-proxy.sh
Restart=on-failure
RestartSec=5
# run-proxy.sh removes its iptables rules in an EXIT/TERM trap.
KillSignal=SIGTERM
TimeoutStopSec=20

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now yufa-mitm-proxy.service
echo "installed + started yufa-mitm-proxy.service (SELECT_SLOT=$SELECT_SLOT, char port $CHAR_PORT)"
echo "logs: journalctl -u yufa-mitm-proxy -f      stop: systemctl stop yufa-mitm-proxy"
systemctl --no-pager status yufa-mitm-proxy.service 2>/dev/null | sed -n '1,8p'
