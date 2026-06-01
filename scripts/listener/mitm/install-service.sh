#!/usr/bin/env bash
#
# install-service.sh — RETIRED.
#
# The PIN-injection proxy now runs IN-PROCESS inside the yufa-mt app (see
# internal/server/pinproxy.go), started automatically by run-sniffer.sh when a
# PIN is configured (PIN_PROXY=1). The standalone root service this script used
# to install is no longer needed and would fight the in-app proxy over the same
# iptables REDIRECT and the 127.0.0.1:7799 listener.
#
# This script now only helps you REMOVE a previously-installed unit:
#   sudo ./install-service.sh uninstall
#
# To enable the in-app proxy instead:
#   1. put YUFA_PIN in ~/.config/yufa-listener/credentials.env
#   2. sudo setcap cap_net_raw,cap_net_admin=eip <repo>/yufa-mt   (cap_net_admin = iptables)
#   3. run-sniffer.sh launches yufa-mt with PIN_PROXY=1 automatically.
#
set -uo pipefail
UNIT=/etc/systemd/system/yufa-mitm-proxy.service

[ "$(id -u)" -eq 0 ] || { echo "run as root: sudo $0 uninstall" >&2; exit 1; }

if [ "${1:-}" = uninstall ]; then
  systemctl disable --now yufa-mitm-proxy.service 2>/dev/null
  rm -f "$UNIT"; systemctl daemon-reload
  echo "uninstalled yufa-mitm-proxy.service (its stop handler removed the iptables rules)."
  echo "the in-app proxy (PIN_PROXY=1 in run-sniffer.sh) now handles the PIN."
  exit 0
fi

cat >&2 <<'EOF'
This service is retired — the proxy now runs inside the yufa-mt app.

  • Enable it: set YUFA_PIN in credentials.env; run-sniffer.sh starts yufa-mt
    with PIN_PROXY=1 (the binary needs cap_net_admin via the documented setcap).
  • Remove a leftover unit from the old setup:  sudo ./install-service.sh uninstall

For a standalone/manual proxy test (not as a service), use ./run-proxy.sh.
EOF
exit 1
