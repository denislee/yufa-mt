---
name: verify-deploy
description: Verify a yufa-mt deploy on the live x230 box — service is up on a new PID, capabilities are intact, chat capture is flowing, and the systemd restart policy still backs the self-update path. Use over SSH to x230.
---

Verify the live yufa-mt deploy on x230 (`dns@x230.taild6ffd8.ts.net`). Run the
checks over SSH. The box uses password auth for `dns` (no authorized key from
this host); if a key prompt blocks, run the command yourself with the `!`
prefix in-session.

## Restart-policy check (gates the self-update path)

The self-update (`internal/server/selfupdate.go`) swaps the binary then
`SIGTERM`s itself, relying on systemd `Restart=always` to respawn onto the new
build — it never calls `systemctl`. So a deploy is only safe if the live unit
still carries that restart policy. Verify it directly against the running unit:

```sh
ssh dns@x230.taild6ffd8.ts.net \
  'systemctl show yufa-mt.service -p Restart -p RestartUSec -p ActiveState -p SubState -p MainPID -p FragmentPath'
```

Pass criteria:

- `Restart=always` and `RestartUSec=3s` — the respawn-on-SIGTERM contract holds.
  Anything else (`Restart=no`, `on-failure`, …) means a self-update would shut the
  server down and **not** bring it back — block the deploy and fix the unit first.
- `ActiveState=active` / `SubState=running` — service is up.
- `FragmentPath=/etc/systemd/system/yufa-mt.service` — the system unit, not a
  stray user/drop-in copy.
- After a deploy, `MainPID` should be a **new** value (the process actually
  restarted onto the new binary).

## Standard deploy checks (per README "Verifying a deploy")

```sh
systemctl status yufa-mt --no-pager                        # active (running), new PID
getcap /home/dns/git/yufa-mt/yufa-mt                       # cap_net_admin,cap_net_raw=eip
curl -fsS http://127.0.0.1:8080/health/zone                # last=<unix>  age=<sec>
journalctl -u yufa-mt --since "1 min ago" --no-pager \
  | grep -iE "packet capture|PIN proxy listening|Web server started"
```

- `getcap` must show `cap_net_admin,cap_net_raw=eip` — caps are wiped on every
  rebuild, so a missing result means `setcap` wasn't re-applied (chat capture +
  PIN proxy will be broken).
- `/health/zone` `age` is seconds since the last captured zone packet; low =
  client in-game and chat flowing. `age=-1` = no packet seen yet (still logging
  in), treat as unknown, not a stall.
- Startup log should show chat capture on `tcp port 6121`, `PIN proxy listening`
  on `127.0.0.1:7799`, and `Web server started`.
