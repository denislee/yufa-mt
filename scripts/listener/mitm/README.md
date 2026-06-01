# yufa-mitm — auto-enter the PIN with a crafted packet (no click, no Gepard bypass)

> **Now built into the app.** This proxy runs **in-process inside `yufa-mt`**
> (see `internal/server/pinproxy.go`), started automatically by `run-sniffer.sh`
> when a PIN is configured (`PIN_PROXY=1`). So the proxy is already up by the
> time the listener logs the client in — no separate service to start. The files
> here remain the reference implementation and a **standalone test harness**
> (`run-proxy.sh`, `proxy.go selftest`); the old root systemd service
> (`install-service.sh`) is **retired** (see below). The protocol explanation in
> this doc applies verbatim to the in-app version.

A transparent local TCP proxy that removes the **last manual step** of keeping a
genuine client connected for the chat sniffer: the randomized character-select
PIN keypad. Instead of clicking the keypad (which the client only accepts from
real hardware input — see [../pinpad/](../pinpad/)), the proxy injects the PIN
packet for the client.

## Why this works (and raw injection doesn't)

The char server (port `7121`) is **plaintext** (see
[../GEPARD-PROTOCOL-FINDINGS.md](../GEPARD-PROTOCOL-FINDINGS.md)). When the PIN
dialog opens it sends, in the clear:

```
HC_SECOND_PASSWD_LOGIN  0x08b9 : b9 08 | seed(u32 LE) | AID(u32 LE) | state(u16 LE)   (state 1 = enter)
```

The client would shuffle the keypad from `seed`, you'd click, and it would send:

```
CH_SECOND_PASSWD_ACK    0x08b8 : b8 08 | AID(u32 LE) | <4 ascii slot digits>
```

The proxy sits in the middle of the char connection and **relays every byte
untouched**, except: on the first `0x08b9 state=1` it reads `seed`, computes the
slot digits for your real PIN (the rAthena pincode shuffle — the same math as
`../pinpad/`, self-tested), and writes the `0x08b8` to the server itself. The
server replies `0x08b9 state=0` (OK), the proxy relays that to the client, and
the client closes the keypad and proceeds to char-select → map → zone on its own.

Key properties:
- **TCP stays consistent.** Because the proxy terminates both sides, there's no
  sequence-number desync (the reason raw socket injection breaks the connection).
- **Gepard sees nothing odd.** The injected `0x08b8` never goes through the
  client's own `send()`; Gepard hooks the *client's* socket, so it only ever sees
  a normal inbound `0x08b9`. Login (`7900`) and zone (`6121`) are **not** proxied,
  so the client performs its own Gepard `0x4753`/`0xc392` handshakes directly.
- **No Gepard bypass.** This does not log in by itself or decrypt anything — it
  only automates the one cleartext PIN exchange of the real, genuine client.

## Requirements

- Linux with `iptables` (nat table).
- **Privilege to edit iptables.** The standalone `run-proxy.sh` runs as **root**
  and skips its own (root-owned) upstream traffic via `-m owner --uid-owner 0`.
  The **in-app** version instead runs as your normal user with `cap_net_admin`
  (the documented `setcap` on the `yufa-mt` binary) and skips its own upstream
  traffic by tagging it with an **fwmark** (`SO_MARK`) and `RETURN`-ing on
  `-m mark` — because in-process it shares your uid with the game client, so the
  uid trick can't tell them apart. Same effect, no redirect loop, no root.

## Status — verified live ✓

Confirmed end-to-end against the real client and server: the proxy injected the
PIN **and** the char-select, and the client advanced all the way into the zone
(`:6121`, in-game) with **no clicks**. The slot digits were accepted by the live
server, validating the shuffle against a real account (not just the captures).
No hardware mouse needed.

## Use

```bash
# PIN from env or ~/.config/yufa-listener/credentials.env (YUFA_PIN).
# SELECT_SLOT=0 also auto-picks the first character after the PIN.
sudo SELECT_SLOT=0 scripts/listener/mitm/run-proxy.sh
```

Then launch / relogin the client (`yufa-listener.sh run`), type the password, and
reach the PIN screen — the proxy does the rest. Log lines:

```
[1] AID = 2009572 (0x1ea9e4)
[1] PIN injected: seed=60253 (0xeb5d) -> slots "3388" (0x08b8 b808e4a91e0033333838)
[1] PIN accepted (state=0); char-select injected: slot 0 (0x0066 660000)
```

`Ctrl+C` removes the iptables rules (always, via an EXIT trap).

### Config (env or `config.env`)
| Var | Default | Meaning |
|---|---|---|
| `PIN` / `YUFA_PIN` | — | your real PIN (required to inject) |
| `SELECT_SLOT` | off | also inject `CH_SELECT_CHAR` for this slot after the PIN (`0` = first char) |
| `CHAR_PORT` | `7121` | char-server port carrying `0x08b9`/`0x08b8` |
| `PROXY_PORT` | `7799` | local port the redirect delivers to |
| `SERVER_IP` | auto/any | narrow the redirect to one server IP (auto-detected from a live connection if possible) |
| `INJECT` | `1` | `0` = pure transparent relay (debug/capture, no injection) |

## Run it persistently — use the in-app proxy

The proxy is part of `yufa-mt`, so it's persistent for free: whatever keeps the
sniffer running (the `~/.config/autostart/yufa-sniffer.desktop` entry / your
service) brings the proxy up with it. To enable it:

```bash
# 1. PIN in credentials.env
echo 'YUFA_PIN="1122"' >> ~/.config/yufa-listener/credentials.env

# 2. grant the binary iptables + capture capabilities (one time, after each rebuild)
sudo setcap cap_net_raw,cap_net_admin=eip <repo>/yufa-mt

# 3. run-sniffer.sh launches yufa-mt with PIN_PROXY=1 automatically.
scripts/listener/run-sniffer.sh
```

Tune it via `config.env` (read by `run-sniffer.sh`): `PIN_PROXY` (`0` to disable),
`PIN_PROXY_SELECT_SLOT` (default `0`; empty = PIN only, no char-select),
`PIN_PROXY_CHAR_PORT`, `PIN_PROXY_LISTEN_PORT`, `PIN_PROXY_SERVER_IP`,
`PIN_PROXY_INJECT`. Logs go to yufa-mt's own log (`sniffer.log`).

The old **root systemd service is retired** — `install-service.sh` now only
removes a leftover unit:

```bash
sudo scripts/listener/mitm/install-service.sh uninstall   # remove an old unit
```

Don't run the in-app proxy and a standalone `run-proxy.sh` at the same time —
they collide on the same iptables REDIRECT and the `127.0.0.1:7799` listener.

## Full hands-free pipeline

1. **Sniffer + proxy** — `run-sniffer.sh` starts `yufa-mt`, which captures
   cleartext chat on `:6121` **and** runs the in-app PIN proxy (`PIN_PROXY=1`),
   installing the char-port REDIRECT on startup.
2. **Client login** — `AUTO_LOGIN=1 yufa-listener.sh run` types the password via
   `ydotool` and clears the startup dialogs; the proxy handles PIN + char-select.
   (Needs `ydotoold` running as root; calibrate `AUTO_LOGIN_*` timings on first run.)

## Test without the game

```bash
go run proxy.go selftest      # 15 captured sessions reproduce their sent slots + builders
```
Set `UPSTREAM=ip:port` to run the proxy against any fixed target without iptables.

## Scope / honesty

Removes a manual click, nothing more. It still requires the genuine client (for
the Gepard-gated login and the zone handshake) and the passive sniffer for chat.
It does not enable a custom/headless client — that remains blocked by Gepard.
