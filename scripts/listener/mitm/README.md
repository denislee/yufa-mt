# yufa-mitm — auto-enter the PIN with a crafted packet (no click, no Gepard bypass)

A transparent local TCP proxy that removes the **last manual step** of keeping a
genuine client connected for the chat sniffer: the randomized character-select
PIN keypad. Instead of clicking the keypad (which needs real hardware input — see
[../pinpad/](../pinpad/) and [../pico-hid/](../pico-hid/)), the proxy injects the
PIN packet for the client.

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

- Linux with `iptables` (nat table) and Go (to build the proxy once).
- The game client running as a **normal user** (the usual lutris/wine case) — the
  redirect deliberately skips root-owned traffic so the proxy's own upstream
  connection isn't redirected back into itself.

## Status — verified live ✓

Confirmed end-to-end against the real client and server: the proxy injected the
PIN **and** the char-select, and the client advanced all the way into the zone
(`:6121`, in-game) with **no clicks**. The slot digits were accepted by the live
server, validating the shuffle against a real account (not just the captures).
The Pico mouse is no longer needed.

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

## Run as a persistent service (survives reboots)

The proxy needs root (iptables), so it can't be a user autostart. Install it as a
root systemd service instead:

```bash
sudo SELECT_SLOT=0 scripts/listener/mitm/install-service.sh   # build + enable + start
sudo scripts/listener/mitm/install-service.sh uninstall       # remove
journalctl -u yufa-mitm-proxy -f                              # logs
```

It builds the binary as your user, writes `/etc/systemd/system/yufa-mitm-proxy.service`
(pointing at your `credentials.env`), and enables it. Use **either** the service
**or** a manual `run-proxy.sh` — not both (they'd add duplicate iptables rules).

## Full hands-free pipeline

1. **Proxy** — the service above (or `run-proxy.sh`), always up.
2. **Client login** — `AUTO_LOGIN=1 yufa-listener.sh run` types the password via
   `ydotool` and clears the startup dialogs; the proxy handles PIN + char-select.
   (Needs `ydotoold` running as root; calibrate `AUTO_LOGIN_*` timings on first run.)
3. **Sniffer** — `run-sniffer.sh` captures cleartext chat on `:6121` into the DB.

## Test without the game

```bash
go run proxy.go selftest      # 15 captured sessions reproduce their sent slots + builders
```
Set `UPSTREAM=ip:port` to run the proxy against any fixed target without iptables.

## Scope / honesty

Removes a manual click, nothing more. It still requires the genuine client (for
the Gepard-gated login and the zone handshake) and the passive sniffer for chat.
It does not enable a custom/headless client — that remains blocked by Gepard.
