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

## Use

```bash
# PIN from env or ~/.config/yufa-listener/credentials.env (YUFA_PIN)
sudo PIN=1122 scripts/listener/mitm/run-proxy.sh
```

Then launch / relogin the client (`yufa-listener.sh run`). When it reaches the
PIN dialog, the proxy injects it automatically; you'll see a log line like:

```
[1] AID = 2008669 (0x1ea65d)
[1] PIN injected: seed=220 (0xdc) -> slots "4545" (0x08b8 b8085da61e0034353435)
```

`Ctrl+C` removes the iptables rules (always, via an EXIT trap).

### Config (env or `config.env`)
| Var | Default | Meaning |
|---|---|---|
| `PIN` / `YUFA_PIN` | — | your real PIN (required to inject) |
| `CHAR_PORT` | `7121` | char-server port carrying `0x08b9`/`0x08b8` |
| `PROXY_PORT` | `7799` | local port the redirect delivers to |
| `SERVER_IP` | auto/any | narrow the redirect to one server IP (auto-detected from a live connection if possible) |
| `INJECT` | `1` | `0` = pure transparent relay (debug/capture, no injection) |

## Test without the game

```bash
go run proxy.go selftest      # 15 captured sessions reproduce their sent slots
```
A loopback functional test (fake char server + client, fixed `UPSTREAM`) lives in
the commit history of this work; set `UPSTREAM=ip:port` to run the proxy against
any fixed target without iptables.

## The one thing to confirm live

This relies on the client closing the keypad when it receives an *unsolicited*
`0x08b9 state=0` (it didn't itself send `0x08b8`). That's the expected behavior —
the handler switches on the state field — but it hasn't been verified against
this exact client build. First run: watch whether the client advances to
char-select after the "PIN injected" log line.

- **If it advances** → done; login is now fully unattended (keyboard via ydotool
  + this proxy for the PIN). No Pico needed.
- **If it doesn't** (ignores the OK because no local click happened) → fall back
  to the [../pico-hid/](../pico-hid/) mouse, using `../pinpad/` to know which
  slots to click. The proxy is still useful as `INJECT=0` for live capture.

## Scope / honesty

Removes a manual click, nothing more. It still requires the genuine client (for
the Gepard-gated login and the zone handshake) and the passive sniffer for chat.
It does not enable a custom/headless client — that remains blocked by Gepard.
