# pinpad — solving the character-select PIN keypad

The last manual step in an otherwise-automatable login (per
[../README.md](../README.md): keyboard auth + char-select already work via
ydotool) is the **randomized PIN keypad**, which needs mouse clicks the client
won't accept from synthetic input. This directory turns the *"which buttons"*
part from guesswork into a deterministic computation.

## The mechanism (reverse-engineered from `~/tmp/amigo/*.pcapng`)

Yufa uses the stock **rAthena secure-pincode** shuffle. Each time the PIN dialog
opens, the char server (port `7121`) sends `HC_SECOND_PASSWD_LOGIN` (`0x08b9`) in
**cleartext**:

```
b9 08 | seed(uint32 LE) | AID(uint32 LE) | state(uint16 LE)
         state 0x0001 = enter PIN     state 0x0000 = accepted
```

From `seed`, the client builds a digit permutation `tab[0..9]` (rAthena's
`char_pincode_decrypt`): **keypad slot `i` displays digit `tab[i]`**. You click
the slot showing each PIN digit; the client sends the *slot indices*, and the
server recovers `real = tab[slotSent]`. So the same PIN produces a different
button layout — and different `0x08b8` bytes — every session.

**Proof it's exactly this algorithm:** 15 captured logins of one account, each
with a different seed and different sent bytes, all decrypt to the same PIN
`9090` (`go run pinpad.go verify`). Because `seed` is on the wire in clear, we
can compute the layout with **no Gepard bypass and no screenshot/OCR**.

## Tools here

| File | Role |
|---|---|
| `pinpad.go` | Pure-stdlib solver. `verify` (self-test) and `solve --seed <s> --pin <d>` → the slot-click sequence. |
| `watch-seed.sh` | Live-sniffs char-server `0x08b9`, prints the seed (and, with `YUFA_PIN` set, the click slots) the instant the PIN dialog appears. |

```bash
go run pinpad.go verify                         # 15/15 must decrypt to 9090
echo dc000000 | go run pinpad.go solve --pin 9090   # seed as on-wire LE hex -> "4 5 4 5"
YUFA_PIN=1122 sudo ./watch-seed.sh              # live: prints click slots per session
```

> **Seed endianness footgun:** the four bytes after `b908` are little-endian.
> `dc 00 00 00` on the wire = seed **220**, not `0xdc00`. Feed the raw on-wire
> hex (`dc000000`) to `solve --seed` / stdin and it parses correctly; only pass
> `0x…` if you already byte-swapped.

## How the answer gets entered: the MITM proxy

The shuffle gives **slot index → which digit**; the only thing left is delivering
those slot digits to the server without a click. That is exactly what
[../mitm/](../mitm/) does — a transparent local proxy that, on the cleartext
`0x08b9` PIN prompt, runs this same shuffle math on the seed and writes the
`0x08b8` answer to the server itself, then relays the server's OK so the genuine
client closes its own keypad and proceeds to char-select → map → zone.

```
char-server 0x08b9 (seed) ──▶ mitm proxy ──pinpad shuffle──▶ inject 0x08b8 (slots)
                                   │
                  relay 0x08b9 state=0 (OK) ──▶ client dismisses keypad, enters world
```

No screenshot, no pixel calibration, no hardware mouse: because the server's
acceptance (`0x08b9 state=0`) is what dismisses the client's keypad, feeding it
that packet drives the client off the dialog without any pointer event. Synthetic
clicks (ydotool `BTN_LEFT`, XTEST, `swaymsg cursor press`) never worked — the
client reads the mouse via DirectInput on the physical device — which is why the
packet-injection route is the one that landed.

## Scope / honesty

This automates the PIN **only** for the legitimate, hardware-driven login of the
real client — it does **not** bypass Gepard or let a custom client log in (see
[../GEPARD-PROTOCOL-FINDINGS.md](../GEPARD-PROTOCOL-FINDINGS.md)). It removes the
last *manual* step from keeping one genuine client connected for the passive
chat sniffer.
