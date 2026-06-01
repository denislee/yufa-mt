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

## What's still needed to fully automate (and why it's now small)

The shuffle gives **slot index → which digit**. Two physical pieces remain:

1. **Slot → screen pixel (one-time calibration).** The 10 buttons sit at fixed
   screen positions; only their labels reshuffle. Calibrate once: launch the
   client, let `watch-seed.sh` print the seed + `tab`, screenshot the keypad
   (`grim`/`import`), and record the pixel center of each slot `0..9`. Confirm
   the model by checking the button at slot `i` actually shows `tab[i]` — if it
   does (expected), `solve` output indexes straight into your pixel table. This
   is the only step the captures can't give us (the keypad art is inside the
   Gepard-encrypted GRF, not loose Lua).

2. **Clicks the client accepts.** Synthetic clicks (ydotool `BTN_LEFT`, XTEST,
   `swaymsg cursor press`) are rejected — the client reads the mouse via
   DirectInput on the physical device. A genuine USB-HID **mouse** (the
   [../pico-hid/](../pico-hid/) RP2040) should pass where synthetic clicks fail.
   This remains the open hardware hypothesis, unchanged by this work.

### End-to-end loop (the design)
```
char-server 0x08b9 ──watch-seed.sh──▶ seed ──pinpad solve──▶ slots e.g. "5 5 2 2"
                                                                    │
                          calibrated slot→pixel table ◀────────────┘
                                       │
                          Pico HID mouse: move+click each pixel, then OK
```

`pico-hid/`'s README originally assumed a screenshot+OCR to read the shuffled
keypad each time. **That's no longer needed** — the seed makes the layout
deterministic, so the host just needs the static pixel table plus a Pico mouse
firmware (move-to-absolute + left-click), which is the remaining firmware to
write.

## Scope / honesty

This automates the PIN **only** for the legitimate, hardware-driven login of the
real client — it does **not** bypass Gepard or let a custom client log in (see
[../GEPARD-PROTOCOL-FINDINGS.md](../GEPARD-PROTOCOL-FINDINGS.md)). It removes the
last *manual* step from keeping one genuine client connected for the passive
chat sniffer.
