# Pico HID login typer (prototype)

A Raspberry Pi Pico (RP2040) acting as a **genuine USB HID keyboard** that types
the Projeto Yufa login on demand. Goal: anti-detection. The headless findings in
[../README.md](../README.md) point at Gepard Shield rejecting the connection most
likely because it detects `ydotoold`'s virtual `uinput` device. A Pico isn't
spoofing a keyboard — it *is* one, enumerated over USB — so keystrokes are
indistinguishable from real hardware at the libinput/Xwayland/client layer.

> **This is a prototype / hypothesis test.** The ydotoold-detection theory is the
> README's "most likely," not confirmed. If Gepard rejects on some *other* signal
> (the virtual mouse, the headless display, a network heuristic), real HID input
> alone won't get you in. Try it and see whether the handshake survives.

## Files

| File | Role |
|---|---|
| `yufa-hid.ino` | Pico firmware. Composite HID keyboard + USB serial. Holds no secret. |
| `pico-type.sh` | Host trigger. Reads `credentials.env`, sends the login macro over serial. |

The password/PIN live **only on the host** (your existing `credentials.env`). The
Pico is a dumb relay: the host sends a macro string, the Pico emits it as HID with
randomized key-hold and inter-key timing (defeats fixed-cadence detection).

## Flash the Pico

Needs the **arduino-pico** core (earlephilhower). With `arduino-cli`:

```bash
arduino-cli core install rp2040:rp2040            # add board index if first time:
#   arduino-cli config add board_manager.additional_urls \
#     https://github.com/earlephilhower/arduino-pico/releases/download/global/package_rp2040_index.json
arduino-cli compile  --fqbn rp2040:rp2040:rpipico scripts/listener/pico-hid
arduino-cli upload   --fqbn rp2040:rp2040:rpipico -p /dev/ttyACM0 scripts/listener/pico-hid
```

First flash: hold **BOOTSEL** while plugging in so the Pico mounts as `RPI-RP2`,
then upload. After the sketch is running it re-flashes over `/dev/ttyACM0`.

Arduino IDE works too: Board = "Raspberry Pi Pico", open `yufa-hid.ino`, Upload.

## Test it (no host needed)

Open any text editor, focus it, press the Pico's **BOOTSEL** button. It should
type `yufa-hid ok` + Enter. That confirms the HID keyboard enumerates and types.

## Use it for login

1. Plug the Pico into the machine running the client (the same box the game sees —
   for a headless server, the server's USB).
2. Launch real mode and bring the client to the login screen
   (`yufa-listener.sh run`). The password field auto-focuses; "Salvar Login"
   pre-fills the ID.
3. Trigger the typing:

   ```bash
   scripts/listener/pico-hid/pico-type.sh
   ```

   It sends `{DELAY 400}<pass>{ENTER}{DELAY 1800}<pin>{ENTER}`. Tune the delays in
   `pico-type.sh` to match how long the PIN dialog takes to appear.

### Macro language

One command per line, `\n`-terminated. Literal text is typed; tokens are special:
`{TAB}`, `{ENTER}`/`{RETURN}`, `{ESC}`, `{DELAY n}` (ms). Send an arbitrary macro
for debugging: `pico-type.sh '{DELAY 200}hello{ENTER}'`.

## Limitations & notes

- **US keyboard layout** is assumed (the core's ASCII→keycode map). Passwords with
  non-US-layout symbols may mistype — adjust or pick an ASCII-simple password.
- A password containing `{` or `}` collides with the macro syntax; `pico-type.sh`
  warns. (A future version could add escaping.)
- **Mouse is still unsolved** — see ../README.md. This only helps because every
  login screen is keyboard-navigable. Any screen needing a real click is still blocked.
- Don't open the serial port at **1200 baud** — that reboots the RP2040 into
  bootloader/flash mode. `pico-type.sh` uses 115200.
- For extra realism the firmware randomizes hold (18–55 ms) and gaps (45–160 ms);
  tune the `*_MS` constants at the top of `yufa-hid.ino`.

## CircuitPython alternative

If you prefer CircuitPython over an Arduino sketch: install CircuitPython + the
`adafruit_hid` bundle, then a `code.py` using `usb_hid` + `Keyboard`/`KeyboardLayoutUS`
reading the same macro over `usb_cdc.data`. Same design, different runtime; the
Arduino sketch here is the maintained prototype.
