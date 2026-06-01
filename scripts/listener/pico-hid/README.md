# Pico HID for the PIN keypad (the real job)

A Raspberry Pi Pico (RP2040) acting as a **genuine USB HID device** — enumerated
over USB, so its events are indistinguishable from real hardware at the
libinput/Xwayland/Wine/Gepard layer.

> **Premise corrected (2026-06-01).** This started as a *keyboard* typer on the
> theory that Gepard rejects the login handshake because it detects `ydotoold`'s
> virtual device. **That theory is disproven** — see [../README.md](../README.md).
> Driving the login with `ydotool` on the real display works end-to-end: account
> auth and character-select all succeed, and the handshake is fine. **Keyboard
> automation is already solved by ydotool, so the keyboard typer below is
> redundant.**
>
> The **only** step that needs genuine hardware is the **mouse click on the
> character-select PIN keypad.** Synthetic clicks (ydotool uinput `BTN_LEFT`) are
> emitted at the kernel but rejected above the OS — Gepard accepts mouse *motion*
> but not the injected *button*. A real USB-HID **mouse** click should pass where
> the synthetic one fails (this is the open hypothesis to test). So the useful
> firmware here is an **absolute-positioned USB mouse**, not a keyboard:
> the host reads the shuffled keypad from a `grim` screenshot, maps `1,1,2,2` to
> cell coordinates, and tells the Pico to move+click each. The keyboard sketch
> below remains only as a reference/fallback.

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
- **This keyboard sketch does NOT solve login** — ydotool already drives the
  keyboard. The unsolved step is the **PIN keypad click**, which needs an
  absolute-positioned USB **mouse** firmware (not yet written here). See
  ../README.md "Automated login" for the full picture.
- Don't open the serial port at **1200 baud** — that reboots the RP2040 into
  bootloader/flash mode. `pico-type.sh` uses 115200.
- For extra realism the firmware randomizes hold (18–55 ms) and gaps (45–160 ms);
  tune the `*_MS` constants at the top of `yufa-hid.ino`.

## CircuitPython alternative

If you prefer CircuitPython over an Arduino sketch: install CircuitPython + the
`adafruit_hid` bundle, then a `code.py` using `usb_hid` + `Keyboard`/`KeyboardLayoutUS`
reading the same macro over `usb_cdc.data`. Same design, different runtime; the
Arduino sketch here is the maintained prototype.
