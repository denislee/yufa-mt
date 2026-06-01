// yufa-hid.ino — Raspberry Pi Pico (RP2040) as a genuine USB HID keyboard that
// types the Projeto Yufa login on demand. The point is anti-detection: keystrokes
// come from a real, enumerated USB HID device — not a runtime uinput/XTEST virtual
// device — so an anti-cheat that flags software input tooling (the suspected
// ydotoold rejection, see ../README.md) sees ordinary hardware.
//
// The sketch stores NO credentials. It is a relay: the host (pico-type.sh) sends a
// small macro over the USB serial (CDC) channel, the Pico emits it as HID with
// human-like timing. The serial control channel is invisible to the game; only the
// keyboard interface is.
//
// Build: Arduino IDE / arduino-cli with the "Raspberry Pi Pico/RP2040" core
//   (earlephilhower). Board: "Raspberry Pi Pico". USB Stack: default (Pico SDK) is
//   fine — the core's Keyboard + Serial form a composite HID+CDC device.
//   Tools > USB Stack > "Adafruit TinyUSB" also works.
//
// Macro language (one command per line, terminated by '\n'):
//   literal text            -> typed key-by-key (US layout, shift handled)
//   {TAB}                   -> Tab
//   {ENTER} / {RETURN}      -> Return
//   {DELAY n}               -> pause n milliseconds (e.g. wait for the PIN dialog)
//   {ESC}                   -> Escape
// Example the host sends for login:
//   {DELAY 400}myPassword{ENTER}{DELAY 1800}1122{ENTER}
//
// Press the onboard BOOTSEL button to type a fixed self-test string instead — lets
// you confirm HID works in any text field without the host wired up.

#include <Keyboard.h>

// ---- Tunables ---------------------------------------------------------------
static const unsigned long SERIAL_BAUD = 115200;  // must match pico-type.sh
static const int  GAP_MIN_MS  = 45;   // inter-keystroke gap (low end)
static const int  GAP_MAX_MS  = 160;  // inter-keystroke gap (high end)
static const int  HOLD_MIN_MS = 18;   // key-down hold time (low end)
static const int  HOLD_MAX_MS = 55;   // key-down hold time (high end)

// Typed when BOOTSEL is pressed — a host-free sanity check (open any text editor).
static const char* TEST_MACRO = "yufa-hid ok{ENTER}";

// ---- State ------------------------------------------------------------------
static char   line[256];
static size_t len = 0;

// True hardware RNG on the RP2040 (ROSC-based); no seeding needed.
static inline int randRange(int lo, int hi) {
  return lo + (int)(rp2040.hwrand32() % (uint32_t)(hi - lo + 1));
}

// Emit one HID key with a realistic key-down hold and a following inter-key gap.
// Accepts both ASCII (mapped to keycode + shift by the core) and KEY_* constants.
static void tap(uint8_t k) {
  Keyboard.press(k);
  delay(randRange(HOLD_MIN_MS, HOLD_MAX_MS));
  Keyboard.release(k);
  delay(randRange(GAP_MIN_MS, GAP_MAX_MS));
}

// Handle a {...} token. Unknown tokens are silently ignored.
static void handleToken(const char* tok) {
  if (!strcmp(tok, "TAB"))                          { tap(KEY_TAB);    return; }
  if (!strcmp(tok, "ENTER") || !strcmp(tok, "RETURN")) { tap(KEY_RETURN); return; }
  if (!strcmp(tok, "ESC")   || !strcmp(tok, "ESCAPE")) { tap(KEY_ESC);    return; }
  if (!strncmp(tok, "DELAY", 5)) {                  // "DELAY 500" or "DELAY500"
    long ms = atol(tok + 5);
    if (ms > 0 && ms <= 60000) delay(ms);
    return;
  }
}

// Parse and execute a macro line.
static void runMacro(const char* s) {
  digitalWrite(LED_BUILTIN, HIGH);
  size_t i = 0, n = strlen(s);
  while (i < n) {
    if (s[i] == '{') {
      size_t j = i + 1;
      while (j < n && s[j] != '}') j++;
      if (j < n) {                                  // found a closing brace
        char tok[32];
        size_t tl = j - (i + 1);
        if (tl >= sizeof(tok)) tl = sizeof(tok) - 1;
        memcpy(tok, s + i + 1, tl);
        tok[tl] = '\0';
        handleToken(tok);
        i = j + 1;
        continue;
      }
      // No closing brace — fall through and type '{' literally.
    }
    tap((uint8_t)s[i]);                             // literal character
    i++;
  }
  digitalWrite(LED_BUILTIN, LOW);
}

void setup() {
  pinMode(LED_BUILTIN, OUTPUT);
  digitalWrite(LED_BUILTIN, LOW);
  Serial.begin(SERIAL_BAUD);
  Keyboard.begin();
}

void loop() {
  // BOOTSEL self-test: type the fixed string, then wait for release (debounce).
  if (BOOTSEL) {
    runMacro(TEST_MACRO);
    while (BOOTSEL) delay(10);
  }

  // Read one newline-terminated macro from the host and execute it.
  while (Serial.available()) {
    char c = (char)Serial.read();
    if (c == '\n') {
      line[len] = '\0';
      if (len) runMacro(line);
      len = 0;
    } else if (c != '\r' && len < sizeof(line) - 1) {
      line[len++] = c;
    }
  }
}
