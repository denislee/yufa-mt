# Can a custom client replace the real one? — protocol recon & verdict

**Question asked:** build our own headless client that does the whole login and
just listens to chat, mimicking/replacing Gepard, instead of running the real
Projeto Yufa client.

**Short answer: no — not without reverse-engineering the proprietary `gepard.dll`.**
The entire RO protocol *around* Gepard is now fully mapped and is plaintext, but
two chokepoints (login and zone entry) require a Gepard challenge-response plus a
session-keyed credential cipher that only the 19.7 MB packed anti-cheat DLL can
produce. This is the same "private/paid bypass" wall the project already chose to
avoid — confirmed here at the byte level, not assumed.

This was derived **passively** from existing captures in `~/tmp/amigo/` and
`~/Downloads/*.pcapng` (a friend's "amigo" login sessions). No live probes or
forged packets were sent — that would risk an account/IP ban for zero upside.

---

## Connection map (one login session)

Project Yufa is fronted by **AWS Global Accelerator** (`99.83.172.22`,
`75.2.90.234`), so every server is the same anycast IP on a different port.

| Stream | Server : port | Role | Gepard? |
|---|---|---|---|
| 0 | `…:7900` | **Login** server | **YES** — challenge-response + encrypted creds |
| 1 | `…:7121` | **Char** server | **NO** — 100% plaintext rAthena |
| 2,3 | `75.2.90.234:8888` | **userconfig** HTTP API (hotkeys/UI JSON) | n/a — not security |
| 4 | `…:6121` | **Zone/map** server (chat lives here) | **YES** — challenge-response only |

Port 8888 looked suspicious but is a red herring: it's a plain
`POST /userconfig/load` multipart upload (`AID`, `WorldName=Yufa`) that returns a
JSON blob of the account's emotion/skillbar hotkeys. Irrelevant to login or chat.

---

## Full handshake (byte-level, from the pcaps)

Opcodes are little-endian `uint16`. `C→S` = client→server.

### 1. Login server `:7900` — **Gepard-gated**
```
C→S  64 00 01 00 00 00 …(zeros)…        CA_LOGIN, 55B, EMPTY creds  (a trigger)
S→C  53 47 24 00  <32-byte seed>        Gepard challenge, opcode 0x4753, len 36
C→S  92 c3 2c 00  <40-byte answer>      Gepard response,  opcode 0xc392, len 44
C→S  64 00 01 00  <48B ENCRYPTED creds> CA_LOGIN again, real (encrypted) id/pw
S→C  c4 0a e0 00  …                     AC_ACCEPT_LOGIN (0x0ac4) — CLEARTEXT:
                                          login_id1, AID, login_id2, web token,
                                          char server 127.0.0.1:7121 "Yufa"
```

### 2. Char server `:7121` — **no Gepard, fully plaintext**
```
C→S  65 00 | AID | login_id1 | login_id2 | clientType | sex      CH_ENTER (0x0065)
S→C  AID(4) + 2d 08 … char list (0x082d) + 0b 72 … (0x0b72)      char "amigo", GID, map
C→S  87 01 | AID                                                 keepalive (0x0187)
C→S  b8 08 | AID | "4545"                                        PIN entry (0x08b8) — CLEARTEXT
S→C  b9 08 …                                                     PIN ack (0x08b9)
C→S  66 00 | slot                                                CH_SELECT_CHAR (0x0066)
S→C  c5 0a | GID | "new_1-1.gat" | zoneIP | zonePort             map info (0x0ac5)
```

### 3. Zone server `:6121` — **Gepard challenge only; payload cleartext**
```
C→S  36 04 | AID | GID | login_id1 | tick | sex                  CZ_ENTER (0x0436)
S→C  53 47 24 00 <32-byte seed>                                  Gepard challenge (0x4753)
S→C  18 0b … map/pos, then channel-join chat:                    ZC_ACCEPT (0x0b18)…
       "You're now in the '#global' channel."  ← CLEARTEXT CHAT
C→S  92 c3 2c 00 <40-byte answer>                                Gepard response (0xc392)
…    ongoing gameplay + chat, all cleartext S→C
```

**Chat is cleartext in the S→C direction** on the zone server — which is exactly
why the existing *passive* sniffer works and needs no crypto.

---

## Why a custom client is blocked

Three things only `gepard.dll` can produce, all on the C→S path:

1. **The `0xc392` response (login).** The 32-byte seed is mostly a static
   server key plus a per-session nonce. Comparing four sessions byte-by-byte:
   - bytes 0–5: random per session
   - **bytes 6–26: constant** = `76 4c d8 a3 11 a5 d6 d8 cf f0 ff a8 65 fa 62 33 f2 26 f4 bd db` (Gepard's server key)
   - bytes 27–31: random per session

   The client returns a 40-byte high-entropy answer computed by an algorithm
   embedded in the DLL. We don't have the algorithm.

2. **The encrypted credentials (login).** The real `CA_LOGIN` body differs
   *completely* across three logins of the **same** account:
   ```
   c0 1a 04 38 6d a4 4c 35 …
   61 77 09 67 04 00 17 d7 …
   b4 d3 a2 ba ab af 72 ad …
   ```
   → a per-session stream cipher keyed off the challenge nonce. A prior attempt
   in `streams/` tried a fixed 3-word XOR key; the decoded output is garbage,
   confirming it is *not* a static XOR.

3. **The `0xc392` response (zone).** Same challenge-response again on `:6121`.
   The first chat burst arrives ~2 ms *before* the client's response, so the
   handshake might be "soft" — but every real capture answers it, and an
   unanswered session is overwhelmingly likely to be dropped/flagged server-side
   by the Gepard plugin. Confirming otherwise would require a live forged
   connection = ban risk.

These three are precisely what an anti-cheat exists to protect, and
`gepard.dll` (19.7 MB, packed, anti-debug, frequently updated — `gepard.register`
was rewritten today) is built to make extracting them as hard as possible.

### The token trap (why a hybrid doesn't escape Gepard either)
`AID` / `login_id1` / `login_id2` / `GID` are **ephemeral, minted per login** by
the Gepard-gated login server. You cannot hardcode them; you must log in to get
them. And even handed valid tokens, opening our own socket to `:6121` still hits
that zone `0x4753` challenge. So the best-case hybrid ("real client logs in →
scrape tokens off the wire → Go client holds the zone socket") **still needs the
real client for login, still faces the zone challenge, and adds ban risk** — i.e.
strictly worse than today's passive sniffer.

---

## Verdict & realistic options

- ✅ **Protocol fully understood** — char server is forgeable today; login/zone
  data and all chat are cleartext to *read*.
- ❌ **A client that logs in by itself is not feasible** without RE'ing
  `gepard.dll` (the paid/private-bypass path the project avoids).
- ✅ **The current architecture is the correct one:** one genuine Gepard client
  holds the session; the passive sniffer reads cleartext chat. Nothing forgeable,
  nothing bannable.

The real lever for "headless / no manual login" is **not** reimplementing Gepard
— it's automating the existing client past the **randomized PIN keypad** (the one
remaining manual step). The PIN travels as a cleartext `0x08b8` packet, but it's
emitted from *inside* the Gepard-wrapped client, so it can't be injected
externally; the keypad needs a real pointer event. That points back at the
`pico-hid/` hardware-mouse prototype (genuine USB HID clicks), which is the
sanctioned way forward documented in the README.
