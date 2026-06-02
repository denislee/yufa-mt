# Handoff: mid-sweep SIGTERM abort test

**Status:** TODO (deferred). Everything else around the `@mobinfo` scrape is built, deployed, and verified on the live box. This is the one *optional* validation still outstanding.

## What this test proves

A shutdown-hang bug was fixed in commit **`d17445c` (fix: app shutdown)**:

- The scheduler runs jobs synchronously in `runOnce`, and their `Func()`s don't observe the app shutdown context. `runMobInfoSweep` previously used `context.Background()`, so a `SIGTERM` **during an in-flight sweep** kept `bgWg.Wait()` busy for up to ~10 minutes → the process never exited → **systemd never respawned it → the self-update path was broken** (self-update SIGTERMs itself and relies on `Restart=always` to come back on the new binary).

The fix has two parts (both in `internal/server/`):
1. `runMobInfoSweep` now derives its context from `bgCtx` (the app shutdown ctx, captured in `startBackgroundJobs`) — so a `SIGTERM` aborts the sweep promptly. (`mobscrape.go`, `scraper_jobs.go`)
2. `bgWg.Wait()` in `server.go` is now **bounded to 15s**, after which the process exits regardless — a hard guarantee that shutdown can't wedge even if some job ignores ctx.

This test exercises the **real** failure path: kick off a long sweep, `SIGTERM` mid-flight, and confirm the process exits fast (≈ seconds, ≤ ~18s) instead of hanging for minutes.

It was verified only indirectly so far (a *normal* restart drained in ~15s with no hang). The destructive mid-sweep proof was skipped to avoid extra churn/relog cycles.

## Environment / reference facts

- **Host:** `dns@x230.taild6ffd8.ts.net` (Tailscale). **Password auth only** — no key from the dev host.
  - The SSH password is **not** stored anywhere. Get it from the operator, or have the operator run the commands themselves with the `!` prefix in-session.
  - Heads-up: the Claude Code auto-mode classifier **blocks SSH writes to this prod host** unless the user has explicitly authorized that specific action in-conversation. Get an explicit "yes, do it" before any restart/kill.
- **Repo on x230:** `/home/dns/git/yufa-mt/` · binary `/home/dns/git/yufa-mt/yufa-mt` · DB `/home/dns/git/yufa-mt/data/runtime/market_data.db`
- **Service:** system unit `yufa-mt` (`/etc/systemd/system/yufa-mt.service`), `User=dns`, `Restart=always`, `RestartSec=3`, `AmbientCapabilities=CAP_NET_RAW CAP_NET_ADMIN`, `Environment=XTABLES_LOCKFILE=/run/yufa-mt/xtables.lock`.
  - **Restart without sudo:** `dns` owns the process, so `kill -TERM <MainPID>` → systemd respawns. Use `kill -9` only if it hangs.
- **Admin API:** HTTP Basic Auth at `http://127.0.0.1:8080`. User = `$ADMIN_USER` (default `admin`), pass = `$ADMIN_PASSWORD` (read from `/home/dns/git/yufa-mt/.env`). Endpoints:
  - `POST /admin/mobscrape/config` — form `from_id`, `to_id`, `delay_ms`
  - `POST /admin/scheduler/run` — form `job_name=mobinfo`
  - `POST /admin/mobscrape/stop`
- **Proxies:** zone proxy REDIRECTs `:6121` → local `:6799`; PIN proxy REDIRECTs char `:7121` → `:7799`. `ZONE_PROXY` defaults **on**; `ZONE_PROXY_CHAR_NAME=observador` (the headless char; needed because the AFK client never sends chat for the proxy to learn the name from).
- **Headless client + watchdog:** the RO client runs headless via Lutris/GE-Proton, driven by `scripts/listener/yufa-listener.sh run` — a **plain bash process, NOT a systemd unit** (`systemctl --user` will say "not found"). Its watchdog relaunches + auto-logs-in (the PIN proxy passes the keypad) when the client dies or loses the zone link (~90s grace).
- **Tables:** `internal_mob_db` = rAthena pre-re YAML baseline (1004 mobs); `mob_server_db` = live `@mobinfo` values (separate, so the bestiary can diff them).

## Preconditions

1. yufa-mt running the binary at/after commit `d17445c`. Verify on x230:
   ```sh
   cd /home/dns/git/yufa-mt && git log --oneline -1     # expect d17445c or later
   systemctl show yufa-mt -p MainPID -p ActiveState --value
   ```
2. A **live zone session** (client in-game through the proxy) so the sweep can actually inject. Verify:
   ```sh
   curl -fsS http://127.0.0.1:8080/health/zone        # age should be small (in-game)
   journalctl -u yufa-mt --since "5 min ago" | grep "zone proxy: new connection"
   ```
   If `age=-1` / no session: relog the client (see Recovery) and wait for it to reach the map.

## Test procedure

All commands run on x230 (over SSH or via the operator's `!`). Read `$ADMIN_USER`/`$ADMIN_PASSWORD` from `.env` on the box so the password never leaves it.

1. **Set a long range** so the sweep is comfortably mid-flight when you SIGTERM (full range ≈ 10 min):
   ```sh
   cd /home/dns/git/yufa-mt
   U=$(grep '^ADMIN_USER=' .env | cut -d= -f2- | tr -d '"'); U=${U:-admin}
   P=$(grep '^ADMIN_PASSWORD=' .env | cut -d= -f2- | tr -d '"')
   curl -s -u "$U:$P" -d from_id=1001 -d to_id=2500 -d delay_ms=400 \
        http://127.0.0.1:8080/admin/mobscrape/config -o /dev/null -w 'config=%{http_code}\n'
   ```
2. **Start the sweep:**
   ```sh
   curl -s -u "$U:$P" -d job_name=mobinfo \
        http://127.0.0.1:8080/admin/scheduler/run -o /dev/null -w 'run=%{http_code}\n'
   ```
3. **Confirm it's running** (let it get a few IDs in, ~5–10s):
   ```sh
   sleep 8
   journalctl -u yufa-mt --since "15 sec ago" | grep "Scraper/MobInfo" | tail -3
   ```
   You should see `starting sweep 1001-2500` and several `Captured @mobinfo for ...` lines.
4. **SIGTERM mid-sweep and time the respawn.** Capture the start time, send SIGTERM, poll for a new MainPID:
   ```sh
   OLD=$(systemctl show yufa-mt -p MainPID --value); echo "old=$OLD t=$(date +%s)"
   kill -TERM "$OLD"
   NEW=$OLD; for i in $(seq 1 12); do sleep 2; NEW=$(systemctl show yufa-mt -p MainPID --value); \
     [ "$NEW" != "$OLD" ] && [ "$NEW" != 0 ] && break; done
   echo "new=$NEW  elapsed≈$((i*2))s"
   ```
5. **Inspect the shutdown logs:**
   ```sh
   journalctl -u yufa-mt --since "40 sec ago" --no-pager | \
     grep -iE "sweep stopped|sweep complete|Waiting for background|All services shut down|did not drain"
   ```

## Pass / fail

- **PASS (ideal):** new MainPID appears within **≤ ~18s** of SIGTERM, AND the log shows `[Scraper/MobInfo] ... sweep stopped at id N` plus `All services shut down. Exiting.` → both the sweep-ctx abort and the bounded drain work.
- **PASS (safety net only):** new MainPID within ~18s but the log shows `Background services did not drain within 15s; exiting anyway` and **no** `sweep stopped` line → the 15s bounded-drain guarantee works (self-update safe), but the sweep did **not** observe `bgCtx`. Investigate fix #1 (`runMobInfoSweep` should derive ctx from `bgCtx`; check `bgCtx` is set in `startBackgroundJobs`).
- **FAIL:** no new MainPID for far longer than 15s (e.g. minutes), or the sweep runs to `sweep complete` despite the SIGTERM → the hang is not fixed. Force-recover with `kill -9 "$OLD"` and re-examine `server.go` drain + `mobscrape.go` ctx wiring.

## Recovery / cleanup (always do after the test)

Restarting yufa-mt drops the proxied game session, so the client must relog:

1. Confirm the respawned process is healthy (both proxies up, no iptables error):
   ```sh
   journalctl -u yufa-mt --since "30 sec ago" | grep -iE "PIN proxy listening|zone proxy listening|failed to install"
   ```
2. **Relog the headless client** (forces a fresh zone connection through the restarted proxy). Use the bracket pattern so `pkill` does **not** match your own SSH shell:
   ```sh
   pkill -f 'Projeto_Yufa[.]exe'      # watchdog relaunches + auto-logs-in
   ```
   The first quick re-login sometimes mistimes (doesn't reach the map); the watchdog does a full relaunch after ~90s and that succeeds. Give it 1–2 minutes, then verify:
   ```sh
   journalctl -u yufa-mt --since "2 min ago" | grep "zone proxy: new connection"
   curl -fsS http://127.0.0.1:8080/health/zone        # age small = back in-game
   ```
3. The scrape config is left at `1001-2500` (the normal full range) — fine to leave. The `mobinfo` scheduler job is `DefaultDisabled`, so it won't auto-run.

## Notes

- This is **destructive to the current scrape session** (one restart + one relog cycle). Already-captured data in `mob_server_db` is unaffected.
- If self-update is being relied upon before this test passes, the bounded-drain (fix #2) alone already guarantees the process exits within 15s — so self-update is safe regardless; this test just confirms the *clean* abort (fix #1) as well.
