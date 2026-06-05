package server

// gamerestart.go — the admin "Restart Game Client" action. The app does NOT
// launch the game client itself; the headless client is supervised by
// scripts/listener/yufa-listener.sh (the yufa-listener.service unit), whose
// cmd_run loop relaunches the client and re-runs the auto-login sequence (the
// PIN proxy injects the PIN + char-select) the instant the client process
// exits. So "restart" here just means: terminate the running client; the
// watchdog brings it back, logged in.
//
// We match the very processes the Machine tab's "Game client" card shows
// (gameClientRegexp over /proc/<pid>/cmdline), so what the operator sees is
// what gets killed. SIGTERM first with a short grace — letting Gepard/wine
// close its socket cleanly, mirroring the listener's own kill_client — then
// SIGKILL any stragglers.

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// findGameClientPIDs walks /proc once and returns the PIDs whose cmdline matches
// the configured game-client pattern (GameClientMatch) — the same predicate the
// Machine-tab "Game client" card uses. Returns nil when monitoring is disabled
// (empty/invalid pattern) or no match is running.
func findGameClientPIDs() []int {
	re := gameClientRegexp()
	if re == nil {
		return nil
	}
	self := os.Getpid()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		cmdline := readCmdline(pid)
		if cmdline != "" && re.MatchString(cmdline) {
			pids = append(pids, pid)
		}
	}
	return pids
}

// pidAlive reports whether pid still exists (signal 0 probes without delivering).
func pidAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// killGameClient SIGTERMs every pid, waits up to a few seconds for a clean exit
// (so Gepard/wine can release its socket), then SIGKILLs whatever remains. It
// does not relaunch — the listener watchdog detects the dead client and brings
// it back, auto-logging in. Run in a goroutine: the grace loop can take seconds.
func killGameClient(pids []int) {
	for _, pid := range pids {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
			log.Printf("[W] [Admin/GameRestart] SIGTERM pid %d failed: %v", pid, err)
		}
	}
	for i := 0; i < 5; i++ {
		time.Sleep(1 * time.Second)
		if len(alivePIDs(pids)) == 0 {
			log.Printf("[I] [Admin/GameRestart] game client stopped cleanly; watchdog will relaunch + auto-login.")
			return
		}
	}
	for _, pid := range alivePIDs(pids) {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			log.Printf("[W] [Admin/GameRestart] SIGKILL pid %d failed: %v", pid, err)
		}
	}
	log.Printf("[I] [Admin/GameRestart] game client force-killed; watchdog will relaunch + auto-login.")
}

// alivePIDs filters pids down to those still running.
func alivePIDs(pids []int) []int {
	var out []int
	for _, pid := range pids {
		if pidAlive(pid) {
			out = append(out, pid)
		}
	}
	return out
}

// adminRestartGameHandler kills the running game client so the listener watchdog
// relaunches it and auto-logs back in via the PIN proxy. POST-only; admin-auth
// is applied by the /admin/ mount in registerAdminRoutes.
func adminRestartGameHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if gameClientRegexp() == nil {
		http.Redirect(w, r, adminRedirectURL(r, "Game-client monitoring is disabled (GAME_CLIENT_MATCH is empty); nothing to restart."), http.StatusSeeOther)
		return
	}

	pids := findGameClientPIDs()
	if len(pids) == 0 {
		log.Printf("[I] [Admin/GameRestart] restart requested but no game client process is running.")
		http.Redirect(w, r, adminRedirectURL(r, "No running game client found. If the listener watchdog (yufa-listener.service) is up it should be launching one shortly; otherwise start the listener."), http.StatusSeeOther)
		return
	}

	log.Printf("[I] [Admin/GameRestart] admin restarting game client (pids %v); watchdog will relaunch + auto-login.", pids)
	go killGameClient(pids)

	pidStrs := make([]string, len(pids))
	for i, pid := range pids {
		pidStrs[i] = strconv.Itoa(pid)
	}
	msg := fmt.Sprintf("Stopping game client (pid %s). The listener watchdog will relaunch it and auto-login via the PIN proxy — watch the Processes card.", strings.Join(pidStrs, ", "))
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}
