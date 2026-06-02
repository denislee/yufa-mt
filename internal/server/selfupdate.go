package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/denislee/yufa-mt/internal/config"
)

// selfUpdate performs an in-place upgrade of the running server:
//
//	git pull → go build -tags fts5 -o <binary>.new ./cmd/server →
//	setcap (best effort) → rename .new over the live binary →
//	SIGTERM self for a graceful shutdown.
//
// The process is expected to run under a supervisor that respawns it
// (systemd Restart=always on the live box), so the SIGTERM lands as a
// restart onto the freshly built binary. The feature is gated behind
// SELF_UPDATE=1 (config.SelfUpdateEnabled) so a dev instance — which has
// no supervisor — can't be shut down by an accidental click.
//
// Split deployment (docs/two-process-split-plan.md): self-update runs in the
// APP process (ModeApp) under yufa-mt-app.service. The SIGTERM restarts only
// that unit; yufa-mt-proxy.service keeps running its old inode and holds the
// live game connection — so a deploy no longer disconnects the client. The
// binary swap below replaces the single shared binary, which the proxy unit
// picks up the next time IT is (rarely) restarted. In ModeAll (single unit) the
// SIGTERM restarts everything, exactly as before.

// updateStatus is the process-wide state of the most recent self-update,
// surfaced on the admin dashboard. Step-by-step output also streams to the
// log buffer (see logbuffer.go) so admins can watch progress live.
type updateState struct {
	mu       sync.Mutex
	running  bool
	started  time.Time
	finished time.Time
	stage    string // human-readable current/last stage
	ok       bool   // whether the last completed run succeeded
	message  string // last result or error summary
}

var updateStatus = &updateState{}

// Snapshot returns a copy of the current update state for templating.
func (s *updateState) Snapshot() UpdateStatusView {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := UpdateStatusView{
		Running: s.running,
		Stage:   s.stage,
		OK:      s.ok,
		Message: s.message,
	}
	if !s.started.IsZero() {
		v.Started = s.started.Format("2006-01-02 15:04:05")
	}
	if !s.finished.IsZero() {
		v.Finished = s.finished.Format("2006-01-02 15:04:05")
	}
	return v
}

// UpdateStatusView is the template-facing view of updateState.
type UpdateStatusView struct {
	Running  bool
	Stage    string
	OK       bool
	Message  string
	Started  string
	Finished string
}

// begin marks an update as started, returning false if one is already in
// flight (so concurrent triggers are rejected).
func (s *updateState) begin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return false
	}
	s.running = true
	s.started = time.Now()
	s.finished = time.Time{}
	s.stage = "starting"
	s.message = ""
	return true
}

func (s *updateState) set(stage string) {
	s.mu.Lock()
	s.stage = stage
	s.mu.Unlock()
}

func (s *updateState) fail(stage, msg string) {
	s.mu.Lock()
	s.running = false
	s.finished = time.Now()
	s.stage = stage
	s.ok = false
	s.message = msg
	s.mu.Unlock()
}

func (s *updateState) succeed(msg string) {
	s.mu.Lock()
	s.running = false
	s.finished = time.Now()
	s.stage = "restarting"
	s.ok = true
	s.message = msg
	s.mu.Unlock()
}

// runUpdateStep runs name+args in dir with a timeout, logging the command
// and its combined output to the log buffer. It returns the output and any
// error so the caller can abort the sequence.
func runUpdateStep(dir, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	slog.Info("[Update] running", "cmd", name+" "+strings.Join(args, " "), "dir", dir)
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	trimmed := strings.TrimRight(string(out), "\n")
	if trimmed != "" {
		// Echo each line of the command output into the log so it shows up
		// in the admin log viewer.
		for _, line := range strings.Split(trimmed, "\n") {
			slog.Info("[Update] " + name + ": " + line)
		}
	}
	return trimmed, err
}

// rebuildBinary runs git pull → go build → (setcap if root) → atomic swap of the
// shared yufa-mt binary. It records progress/failure in updateStatus and returns
// an error on failure (the caller just returns). It does NOT restart anything —
// callers decide whether to restart the app (performSelfUpdate) or the proxy
// (performProxyUpdate).
func rebuildBinary() error {
	repoDir, err := os.Getwd()
	if err != nil {
		updateStatus.fail("init", "could not determine working directory: "+err.Error())
		slog.Error("[Update] getwd failed", "error", err)
		return err
	}

	// Name of the live binary, e.g. "yufa-mt". Derive from the running
	// executable so the rename target matches whatever the supervisor execs.
	binName := "yufa-mt"
	if exe, err := os.Executable(); err == nil {
		binName = filepath.Base(exe)
	}
	newBin := binName + ".new"

	branch := "main"
	if appConfig != nil && appConfig.SelfUpdateBranch != "" {
		branch = appConfig.SelfUpdateBranch
	}

	slog.Info("[Update] rebuild started", "repo", repoDir, "binary", binName, "branch", branch)

	// 1. git pull (fast-forward only — refuse to merge divergent history).
	updateStatus.set("git pull")
	if out, err := runUpdateStep(repoDir, "git", "pull", "--ff-only", "origin", branch); err != nil {
		updateStatus.fail("git pull", "git pull failed: "+err.Error()+"\n"+out)
		slog.Error("[Update] git pull failed", "error", err)
		return err
	}

	// 2. Build the new binary alongside the current one.
	updateStatus.set("build")
	if out, err := runUpdateStep(repoDir, "go", "build", "-tags", "fts5", "-o", newBin, "./cmd/server"); err != nil {
		updateStatus.fail("build", "build failed: "+err.Error()+"\n"+out)
		slog.Error("[Update] build failed", "error", err)
		return err
	}

	// 3. Re-apply the network capabilities the live binary needs (chat capture
	// + PIN proxy use CAP_NET_RAW/CAP_NET_ADMIN). File caps are wiped by every
	// rebuild, so they must be re-set on the new binary — but only when we run
	// as root: setcap itself requires CAP_SETFCAP. When the service runs
	// unprivileged (User=dns), the process instead inherits the caps from
	// systemd's AmbientCapabilities=, so file caps aren't needed and setcap
	// would only fail; skip it. Best effort either way (non-fatal).
	if os.Geteuid() == 0 {
		updateStatus.set("setcap")
		if _, err := runUpdateStep(repoDir, "setcap", "cap_net_raw,cap_net_admin=eip", filepath.Join(repoDir, newBin)); err != nil {
			slog.Warn("[Update] setcap failed (non-fatal); re-apply capabilities manually if needed", "error", err)
		}
	} else {
		slog.Info("[Update] running unprivileged; skipping setcap (capabilities come from systemd AmbientCapabilities)")
	}

	// 4. Atomically swap the new binary over the live one. The running
	// process keeps executing its old inode until it exits, so this is safe.
	updateStatus.set("swap binary")
	if err := os.Rename(filepath.Join(repoDir, newBin), filepath.Join(repoDir, binName)); err != nil {
		updateStatus.fail("swap binary", "could not replace binary: "+err.Error())
		slog.Error("[Update] rename failed", "error", err)
		return err
	}
	return nil
}

// performSelfUpdate rebuilds the binary then restarts THIS (app) process. On
// success it does NOT return — it SIGTERMs itself so the supervisor respawns on
// the new binary. In the split deployment this restarts only the app; the proxy
// (and the game connection) are untouched.
func performSelfUpdate() {
	if err := rebuildBinary(); err != nil {
		return // updateStatus already records the failure
	}

	updateStatus.succeed("Update applied. Restarting to load the new build…")
	if appConfig != nil && appConfig.Mode == config.ModeApp {
		slog.Info("[Update] new binary in place; restarting app process only — the proxy keeps the game connection up")
	} else {
		slog.Info("[Update] new binary in place; sending SIGTERM for graceful restart")
	}

	// Trigger a graceful shutdown via the existing SIGTERM handler in Run. The
	// supervisor (systemd Restart=always) respawns the process onto the freshly
	// built binary. Delay briefly so the triggering request's response flushes.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			slog.Error("[Update] failed to signal self for restart", "error", err)
		}
	}()
}

// performProxyUpdate rebuilds the binary then asks the PROXY process to restart
// (over IPC) so it execs the new binary. Runs in the app process; the app itself
// is NOT restarted. This is the one update that drops the live game connection
// (the listener watchdog reconnects). ModeApp only.
func performProxyUpdate() {
	if err := rebuildBinary(); err != nil {
		return // updateStatus already records the failure
	}

	updateStatus.set("restart proxy")
	slog.Info("[Update] new binary in place; asking the proxy to restart (one game reconnect)")
	if err := requestProxyRestart(); err != nil {
		updateStatus.fail("restart proxy", "binary rebuilt OK, but the proxy restart request failed: "+err.Error())
		slog.Error("[Update] proxy restart request failed", "error", err)
		return
	}
	updateStatus.succeed("Proxy rebuilt and restart requested. The game client will briefly disconnect and the watchdog will reconnect it.")
}

// adminSelfUpdateHandler triggers a self-update. Gated behind
// config.SelfUpdateEnabled.
func adminSelfUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if appConfig == nil || !appConfig.SelfUpdateEnabled {
		http.Redirect(w, r, adminRedirectURL(r, "Self-update is disabled. Set SELF_UPDATE=1 to enable it."), http.StatusSeeOther)
		return
	}

	if !updateStatus.begin() {
		http.Redirect(w, r, adminRedirectURL(r, "An update is already in progress."), http.StatusSeeOther)
		return
	}

	go performSelfUpdate()

	slog.Info("[Update] admin triggered self-update")
	msg := fmt.Sprintf("Self-update started (branch %s). Watch the Server Logs panel for progress; the server will restart automatically.", appConfig.SelfUpdateBranch)
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}

// adminProxyUpdateHandler rebuilds the binary and restarts the PROXY process
// (not the app). Gated behind SELF_UPDATE=1 AND the split deployment (ModeApp) —
// in single-process mode there is no separate proxy to update. This drops the
// live game connection once (the watchdog reconnects).
func adminProxyUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin", http.StatusSeeOther)
		return
	}

	if appConfig == nil || !appConfig.SelfUpdateEnabled {
		http.Redirect(w, r, adminRedirectURL(r, "Self-update is disabled. Set SELF_UPDATE=1 to enable it."), http.StatusSeeOther)
		return
	}
	if appConfig.Mode != config.ModeApp {
		http.Redirect(w, r, adminRedirectURL(r, "Proxy update only applies in the split deployment (run with --mode=app + a separate proxy unit)."), http.StatusSeeOther)
		return
	}

	if !updateStatus.begin() {
		http.Redirect(w, r, adminRedirectURL(r, "An update is already in progress."), http.StatusSeeOther)
		return
	}

	go performProxyUpdate()

	slog.Info("[Update] admin triggered proxy update")
	msg := fmt.Sprintf("Proxy update started (branch %s). The game client will briefly disconnect and reconnect. Watch the Server Logs panel (Proxy view) for progress.", appConfig.SelfUpdateBranch)
	http.Redirect(w, r, adminRedirectURL(r, msg), http.StatusSeeOther)
}
