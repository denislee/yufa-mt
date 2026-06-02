package server

// procstats.go — per-process liveness + resource sampling for the admin
// "Machine" tab. It answers two operator questions about the x230 box:
//
//   1. Are the moving parts still up? (the main app, the connection-owning
//      proxy process, and the headless game client)
//   2. How much CPU / memory is each consuming right now?
//
// Everything is best-effort from /proc, mirroring sysstats.go: on a platform
// where a source is unavailable a group simply reports not-running rather than
// failing the whole snapshot. CPU% is normalized to the WHOLE machine (0..100,
// the same scale as SystemStats.CPUPercent) so a process group's number is
// directly comparable to the top-level CPU gauge — it is the group's busy-jiffie
// delta divided by the aggregate /proc/stat jiffie delta between two polls.

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/denislee/yufa-mt/internal/config"
)

// ProcessStat is a point-in-time view of one monitored process group. A group
// can span several PIDs (the game client under wine/Proton spawns a tree), in
// which case CPU% and RSS are summed across them.
type ProcessStat struct {
	Key        string  `json:"key"`         // "app" | "proxy" | "game"
	Label      string  `json:"label"`       // human label for the card
	Running    bool    `json:"running"`     // at least one matching PID is alive
	InProcess  bool    `json:"in_process"`  // proxy only: runs inside the app process (ModeAll)
	NumProcs   int     `json:"num_procs"`   // count of matched PIDs
	PIDs       []int   `json:"pids"`        // matched PIDs (capped for display sanity)
	CPUPercent float64 `json:"cpu_percent"` // whole-machine %, summed across the group
	RSS        uint64  `json:"rss"`         // resident set size, bytes, summed
	Note       string  `json:"note,omitempty"`
}

// procGroup is one monitored group plus the predicate that decides whether a
// given process (by PID and full cmdline) belongs to it.
type procGroup struct {
	key   string
	label string
	match func(pid int, cmdline string) bool
}

// procSampler remembers the previous per-PID busy jiffies and the aggregate
// /proc/stat total, so each poll reports CPU usage since the last poll rather
// than since the process started.
type procSampler struct {
	mu        sync.Mutex
	prevByPID map[int]uint64 // pid -> utime+stime jiffies
	prevTotal uint64         // aggregate /proc/stat total jiffies
	hasPrev   bool
}

var procState procSampler

// gameMatchOnce caches the compiled game-client regexp (and the source pattern
// it was built from) so we don't recompile on every 3s poll.
var (
	gameMatchOnce sync.Mutex
	gameMatchSrc  string
	gameMatchRe   *regexp.Regexp
)

// collectProcessStats fills s.Processes with one entry per monitored group
// (main app, proxy, game client). Called from collectSystemStats.
func collectProcessStats(s *SystemStats) {
	groups := buildProcGroups()

	procState.mu.Lock()
	defer procState.mu.Unlock()

	// On the first poll of this process there is no previous sample to diff
	// against, so a short priming snapshot lets us report a real CPU% on the
	// very first render instead of a misleading 0% (mirrors readCPUUsage).
	if !procState.hasPrev {
		primeJ, primeTotal := scanProcs(groups, nil)
		time.Sleep(150 * time.Millisecond)
		procState.prevByPID = primeJ
		procState.prevTotal = primeTotal
		procState.hasPrev = true
	}

	stats := make([]*ProcessStat, len(groups))
	for i, g := range groups {
		stats[i] = &ProcessStat{Key: g.key, Label: g.label}
	}
	curJ, curTotal := scanProcs(groups, stats)

	totalDelta := float64(curTotal - procState.prevTotal)
	for _, st := range stats {
		if totalDelta <= 0 {
			break
		}
		var busy uint64
		for _, pid := range st.PIDs {
			cur := curJ[pid]
			if prev, ok := procState.prevByPID[pid]; ok && cur >= prev {
				busy += cur - prev
			}
		}
		st.CPUPercent = clampPct(float64(busy) / totalDelta * 100)
	}

	procState.prevByPID = curJ
	procState.prevTotal = curTotal

	// The proxy runs inside this process under the single-process default
	// (ModeAll): there is no separate PID to find, so report it as in-process
	// and let the UI show the app's footprint as the shared one.
	if appConfig != nil && appConfig.Mode == config.ModeAll {
		for _, st := range stats {
			if st.Key == "proxy" {
				st.Running = true
				st.InProcess = true
				st.NumProcs = 0
				st.PIDs = nil
				st.Note = "Runs inside the main app (single-process mode)"
			}
		}
	}

	out := make([]ProcessStat, len(stats))
	for i, st := range stats {
		out[i] = *st
	}
	s.Processes = out
}

// buildProcGroups returns the monitored groups in display order. The proxy
// matcher only ever fires in the split deployment (ModeApp), where the proxy is
// a separate `yufa-mt --mode=proxy` process; under ModeAll it is folded into the
// app process after the scan (see collectProcessStats).
func buildProcGroups() []procGroup {
	self := os.Getpid()

	groups := []procGroup{
		{
			key:   "app",
			label: "Main app",
			match: func(pid int, _ string) bool { return pid == self },
		},
		{
			key:   "proxy",
			label: "Proxy",
			match: func(pid int, cmdline string) bool {
				if pid == self {
					return false
				}
				return strings.Contains(cmdline, "mode=proxy") || strings.Contains(cmdline, "mode proxy")
			},
		},
	}

	if re := gameClientRegexp(); re != nil {
		groups = append(groups, procGroup{
			key:   "game",
			label: "Game client",
			match: func(pid int, cmdline string) bool {
				if pid == self || cmdline == "" {
					return false
				}
				return re.MatchString(cmdline)
			},
		})
	}

	return groups
}

// gameClientRegexp compiles (and caches) the configured game-client match
// pattern. Returns nil when monitoring is disabled (empty pattern) or the
// pattern fails to compile.
func gameClientRegexp() *regexp.Regexp {
	pattern := `Projeto_Yufa|[Rr]agexe|[Rr]agnarok`
	if appConfig != nil {
		pattern = appConfig.GameClientMatch
	}
	if pattern == "" {
		return nil
	}

	gameMatchOnce.Lock()
	defer gameMatchOnce.Unlock()
	if gameMatchRe != nil && gameMatchSrc == pattern {
		return gameMatchRe
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}
	gameMatchSrc = pattern
	gameMatchRe = re
	return re
}

// scanProcs walks /proc once: it classifies every process into the first group
// whose matcher accepts it, accumulating PID/RSS into that group's *ProcessStat
// (when stats is non-nil), and returns the busy-jiffie reading for each matched
// PID plus the aggregate /proc/stat total. Passing stats=nil does a jiffies-only
// pass (used to prime the sampler).
func scanProcs(groups []procGroup, stats []*ProcessStat) (jiffies map[int]uint64, total uint64) {
	jiffies = make(map[int]uint64)

	entries, err := os.ReadDir("/proc")
	if err != nil {
		return jiffies, 0
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // non-PID entries (e.g. /proc/stat, /proc/self)
		}
		cmdline := readCmdline(pid)
		for i := range groups {
			if !groups[i].match(pid, cmdline) {
				continue
			}
			jiffies[pid] = readPidJiffies(pid)
			if stats != nil {
				st := stats[i]
				st.Running = true
				st.NumProcs++
				if len(st.PIDs) < 16 { // cap the list; NumProcs still counts all
					st.PIDs = append(st.PIDs, pid)
				}
				st.RSS += readPidRSS(pid)
			}
			break // first matching group wins
		}
	}

	all, _ := readProcStat()
	return jiffies, all.total
}

// readCmdline returns /proc/<pid>/cmdline with its NUL separators turned into
// spaces. Empty for kernel threads (which have no cmdline) or a vanished PID.
func readCmdline(pid int) string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil || len(b) == 0 {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(string(b), "\x00", " "))
}

// readPidJiffies returns utime+stime (busy CPU jiffies) from /proc/<pid>/stat.
// The comm field (2) is wrapped in parens and may itself contain spaces or
// parens, so we split after the LAST ')': the remaining tokens start at field 3
// (state), making utime field 14 -> index 11 and stime field 15 -> index 12.
func readPidJiffies(pid int) uint64 {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	s := string(b)
	close := strings.LastIndexByte(s, ')')
	if close < 0 || close+2 >= len(s) {
		return 0
	}
	fields := strings.Fields(s[close+2:])
	if len(fields) < 13 {
		return 0
	}
	utime, _ := strconv.ParseUint(fields[11], 10, 64)
	stime, _ := strconv.ParseUint(fields[12], 10, 64)
	return utime + stime
}

// readPidRSS returns the resident set size of one process in bytes, from the
// VmRSS line of /proc/<pid>/status (kB). Matches readProcessMem's source.
func readPidRSS(pid int) uint64 {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					return v * 1024 // kB -> bytes
				}
			}
			return 0
		}
	}
	return 0
}
