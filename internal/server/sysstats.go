package server

import (
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// processStart marks when this process began, for reporting app uptime.
var processStart = time.Now()

// SystemStats is a point-in-time snapshot of the host machine the server runs
// on, plus a few facts about this process. All values are best-effort: on a
// platform (or container) where a particular source is unavailable, the
// corresponding field stays at its zero value rather than failing the whole
// snapshot. The admin "Machine" tab polls /admin/stats for these.
type SystemStats struct {
	Timestamp     int64      `json:"timestamp"`
	Hostname      string     `json:"hostname"`
	Kernel        string     `json:"kernel"`
	Uptime        string     `json:"uptime"`
	UptimeSeconds float64    `json:"uptime_seconds"`
	AppUptime     string     `json:"app_uptime"`
	NumCPU        int        `json:"num_cpu"`
	CPUPercent    float64    `json:"cpu_percent"`
	PerCPU        []float64  `json:"per_cpu"`
	LoadAvg       [3]float64 `json:"load_avg"`
	MemTotal      uint64     `json:"mem_total"`
	MemUsed       uint64     `json:"mem_used"`
	MemPercent    float64    `json:"mem_percent"`
	SwapTotal     uint64     `json:"swap_total"`
	SwapUsed      uint64     `json:"swap_used"`
	SwapPercent   float64    `json:"swap_percent"`
	DiskTotal     uint64     `json:"disk_total"`
	DiskUsed      uint64     `json:"disk_used"`
	DiskPercent   float64    `json:"disk_percent"`
	TempC         float64    `json:"temp_c"`
	HasTemp       bool       `json:"has_temp"`
	Procs         int        `json:"procs"`
	Goroutines    int        `json:"goroutines"`
	AppRSS        uint64     `json:"app_rss"`
	GoHeap        uint64     `json:"go_heap"`

	// Processes is per-process liveness + CPU/memory for the moving parts on
	// the host (main app, proxy, game client). See procstats.go.
	Processes []ProcessStat `json:"processes"`
}

// cpuTimes holds the cumulative jiffie counters parsed from one /proc/stat
// line (aggregate or a single core). CPU usage is the ratio of busy-time delta
// to total-time delta between two readings.
type cpuTimes struct {
	idle  uint64
	total uint64
}

// cpuSampler remembers the previous /proc/stat reading so each poll reports
// usage since the last poll rather than since boot.
type cpuSampler struct {
	mu      sync.Mutex
	prevAll cpuTimes
	prevPer []cpuTimes
	hasPrev bool
}

var cpuState cpuSampler

// adminSystemStatsHandler serves a SystemStats snapshot as JSON. The Machine
// tab polls it on an interval to drive its gauges and sparklines.
func adminSystemStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats := collectSystemStats()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(stats)
}

func collectSystemStats() SystemStats {
	var s SystemStats
	s.Timestamp = time.Now().UnixMilli()
	s.NumCPU = runtime.NumCPU()
	s.Goroutines = runtime.NumGoroutine()

	if hn, err := os.Hostname(); err == nil {
		s.Hostname = hn
	}
	s.Kernel = strings.TrimSpace(readFirstLine("/proc/sys/kernel/osrelease"))

	s.AppUptime = humanizeDuration(time.Since(processStart))

	readHostUptime(&s)
	readLoadAvgAndProcs(&s)
	readCPUUsage(&s)
	readMemInfo(&s)
	readDiskUsage(&s)
	readTemperature(&s)
	readProcessMem(&s)
	collectProcessStats(&s)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s.GoHeap = m.HeapAlloc

	return s
}

func readHostUptime(s *SystemStats) {
	line := readFirstLine("/proc/uptime")
	if line == "" {
		return
	}
	if f, err := strconv.ParseFloat(strings.Fields(line)[0], 64); err == nil {
		s.UptimeSeconds = f
		s.Uptime = humanizeDuration(time.Duration(f) * time.Second)
	}
}

func readLoadAvgAndProcs(s *SystemStats) {
	// /proc/loadavg: "0.12 0.34 0.56 1/234 5678"
	fields := strings.Fields(readFirstLine("/proc/loadavg"))
	if len(fields) >= 3 {
		for i := 0; i < 3; i++ {
			s.LoadAvg[i], _ = strconv.ParseFloat(fields[i], 64)
		}
	}
	if len(fields) >= 4 {
		if parts := strings.SplitN(fields[3], "/", 2); len(parts) == 2 {
			s.Procs, _ = strconv.Atoi(parts[1])
		}
	}
}

func readCPUUsage(s *SystemStats) {
	all, per := readProcStat()
	if all.total == 0 {
		return
	}

	cpuState.mu.Lock()
	defer cpuState.mu.Unlock()

	if !cpuState.hasPrev {
		// First call this process: take a short second sample so we can
		// report a meaningful value immediately instead of 0%.
		time.Sleep(150 * time.Millisecond)
		all2, per2 := readProcStat()
		s.CPUPercent = cpuDeltaPercent(all, all2)
		s.PerCPU = perCPUDelta(per, per2)
		cpuState.prevAll = all2
		cpuState.prevPer = per2
		cpuState.hasPrev = true
		return
	}

	s.CPUPercent = cpuDeltaPercent(cpuState.prevAll, all)
	s.PerCPU = perCPUDelta(cpuState.prevPer, per)
	cpuState.prevAll = all
	cpuState.prevPer = per
}

func cpuDeltaPercent(prev, cur cpuTimes) float64 {
	totalDelta := float64(cur.total - prev.total)
	idleDelta := float64(cur.idle - prev.idle)
	if totalDelta <= 0 {
		return 0
	}
	pct := (totalDelta - idleDelta) / totalDelta * 100
	return clampPct(pct)
}

func perCPUDelta(prev, cur []cpuTimes) []float64 {
	if len(prev) == 0 || len(cur) == 0 {
		return nil
	}
	n := len(cur)
	if len(prev) < n {
		n = len(prev)
	}
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = cpuDeltaPercent(prev[i], cur[i])
	}
	return out
}

// readProcStat parses /proc/stat, returning the aggregate "cpu" line and one
// entry per "cpuN" core line.
func readProcStat() (cpuTimes, []cpuTimes) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuTimes{}, nil
	}
	defer f.Close()

	var all cpuTimes
	var per []cpuTimes
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cpu") {
			break // cpu lines are first in /proc/stat
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		// fields[1:] = user nice system idle iowait irq softirq steal ...
		var total uint64
		var idle uint64
		for i := 1; i < len(fields); i++ {
			v, err := strconv.ParseUint(fields[i], 10, 64)
			if err != nil {
				continue
			}
			total += v
			if i == 4 || i == 5 { // idle + iowait
				idle += v
			}
		}
		ct := cpuTimes{idle: idle, total: total}
		if fields[0] == "cpu" {
			all = ct
		} else {
			per = append(per, ct)
		}
	}
	if err := sc.Err(); err != nil {
		log.Printf("[W] [SysStats] Error scanning /proc/stat: %v", err)
	}
	return all, per
}

func readMemInfo(s *SystemStats) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return
	}
	defer f.Close()

	vals := map[string]uint64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		vals[key] = v * 1024 // kB -> bytes
	}
	if err := sc.Err(); err != nil {
		log.Printf("[W] [SysStats] Error scanning /proc/meminfo: %v", err)
	}

	s.MemTotal = vals["MemTotal"]
	if s.MemTotal > 0 {
		avail := vals["MemAvailable"]
		if avail > s.MemTotal {
			avail = s.MemTotal
		}
		s.MemUsed = s.MemTotal - avail
		s.MemPercent = clampPct(float64(s.MemUsed) / float64(s.MemTotal) * 100)
	}

	s.SwapTotal = vals["SwapTotal"]
	if s.SwapTotal > 0 {
		s.SwapUsed = s.SwapTotal - vals["SwapFree"]
		s.SwapPercent = clampPct(float64(s.SwapUsed) / float64(s.SwapTotal) * 100)
	}
}

func readDiskUsage(s *SystemStats) {
	var st syscall.Statfs_t
	if err := syscall.Statfs("/", &st); err != nil {
		return
	}
	bsize := uint64(st.Bsize)
	s.DiskTotal = st.Blocks * bsize
	free := st.Bavail * bsize
	if free > s.DiskTotal {
		free = s.DiskTotal
	}
	s.DiskUsed = s.DiskTotal - free
	if s.DiskTotal > 0 {
		s.DiskPercent = clampPct(float64(s.DiskUsed) / float64(s.DiskTotal) * 100)
	}
}

// readTemperature reads the hottest CPU/board thermal zone, in degrees C.
// Many VMs and containers expose no thermal zones; HasTemp stays false there.
func readTemperature(s *SystemStats) {
	entries, err := os.ReadDir("/sys/class/thermal")
	if err != nil {
		return
	}
	var maxMilli int64 = -1
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "thermal_zone") {
			continue
		}
		raw := readFirstLine("/sys/class/thermal/" + e.Name() + "/temp")
		if raw == "" {
			continue
		}
		v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			continue
		}
		if v > maxMilli {
			maxMilli = v
		}
	}
	if maxMilli >= 0 {
		s.TempC = float64(maxMilli) / 1000.0
		s.HasTemp = true
	}
}

// readProcessMem reads this process's resident set size from /proc/self/status.
func readProcessMem(s *SystemStats) {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					s.AppRSS = v * 1024 // kB -> bytes
				}
			}
			return
		}
	}
	if err := sc.Err(); err != nil {
		log.Printf("[W] [SysStats] Error scanning /proc/self/status: %v", err)
	}
}

func readFirstLine(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	if i := strings.IndexByte(string(b), '\n'); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}

func clampPct(p float64) float64 {
	if p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}

func humanizeDuration(d time.Duration) string {
	if d < time.Minute {
		return strconv.Itoa(int(d.Seconds())) + "s"
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60
	var b strings.Builder
	if days > 0 {
		b.WriteString(strconv.Itoa(days))
		b.WriteString("d ")
	}
	if days > 0 || hours > 0 {
		b.WriteString(strconv.Itoa(hours))
		b.WriteString("h ")
	}
	b.WriteString(strconv.Itoa(mins))
	b.WriteString("m")
	return b.String()
}
