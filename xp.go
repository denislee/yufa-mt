package main

import (
	"log"
	"net/http"
	"strconv"
)

// levelXPDelta stores the XP required to go from Level X to Level X+1
// Data is derived from the provided level_table.txt
var levelXPDelta = map[int]int64{
	1: 9, 2: 16, 3: 25, 4: 36, 5: 77, 6: 112, 7: 153, 8: 200, 9: 253,
	10: 320, 11: 385, 12: 490, 13: 585, 14: 700, 15: 830, 16: 970, 17: 1120, 18: 1260, 19: 1420,
	20: 1620, 21: 1860, 22: 1990, 23: 2240, 24: 2504, 25: 2950, 26: 3426, 27: 3934, 28: 4474, 29: 6889,
	30: 7995, 31: 9174, 32: 10425, 33: 11748, 34: 13967, 35: 15775, 36: 17678, 37: 19677, 38: 21773, 39: 30543,
	40: 34212, 41: 38065, 42: 42102, 43: 46323, 44: 53026, 45: 58419, 46: 64041, 47: 69892, 48: 75973, 49: 102468,
	50: 115254, 51: 128692, 52: 142784, 53: 157528, 54: 178184, 55: 196300, 56: 215198, 57: 234879, 58: 255341, 59: 330188,
	60: 365914, 61: 403224, 62: 442116, 63: 482590, 64: 536948, 65: 585191, 66: 635278, 67: 687211, 68: 740988, 69: 925400,
	70: 1473746, 71: 1594058, 72: 1718928, 73: 1848355, 74: 1982340, 75: 2230113, 76: 2386162, 77: 2547417, 78: 2713878, 79: 3206160,
	80: 3681024, 81: 4022472, 82: 4377024, 83: 4744680, 84: 5125440, 85: 5767272, 86: 6204000, 87: 6655464, 88: 7121664, 89: 7602600,
	90: 9738720, 91: 11649960, 92: 13643520, 93: 18339300, 94: 23836800, 95: 35658000, 96: 48687000, 97: 58135000, 98: 99999998,
}

// --- REPLACED: Job Level XP Data (from xp-job.txt) ---

var jobLevelXPDelta = map[int]int64{
	1:  184,
	2:  284,
	3:  348,
	4:  603,
	5:  887,
	6:  1096,
	7:  1598,
	8:  2540,
	9:  3676,
	10: 4290,
	11: 4946,
	12: 6679,
	13: 9492,
	14: 12770,
	15: 14344,
	16: 16005,
	17: 20642,
	18: 27434,
	19: 35108,
	20: 38577,
	21: 42206,
	22: 52708,
	23: 66971,
	24: 82688,
	25: 89544,
	26: 96669,
	27: 117821,
	28: 144921,
	29: 174201,
	30: 186677,
	31: 199584,
	32: 238617,
	33: 286366,
	34: 337147,
	35: 358435,
	36: 380376,
	37: 447685,
	38: 526989,
	39: 610246,
	40: 644736,
	41: 793535,
	42: 921810,
	43: 1106758,
	44: 1260955,
	45: 1487304,
	46: 1557657,
	47: 1990632,
	48: 2083386,
	49: 2125053,
}

// levelXPCumulative stores the TOTAL XP required to reach Level X (at 0.0%)
var levelXPCumulative = make(map[int]int64)

// --- ADDED: Job Level Cumulative Map ---
var jobLevelXPCumulative = make(map[int]int64)

// --- ADDED: Slices for the calculator ---
var baseXPTable []int64
var jobXPTable []int64

// init function to populate the cumulative map
// This will run automatically as it's part of the 'main' package
func init() {
	log.Println("[I] [XP] Populating cumulative XP table...")
	var currentCumulativeXP int64 = 0
	levelXPCumulative[1] = 0 // Base case

	// Max level is 99
	for i := 1; i <= 98; i++ {
		xpDelta, ok := levelXPDelta[i]
		if !ok {
			log.Fatalf("[F] [XP] Missing XP delta for level %d", i)
		}
		currentCumulativeXP += xpDelta
		levelXPCumulative[i+1] = currentCumulativeXP
	}
	log.Println("[I] [XP] Cumulative XP table populated.")

	// --- ADDED: Populate Job XP Cumulative Table ---
	log.Println("[I] [XP] Populating cumulative Job XP table...")
	var currentJobCumulativeXP int64 = 0
	jobLevelXPCumulative[1] = 0 // Base case

	// Max job level is 50
	for i := 1; i <= 49; i++ {
		jobXPDelt, ok := jobLevelXPDelta[i]
		if !ok {
			log.Fatalf("[F] [XP] Missing Job XP delta for level %d", i)
		}
		currentJobCumulativeXP += jobXPDelt
		jobLevelXPCumulative[i+1] = currentJobCumulativeXP
	}
	log.Println("[I] [XP] Cumulative Job XP table populated.")
	// --- END ADDITION ---

	// --- ADDED: Populate the delta slices for the calculator ---
	// baseXPTable[0] will be XP for 1->2 (which is levelXPDelta[1])
	// Max base level is 99, so there are 98 deltas (1->2, 2->3, ... 98->99)
	baseXPTable = make([]int64, 98)
	for i := 0; i < 98; i++ {
		baseXPTable[i] = levelXPDelta[i+1]
	}

	// jobXPTable[0] will be XP for 1->2 (which is jobLevelXPDelta[1])
	// Max job level is 50, so there are 49 deltas (1->2, 2->3, ... 49->50)
	jobXPTable = make([]int64, 49)
	for i := 0; i < 49; i++ {
		jobXPTable[i] = jobLevelXPDelta[i+1]
	}
	log.Println("[I] [XP] Delta slices populated.")
	// --- END ADDITION ---
}

// xpCalculatorHandler handles the XP calculator page request and form submission.
func xpCalculatorHandler(w http.ResponseWriter, r *http.Request) {
	data := XPCalculatorPageData{
		PageTitle:      "XP Calculator",
		LastScrapeTime: GetLastScrapeTime(),
		// Default form values
		StartLevel:  1,
		StartPerc:   0,
		EndLevel:    99,
		EndPerc:     0,
		TimeHours:   0,
		TimeMinutes: 0,
		CalcType:    "base", // Default to Base Level
	}

	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			data.ErrorMessage = "Error parsing form."
			renderTemplate(w, r, "xp_calculator.html", data)
			return
		}

		// Parse form values with error handling
		data.StartLevel, _ = strconv.Atoi(r.FormValue("start_level"))
		data.StartPerc, _ = strconv.ParseFloat(r.FormValue("start_perc"), 64)
		data.EndLevel, _ = strconv.Atoi(r.FormValue("end_level"))
		data.EndPerc, _ = strconv.ParseFloat(r.FormValue("end_perc"), 64)
		data.TimeHours, _ = strconv.Atoi(r.FormValue("time_hours"))
		data.TimeMinutes, _ = strconv.Atoi(r.FormValue("time_minutes"))
		data.CalcType = r.FormValue("calc_type")

		// Select the correct Cumulative Map and Delta Table based on type
		var cumulativeMap map[int]int64
		var deltaTable []int64
		var maxLevel int

		if data.CalcType == "job" {
			cumulativeMap = jobLevelXPCumulative
			deltaTable = jobXPTable
			maxLevel = len(jobXPTable) + 1 // 50
		} else {
			cumulativeMap = levelXPCumulative
			deltaTable = baseXPTable
			maxLevel = len(baseXPTable) + 1 // 99
		}

		// Validate inputs
		if data.StartLevel < 1 || data.StartLevel > maxLevel ||
			data.EndLevel < 1 || data.EndLevel > maxLevel ||
			data.EndLevel < data.StartLevel {
			data.ErrorMessage = "Invalid level range."
			renderTemplate(w, r, "xp_calculator.html", data)
			return
		}

		// Calculate total XP using O(1) optimization
		startXp := calculateXPTotal(cumulativeMap, deltaTable, data.StartLevel, data.StartPerc)
		endXp := calculateXPTotal(cumulativeMap, deltaTable, data.EndLevel, data.EndPerc)

		data.TotalXPGained = endXp - startXp
		if data.TotalXPGained < 0 {
			data.ErrorMessage = "Final level/XP is lower than initial level/XP."
			renderTemplate(w, r, "xp_calculator.html", data)
			return
		}

		// Calculate time and XP/hr
		totalMinutes := (data.TimeHours * 60) + data.TimeMinutes
		if totalMinutes > 0 {
			totalHours := float64(totalMinutes) / 60.0
			data.XPPerHour = int64(float64(data.TotalXPGained) / totalHours)
		}

		data.ShowResults = true
	}

	renderTemplate(w, r, "xp_calculator.html", data)
}

// calculateXPTotal calculates total XP using O(1) map lookups.
// It replaces the previous iterative approach.
func calculateXPTotal(cumulativeMap map[int]int64, deltaTable []int64, level int, percentage float64) int64 {
	// 1. Base Cumulative XP (O(1) lookup)
	// The map stores total XP required to REACH level X.
	baseXP, ok := cumulativeMap[level]
	if !ok {
		return 0
	}

	// 2. Add Percentage of current level
	var extraXP int64 = 0

	// deltaTable is 0-indexed. Index 0 represents XP needed for Level 1 -> 2.
	// So for Level X, we look at index X-1.
	sliceIndex := level - 1

	// Ensure we don't go out of bounds (e.g., if level is Max Level, there is no "next" level XP)
	if sliceIndex >= 0 && sliceIndex < len(deltaTable) {
		currentLevelReqXP := deltaTable[sliceIndex]
		if percentage > 0 {
			extraXP = int64((percentage / 100.0) * float64(currentLevelReqXP))
		}
	}

	return baseXP + extraXP
}
