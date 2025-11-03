package main

import (
	"fmt"
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
}

// xpCalculatorHandler handles the new XP calculator page
func xpCalculatorHandler(w http.ResponseWriter, r *http.Request) {
	data := XPCalculatorPageData{
		PageTitle:      "XP Calculator",
		LastScrapeTime: GetLastScrapeTime(), //
		ShowResults:    false,
		// Default values
		StartLevel:  1,
		StartPerc:   0.0,
		EndLevel:    1,
		EndPerc:     0.0,
		TimeHours:   1,      // <-- CHANGED
		TimeMinutes: 0,      // <-- ADDED
		CalcType:    "base", // <-- ADDED
	}

	// Handle GET request (show empty form)
	if r.Method != http.MethodPost {
		renderTemplate(w, "xp_calculator.html", data) //
		return
	}

	// Handle POST request (process form)
	if err := r.ParseForm(); err != nil {
		data.ErrorMessage = "Failed to parse form."
		renderTemplate(w, "xp_calculator.html", data) //
		return
	}

	// Parse inputs
	startLvl, _ := strconv.Atoi(r.FormValue("start_level"))
	startPerc, _ := strconv.ParseFloat(r.FormValue("start_perc"), 64)
	endLvl, _ := strconv.Atoi(r.FormValue("end_level"))
	endPerc, _ := strconv.ParseFloat(r.FormValue("end_perc"), 64)
	// --- CHANGED: Parse new time fields ---
	timeHours, _ := strconv.Atoi(r.FormValue("time_hours"))
	timeMinutes, _ := strconv.Atoi(r.FormValue("time_minutes"))
	calcType := r.FormValue("calc_type")
	// --- END CHANGE ---

	// Persist form values even if there's an error
	data.StartLevel = startLvl
	data.StartPerc = startPerc
	data.EndLevel = endLvl
	data.EndPerc = endPerc
	// --- CHANGED: Persist new fields ---
	data.TimeHours = timeHours
	data.TimeMinutes = timeMinutes
	data.CalcType = calcType
	// --- END CHANGE ---

	// --- ADDED: Select correct XP maps and max level ---
	var xpDeltaMap map[int]int64
	var xpCumulativeMap map[int]int64
	var maxLevel int

	if calcType == "job" {
		xpDeltaMap = jobLevelXPDelta
		xpCumulativeMap = jobLevelXPCumulative
		maxLevel = 50
	} else {
		xpDeltaMap = levelXPDelta
		xpCumulativeMap = levelXPCumulative
		maxLevel = 99
		calcType = "base" // Default to base if value is invalid
	}
	// --- END ADDITION ---

	// --- Validation ---
	// --- CHANGED: Use dynamic maxLevel ---
	if startLvl < 1 || startLvl > maxLevel || endLvl < 1 || endLvl > maxLevel {
		data.ErrorMessage = fmt.Sprintf("Levels must be between 1 and %d for the selected type.", maxLevel)
		renderTemplate(w, "xp_calculator.html", data) //
		return
	}
	// --- END CHANGE ---
	if startPerc < 0 || startPerc > 100 || endPerc < 0 || endPerc > 100 {
		data.ErrorMessage = "Percentage must be between 0 and 100."
		renderTemplate(w, "xp_calculator.html", data) //
		return
	}
	if endLvl < startLvl || (endLvl == startLvl && endPerc <= startPerc) {
		data.ErrorMessage = "Final level/percentage must be greater than the initial level/percentage."
		renderTemplate(w, "xp_calculator.html", data) //
		return
	}
	// --- CHANGED: Calculate total time in hours ---
	timeHoursFloat, _ := strconv.ParseFloat(r.FormValue("time_hours"), 64)
	timeMinutesFloat, _ := strconv.ParseFloat(r.FormValue("time_minutes"), 64)
	totalTimeHours := timeHoursFloat + (timeMinutesFloat / 60.0)

	if totalTimeHours <= 0 {
		totalTimeHours = 1.0 // Default to 1 hour if invalid
		data.TimeHours = 1
		data.TimeMinutes = 0
	}
	// --- END CHANGE ---

	// --- Calculation ---
	// 1. Get total XP at start
	// --- CHANGED: Use selected maps ---
	startXPCumulative, ok1 := xpCumulativeMap[startLvl]
	startXPDelt, ok2 := xpDeltaMap[startLvl]
	if !ok1 || (startLvl < maxLevel && !ok2) { // Allow max level (no delta)
		data.ErrorMessage = fmt.Sprintf("Could not find level data for level %d.", startLvl)
		renderTemplate(w, "xp_calculator.html", data) //
		return
	}
	if startLvl == maxLevel {
		startXPDelt = 0
	} // No delta XP at max level
	totalStartX := startXPCumulative + int64(float64(startXPDelt)*(startPerc/100.0))

	// 2. Get total XP at end
	endXPCumulative, ok1 := xpCumulativeMap[endLvl]
	endXPDelt, ok2 := xpDeltaMap[endLvl]
	if !ok1 || (endLvl < maxLevel && !ok2) {
		data.ErrorMessage = fmt.Sprintf("Could not find level data for level %d.", endLvl)
		renderTemplate(w, "xp_calculator.html", data) //
		return
	}
	if endLvl == maxLevel {
		endXPDelt = 0
	} // No delta XP at max level
	totalEndX := endXPCumulative + int64(float64(endXPDelt)*(endPerc/100.0))
	// --- END CHANGE ---

	// 3. Calculate results
	totalGained := totalEndX - totalStartX
	xpPerHour := int64(float64(totalGained) / totalTimeHours)

	// 4. Set data for template
	data.TotalXPGained = totalGained
	data.XPPerHour = xpPerHour
	data.ShowResults = true

	renderTemplate(w, "xp_calculator.html", data) //
}
