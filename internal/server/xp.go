package server

import (
	"net/http"
	"strconv"

	"github.com/denislee/yufa-mt/internal/xp"
)

// xpCalculatorHandler handles the XP calculator page request and form submission.
func xpCalculatorHandler(w http.ResponseWriter, r *http.Request) {
	data := XPCalculatorPageData{
		PageTitle:      "XP Calculator",
		LastScrapeTime: GetLastScrapeTime(),
		StartLevel:     1,
		StartPerc:      0,
		EndLevel:       99,
		EndPerc:        0,
		TimeHours:      0,
		TimeMinutes:    0,
		CalcType:       "base",
	}

	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			data.ErrorMessage = "Error parsing form."
			renderTemplate(w, r, "xp_calculator.html", data)
			return
		}

		data.StartLevel, _ = strconv.Atoi(r.FormValue("start_level"))
		data.StartPerc, _ = strconv.ParseFloat(r.FormValue("start_perc"), 64)
		data.EndLevel, _ = strconv.Atoi(r.FormValue("end_level"))
		data.EndPerc, _ = strconv.ParseFloat(r.FormValue("end_perc"), 64)
		data.TimeHours, _ = strconv.Atoi(r.FormValue("time_hours"))
		data.TimeMinutes, _ = strconv.Atoi(r.FormValue("time_minutes"))
		data.CalcType = r.FormValue("calc_type")

		var cumulative map[int]int64
		var delta []int64
		var maxLevel int

		if data.CalcType == "job" {
			cumulative = xp.JobCumulative
			delta = xp.JobTable
			maxLevel = len(xp.JobTable) + 1
		} else {
			cumulative = xp.BaseCumulative
			delta = xp.BaseTable
			maxLevel = len(xp.BaseTable) + 1
		}

		if data.StartLevel < 1 || data.StartLevel > maxLevel ||
			data.EndLevel < 1 || data.EndLevel > maxLevel ||
			data.EndLevel < data.StartLevel {
			data.ErrorMessage = "Invalid level range."
			renderTemplate(w, r, "xp_calculator.html", data)
			return
		}

		startXP := xp.Total(cumulative, delta, data.StartLevel, data.StartPerc)
		endXP := xp.Total(cumulative, delta, data.EndLevel, data.EndPerc)

		data.TotalXPGained = endXP - startXP
		if data.TotalXPGained < 0 {
			data.ErrorMessage = "Final level/XP is lower than initial level/XP."
			renderTemplate(w, r, "xp_calculator.html", data)
			return
		}

		totalMinutes := (data.TimeHours * 60) + data.TimeMinutes
		if totalMinutes > 0 {
			totalHours := float64(totalMinutes) / 60.0
			data.XPPerHour = int64(float64(data.TotalXPGained) / totalHours)
		}

		data.ShowResults = true
	}

	renderTemplate(w, r, "xp_calculator.html", data)
}
