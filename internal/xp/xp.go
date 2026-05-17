// Package xp owns Ragnarok Online base- and job-level XP tables and
// provides O(1) lookups for total XP required between two levels.
package xp

import "log"

// BaseDelta is XP needed to advance from base level X to X+1.
var BaseDelta = map[int]int64{
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

// JobDelta is XP needed to advance from job level X to X+1.
var JobDelta = map[int]int64{
	1: 184, 2: 284, 3: 348, 4: 603, 5: 887, 6: 1096, 7: 1598, 8: 2540, 9: 3676,
	10: 4290, 11: 4946, 12: 6679, 13: 9492, 14: 12770, 15: 14344, 16: 16005, 17: 20642, 18: 27434, 19: 35108,
	20: 38577, 21: 42206, 22: 52708, 23: 66971, 24: 82688, 25: 89544, 26: 96669, 27: 117821, 28: 144921, 29: 174201,
	30: 186677, 31: 199584, 32: 238617, 33: 286366, 34: 337147, 35: 358435, 36: 380376, 37: 447685, 38: 526989, 39: 610246,
	40: 644736, 41: 793535, 42: 921810, 43: 1106758, 44: 1260955, 45: 1487304, 46: 1557657, 47: 1990632, 48: 2083386, 49: 2125053,
}

// BaseCumulative[L] is total XP from level 1 to reach level L.
var BaseCumulative = make(map[int]int64)

// JobCumulative[L] is total XP from job level 1 to reach job level L.
var JobCumulative = make(map[int]int64)

// BaseTable[i] is the XP delta from level i+1 to i+2 (1-indexed via i+1).
var BaseTable []int64

// JobTable[i] is the XP delta from job level i+1 to i+2.
var JobTable []int64

func init() {
	BaseCumulative[1] = 0
	var cum int64
	for i := 1; i <= 98; i++ {
		d, ok := BaseDelta[i]
		if !ok {
			log.Fatalf("[F] [XP] missing base delta for level %d", i)
		}
		cum += d
		BaseCumulative[i+1] = cum
	}

	JobCumulative[1] = 0
	cum = 0
	for i := 1; i <= 49; i++ {
		d, ok := JobDelta[i]
		if !ok {
			log.Fatalf("[F] [XP] missing job delta for level %d", i)
		}
		cum += d
		JobCumulative[i+1] = cum
	}

	BaseTable = make([]int64, 98)
	for i := 0; i < 98; i++ {
		BaseTable[i] = BaseDelta[i+1]
	}
	JobTable = make([]int64, 49)
	for i := 0; i < 49; i++ {
		JobTable[i] = JobDelta[i+1]
	}
}

// Total returns the total XP needed to reach level (with the given
// percentage of progress into that level) from level 1.
func Total(cumulative map[int]int64, delta []int64, level int, percentage float64) int64 {
	base, ok := cumulative[level]
	if !ok {
		return 0
	}
	var extra int64
	idx := level - 1
	if idx >= 0 && idx < len(delta) && percentage > 0 {
		extra = int64((percentage / 100.0) * float64(delta[idx]))
	}
	return base + extra
}
