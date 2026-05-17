package xp

import "testing"

func TestTotalBaseAtLevel1IsZero(t *testing.T) {
	if got := Total(BaseCumulative, BaseTable, 1, 0); got != 0 {
		t.Errorf("Total at level 1 / 0%% = %d, want 0", got)
	}
}

func TestTotalBaseLevel2EqualsFirstDelta(t *testing.T) {
	if got := Total(BaseCumulative, BaseTable, 2, 0); got != BaseDelta[1] {
		t.Errorf("Total at level 2 = %d, want %d", got, BaseDelta[1])
	}
}

func TestTotalAccumulatesAcrossLevels(t *testing.T) {
	want := BaseDelta[1] + BaseDelta[2] + BaseDelta[3]
	if got := Total(BaseCumulative, BaseTable, 4, 0); got != want {
		t.Errorf("Total at level 4 = %d, want %d", got, want)
	}
}

func TestTotalHalfwayThroughCurrentLevel(t *testing.T) {
	// 50% of level 5 means halfway between 5 (0%) and 6 (0%).
	base := Total(BaseCumulative, BaseTable, 5, 0)
	halfDelta := BaseDelta[5] / 2
	got := Total(BaseCumulative, BaseTable, 5, 50)
	if got != base+halfDelta {
		t.Errorf("Total at level 5 / 50%% = %d, want %d", got, base+halfDelta)
	}
}

func TestTotalUnknownLevelReturnsZero(t *testing.T) {
	if got := Total(BaseCumulative, BaseTable, 9999, 0); got != 0 {
		t.Errorf("Total at unknown level = %d, want 0", got)
	}
}

func TestJobTotalAtLevel1IsZero(t *testing.T) {
	if got := Total(JobCumulative, JobTable, 1, 0); got != 0 {
		t.Errorf("job Total at level 1 = %d, want 0", got)
	}
}

func TestJobTotalLevel2EqualsFirstDelta(t *testing.T) {
	if got := Total(JobCumulative, JobTable, 2, 0); got != JobDelta[1] {
		t.Errorf("job Total at level 2 = %d, want %d", got, JobDelta[1])
	}
}
