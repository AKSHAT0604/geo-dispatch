package surge

import (
	"math"
	"testing"
	"time"

	"github.com/uber/h3-go/v4"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
)

var testCell = mustCell(17.3850, 78.4867)

func mustCell(lat, lng float64) h3.Cell {
	c, err := h3index.CellFor(lat, lng, h3index.DefaultResolution)
	if err != nil {
		panic(err)
	}
	return c
}

// cell returns the single fixed test cell every test in this file uses.
func cell() h3.Cell { return testCell }

func TestRatioIsZeroWithNoOpenTrips(t *testing.T) {
	a := NewAggregator(time.Minute)
	now := time.Now()
	a.RecordDriver(cell(), "d1", "AVAILABLE", now)
	if r := a.Ratio(cell(), now); r != 0 {
		t.Fatalf("Ratio with no open trips = %v, want 0", r)
	}
}

func TestRatioIsInfWithOpenTripsAndNoDrivers(t *testing.T) {
	a := NewAggregator(time.Minute)
	now := time.Now()
	a.RecordTrip(cell(), "trip-1", "REQUESTED", now)
	if r := a.Ratio(cell(), now); !math.IsInf(r, 1) {
		t.Fatalf("Ratio with demand and no supply = %v, want +Inf", r)
	}
}

func TestRatioReflectsOpenTripsOverAvailableDrivers(t *testing.T) {
	a := NewAggregator(time.Minute)
	now := time.Now()

	for _, id := range []string{"d1", "d2"} {
		a.RecordDriver(cell(), id, "AVAILABLE", now)
	}
	for _, id := range []string{"t1", "t2", "t3", "t4"} {
		a.RecordTrip(cell(), id, "REQUESTED", now)
	}

	if r := a.Ratio(cell(), now); r != 2.0 {
		t.Fatalf("Ratio = %v, want 2.0 (4 trips / 2 drivers)", r)
	}
}

func TestRecordTripClosesOnMatchedOrUnfulfilled(t *testing.T) {
	a := NewAggregator(time.Minute)
	now := time.Now()

	a.RecordDriver(cell(), "d1", "AVAILABLE", now)
	a.RecordTrip(cell(), "t1", "REQUESTED", now)
	a.RecordTrip(cell(), "t2", "REQUESTED", now)
	if r := a.Ratio(cell(), now); r != 2.0 {
		t.Fatalf("Ratio after 2 requests = %v, want 2.0", r)
	}

	a.RecordTrip(cell(), "t1", "MATCHED", now)
	if r := a.Ratio(cell(), now); r != 1.0 {
		t.Fatalf("Ratio after one match = %v, want 1.0", r)
	}

	a.RecordTrip(cell(), "t2", "UNFULFILLED", now)
	if r := a.Ratio(cell(), now); r != 0 {
		t.Fatalf("Ratio after all resolved = %v, want 0", r)
	}
}

func TestRecordDriverRemovesOnNonAvailableState(t *testing.T) {
	a := NewAggregator(time.Minute)
	now := time.Now()

	a.RecordDriver(cell(), "d1", "AVAILABLE", now)
	a.RecordTrip(cell(), "t1", "REQUESTED", now)
	if r := a.Ratio(cell(), now); r != 1.0 {
		t.Fatalf("Ratio = %v, want 1.0", r)
	}

	a.RecordDriver(cell(), "d1", "OFFERED", now)
	if r := a.Ratio(cell(), now); !math.IsInf(r, 1) {
		t.Fatalf("Ratio after driver goes OFFERED = %v, want +Inf (no available supply)", r)
	}
}

func TestRatioOnlyCountsEntriesWithinWindow(t *testing.T) {
	a := NewAggregator(time.Minute)
	t0 := time.Now()

	a.RecordDriver(cell(), "d1", "AVAILABLE", t0)
	a.RecordTrip(cell(), "t1", "REQUESTED", t0)

	stillFresh := t0.Add(30 * time.Second)
	if r := a.Ratio(cell(), stillFresh); r != 1.0 {
		t.Fatalf("Ratio within window = %v, want 1.0", r)
	}

	expired := t0.Add(90 * time.Second)
	if r := a.Ratio(cell(), expired); r != 0 {
		t.Fatalf("Ratio after window elapsed = %v, want 0 (both entries aged out)", r)
	}
}

func TestPruneRemovesStaleEntriesAndEmptyCells(t *testing.T) {
	a := NewAggregator(time.Minute)
	t0 := time.Now()

	a.RecordTrip(cell(), "t1", "REQUESTED", t0)
	if len(a.Cells()) != 1 {
		t.Fatalf("Cells() = %d, want 1 before prune", len(a.Cells()))
	}

	a.Prune(t0.Add(90 * time.Second))
	if len(a.Cells()) != 0 {
		t.Fatalf("Cells() = %d, want 0 after pruning a fully-stale cell", len(a.Cells()))
	}
}

func TestMultiplierConfigClampsAndSteps(t *testing.T) {
	cfg := DefaultMultiplierConfig
	cases := []struct {
		ratio float64
		want  float64
	}{
		{0, 1.0},
		{1.0, 1.0},
		{1.5, 1.2},
		{2.9, 1.2},
		{3.0, 1.75},
		{5.0, 2.5},
		{8.0, 3.0},
		{1000.0, 3.0},
		{math.Inf(1), 3.0},
	}
	for _, c := range cases {
		if got := cfg.Multiplier(c.ratio); got != c.want {
			t.Errorf("Multiplier(%v) = %v, want %v", c.ratio, got, c.want)
		}
	}
}
