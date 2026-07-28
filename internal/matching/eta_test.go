package matching

import (
	"math"
	"testing"
	"time"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
)

func TestHaversineEstimatorZeroDistanceIsZeroDuration(t *testing.T) {
	e := NewHaversineEstimator()
	loc := Location{Lat: 17.3850, Lng: 78.4867}
	eta, err := e.Estimate(loc, loc)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	if eta != 0 {
		t.Fatalf("Estimate(same point) = %v, want 0", eta)
	}
}

func TestHaversineEstimatorMatchesDistanceOverSpeed(t *testing.T) {
	e := &HaversineEstimator{SpeedKmh: 30}
	from := Location{Lat: 17.3850, Lng: 78.4867}
	to := Location{Lat: 17.4300, Lng: 78.4867} // due north, same longitude

	eta, err := e.Estimate(from, to)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}

	// ~0.045 deg latitude is close to 5km; at 30km/h that's 10 minutes.
	// Allow a wide tolerance since we're not hand-computing great-circle
	// distance here, just sanity-checking the order of magnitude and that
	// SpeedKmh is actually being used.
	wantMinutes := 10.0
	gotMinutes := eta.Minutes()
	if math.Abs(gotMinutes-wantMinutes) > 2 {
		t.Fatalf("Estimate ~= %.1f min, want close to %.1f min", gotMinutes, wantMinutes)
	}
}

func TestHaversineEstimatorFallsBackWhenSpeedUnset(t *testing.T) {
	e := &HaversineEstimator{} // SpeedKmh left at zero value
	from := Location{Lat: 17.3850, Lng: 78.4867}
	to := Location{Lat: 17.4300, Lng: 78.4867}

	eta, err := e.Estimate(from, to)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	if eta <= 0 {
		t.Fatalf("Estimate with zero SpeedKmh = %v, want a positive fallback duration", eta)
	}
}

func TestInMemoryCongestionProviderDefaultsToFreeFlow(t *testing.T) {
	p := NewInMemoryCongestionProvider()
	cell, err := h3index.CellFor(17.3850, 78.4867, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}
	if got := p.CongestionFactor(cell); got != 1.0 {
		t.Fatalf("CongestionFactor(unset cell) = %v, want 1.0", got)
	}
}

func TestCongestionAwareEstimatorScalesByFactor(t *testing.T) {
	from := Location{Lat: 17.3850, Lng: 78.4867}
	to := Location{Lat: 17.4300, Lng: 78.4867}
	cell, err := h3index.CellFor(from.Lat, from.Lng, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	provider := NewInMemoryCongestionProvider()
	provider.Set(cell, 2.0)

	estimator := NewCongestionAwareEstimator(provider, h3index.DefaultResolution)
	base, err := estimator.Base.Estimate(from, to)
	if err != nil {
		t.Fatalf("base Estimate: %v", err)
	}

	adjusted, err := estimator.Estimate(from, to)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}

	want := time.Duration(float64(base) * 2.0)
	if adjusted != want {
		t.Fatalf("Estimate with 2x congestion = %v, want %v (2x base %v)", adjusted, want, base)
	}
}
