package surge

import (
	"fmt"
	"testing"
	"time"
)

// TestSyntheticDemandProducesRiseThenDecay is the phase 4 definition-of-done
// demo: drive synthetic demand into a single cell and show the multiplier
// rise, then decay after demand stops. It runs on simulated time (every
// RecordTrip/RecordDriver/Ratio call takes an explicit timestamp) so the
// whole rise-and-decay curve executes instantly instead of requiring real
// wall-clock waiting - see docs/SURGE.md for the captured log this test
// produces.
func TestSyntheticDemandProducesRiseThenDecay(t *testing.T) {
	const window = 2 * time.Minute
	a := NewAggregator(window)
	cfg := DefaultMultiplierConfig
	start := time.Now()

	// Five drivers stay available throughout, re-pinging every 30s so they
	// never age out of the window.
	drivers := []string{"d1", "d2", "d3", "d4", "d5"}
	pingDrivers := func(at time.Time) {
		for _, id := range drivers {
			a.RecordDriver(testCell, id, "AVAILABLE", at)
		}
	}
	pingDrivers(start)

	tripSeq := 0
	requestTrips := func(n int, at time.Time) {
		for i := 0; i < n; i++ {
			tripSeq++
			a.RecordTrip(testCell, fmt.Sprintf("trip-%d", tripSeq), "REQUESTED", at)
		}
	}

	t.Logf("t=%4ds  ratio=%.2f  multiplier=%.2fx  (baseline, no demand yet)",
		0, a.Ratio(testCell, start), a.Multiplier(testCell, start, cfg))

	// Ramp: demand arrives in bursts every 10s for 90s while supply holds
	// steady, driving the ratio - and multiplier - up.
	for elapsed := 10 * time.Second; elapsed <= 90*time.Second; elapsed += 10 * time.Second {
		now := start.Add(elapsed)
		requestTrips(3, now)
		if elapsed%(30*time.Second) == 0 {
			pingDrivers(now)
		}
		t.Logf("t=%4.0fs  ratio=%.2f  multiplier=%.2fx  (demand ramping)",
			elapsed.Seconds(), a.Ratio(testCell, now), a.Multiplier(testCell, now, cfg))
	}

	peak := a.Multiplier(testCell, start.Add(90*time.Second), cfg)
	if peak <= cfg.Min {
		t.Fatalf("multiplier at peak demand = %.2fx, want > baseline %.2fx", peak, cfg.Min)
	}

	// Demand stops: no more requests are recorded. Supply keeps pinging so
	// the decay is driven purely by open trips aging out of the window.
	for elapsed := 100 * time.Second; elapsed <= 240*time.Second; elapsed += 20 * time.Second {
		now := start.Add(elapsed)
		pingDrivers(now)
		t.Logf("t=%4.0fs  ratio=%.2f  multiplier=%.2fx  (demand stopped, decaying)",
			elapsed.Seconds(), a.Ratio(testCell, now), a.Multiplier(testCell, now, cfg))
	}

	final := a.Multiplier(testCell, start.Add(240*time.Second), cfg)
	if final != cfg.Min {
		t.Fatalf("multiplier after decay = %.2fx, want back to baseline %.2fx", final, cfg.Min)
	}
}
