package matching

import (
	"fmt"
	"testing"
	"time"

	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// fixtureEstimator returns a fixed ETA per driver origin, so ranking tests
// can set up exact score relationships without depending on great-circle
// distance math.
type fixtureEstimator struct {
	etaByOrigin map[Location]time.Duration
}

func (f fixtureEstimator) Estimate(from, to Location) (time.Duration, error) {
	eta, ok := f.etaByOrigin[from]
	if !ok {
		return 0, fmt.Errorf("no fixture eta for origin %v", from)
	}
	return eta, nil
}

func driverAt(id string, loc Location, availableSince time.Time) *store.DriverRecord {
	return &store.DriverRecord{
		DriverID:       id,
		Lat:            loc.Lat,
		Lng:            loc.Lng,
		State:          statemachine.DriverAvailable,
		AvailableSince: availableSince,
	}
}

func TestRankOrdersByETAAscending(t *testing.T) {
	now := time.Now()
	locNear := Location{Lat: 1, Lng: 1}
	locFar := Location{Lat: 2, Lng: 2}

	estimator := fixtureEstimator{etaByOrigin: map[Location]time.Duration{
		locNear: 60 * time.Second,
		locFar:  300 * time.Second,
	}}

	candidates := []*store.DriverRecord{
		driverAt("far", locFar, now),
		driverAt("near", locNear, now),
	}

	ranked, err := Rank(estimator, Location{}, candidates, RankConfig{FairnessWeight: 0}, now)
	if err != nil {
		t.Fatalf("Rank: %v", err)
	}
	if len(ranked) != 2 || ranked[0].Driver.DriverID != "near" || ranked[1].Driver.DriverID != "far" {
		t.Fatalf("Rank order = %v, want [near far]", ids(ranked))
	}
}

func TestRankBreaksTiesByDriverID(t *testing.T) {
	now := time.Now()
	loc := Location{Lat: 1, Lng: 1}
	estimator := fixtureEstimator{etaByOrigin: map[Location]time.Duration{loc: 90 * time.Second}}

	// All three candidates share the same origin location, so they get an
	// identical ETA and, with fairness disabled, an identical score.
	candidates := []*store.DriverRecord{
		driverAt("charlie", loc, now),
		driverAt("alpha", loc, now),
		driverAt("bravo", loc, now),
	}

	ranked, err := Rank(estimator, Location{}, candidates, RankConfig{FairnessWeight: 0}, now)
	if err != nil {
		t.Fatalf("Rank: %v", err)
	}
	want := []string{"alpha", "bravo", "charlie"}
	if got := ids(ranked); !equalIDs(got, want) {
		t.Fatalf("Rank tie order = %v, want %v", got, want)
	}
}

func TestRankFairnessLetsIdleDriverOvertakeCloserOne(t *testing.T) {
	now := time.Now()
	locFresh := Location{Lat: 1, Lng: 1}
	locIdle := Location{Lat: 2, Lng: 2}

	estimator := fixtureEstimator{etaByOrigin: map[Location]time.Duration{
		locFresh: 144 * time.Second, // closer driver, just went available
		locIdle:  158 * time.Second, // slightly farther, but idle a long time
	}}

	candidates := []*store.DriverRecord{
		driverAt("fresh", locFresh, now),                    // idle 0 minutes
		driverAt("idle", locIdle, now.Add(-20*time.Minute)), // idle 20 minutes
	}

	// FairnessWeight=1.0: 20 idle minutes discount the score by 20 seconds,
	// enough to overcome the 14 second ETA gap (158 - 20 = 138 < 144).
	ranked, err := Rank(estimator, Location{}, candidates, RankConfig{FairnessWeight: 1.0}, now)
	if err != nil {
		t.Fatalf("Rank: %v", err)
	}
	if ranked[0].Driver.DriverID != "idle" {
		t.Fatalf("Rank order = %v, want idle driver first", ids(ranked))
	}

	// With fairness disabled, the nominally-closer driver wins instead.
	ranked, err = Rank(estimator, Location{}, candidates, RankConfig{FairnessWeight: 0}, now)
	if err != nil {
		t.Fatalf("Rank: %v", err)
	}
	if ranked[0].Driver.DriverID != "fresh" {
		t.Fatalf("Rank order with fairness disabled = %v, want fresh driver first", ids(ranked))
	}
}

func ids(ranked []RankedCandidate) []string {
	out := make([]string, len(ranked))
	for i, r := range ranked {
		out[i] = r.Driver.DriverID
	}
	return out
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
