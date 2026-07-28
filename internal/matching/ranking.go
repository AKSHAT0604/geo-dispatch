package matching

import (
	"fmt"
	"sort"
	"time"

	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// RankConfig controls how ETA and driver fairness combine into a ranking
// score.
type RankConfig struct {
	// FairnessWeight is the ETA discount, in seconds, per minute a driver
	// has been idle. Zero disables the fairness term entirely, reducing
	// ranking to pure ETA.
	FairnessWeight float64
}

// DefaultRankConfig applies a small fairness nudge: a driver idle for ten
// minutes gets a ten-second ETA discount. Real dispatch balances rider wait
// against driver earnings equity - without this term the same
// centrally-located drivers absorb every trip while drivers on the edge of
// a cell's coverage sit idle.
var DefaultRankConfig = RankConfig{FairnessWeight: 1.0}

// RankedCandidate pairs a driver with its estimated pickup time and the
// score it was ranked by. Lower score ranks first.
type RankedCandidate struct {
	Driver *store.DriverRecord
	ETA    time.Duration
	Score  float64
}

// Rank orders candidates by ETA to riderLocation, discounted by a fairness
// bonus for drivers idle longer. Ties (equal score) break on DriverID so
// the ordering is deterministic regardless of the input order candidates
// arrived in - which, coming from a Redis set via FindCandidates, is not
// itself guaranteed stable.
func Rank(estimator ETAEstimator, riderLocation Location, candidates []*store.DriverRecord, cfg RankConfig, now time.Time) ([]RankedCandidate, error) {
	ranked := make([]RankedCandidate, 0, len(candidates))
	for _, d := range candidates {
		eta, err := estimator.Estimate(Location{Lat: d.Lat, Lng: d.Lng}, riderLocation)
		if err != nil {
			return nil, fmt.Errorf("estimate eta for driver %s: %w", d.DriverID, err)
		}

		var idleMinutes float64
		if !d.AvailableSince.IsZero() {
			idleMinutes = now.Sub(d.AvailableSince).Minutes()
			if idleMinutes < 0 {
				idleMinutes = 0
			}
		}

		score := eta.Seconds() - cfg.FairnessWeight*idleMinutes
		ranked = append(ranked, RankedCandidate{Driver: d, ETA: eta, Score: score})
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].Score != ranked[j].Score {
			return ranked[i].Score < ranked[j].Score
		}
		return ranked[i].Driver.DriverID < ranked[j].Driver.DriverID
	})
	return ranked, nil
}
