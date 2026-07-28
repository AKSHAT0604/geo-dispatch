package matching

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/h3-go/v4"
)

// Match runs candidate search followed by ranking: the full pipeline disco
// calls to turn a ride request's origin cell and location into an ordered
// list of drivers to offer the trip to, best first.
func Match(ctx context.Context, lookup DriverLookup, estimator ETAEstimator, origin h3.Cell, riderLocation Location, searchCfg SearchConfig, rankCfg RankConfig) ([]RankedCandidate, error) {
	candidates, err := FindCandidates(ctx, lookup, origin, searchCfg)
	if err != nil {
		return nil, fmt.Errorf("find candidates: %w", err)
	}
	return Rank(estimator, riderLocation, candidates, rankCfg, time.Now())
}
