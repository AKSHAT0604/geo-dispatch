package offers

import (
	"context"
	"fmt"
	"time"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/matching"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// Reconciler finds trips left in OFFERED state with no live offer -
// because the node running their reoffer loop crashed, or ownership of
// their cell changed hands before it finished - and resumes them. Redis
// is the source of truth for which trips are mid-flight; nothing about a
// trip's progress needs to survive in any one node's memory for recovery
// to work, which is what makes handoff safe: the new owner rebuilds
// everything it needs from Redis rather than from state the old owner
// held locally.
//
// Recovery re-runs candidate search and ranking from scratch rather than
// resuming from a specific round, because the original ranked candidate
// list lived only in the crashed node's memory and was never persisted -
// a full re-match against current driver availability is the correct
// thing to do anyway, since availability may have changed since the
// original attempt.
type Reconciler struct {
	Trips      *store.TripStore
	Offers     *store.OfferStore
	Drivers    *store.DriverStore
	Dispatcher *Dispatcher
	Estimator  matching.ETAEstimator
	Resolution int
	SearchCfg  matching.SearchConfig
	RankCfg    matching.RankConfig
}

// Sweep finds every OFFERED trip with no live offer and resumes dispatch
// for it, returning how many it recovered.
func (rc *Reconciler) Sweep(ctx context.Context) (recovered int, err error) {
	tripIDs, err := rc.Trips.OfferedTrips(ctx)
	if err != nil {
		return 0, fmt.Errorf("list offered trips: %w", err)
	}

	for _, tripID := range tripIDs {
		resumed, err := rc.resumeIfStuck(ctx, tripID)
		if err != nil {
			return recovered, fmt.Errorf("resume trip %s: %w", tripID, err)
		}
		if resumed {
			recovered++
		}
	}
	return recovered, nil
}

func (rc *Reconciler) resumeIfStuck(ctx context.Context, tripID string) (bool, error) {
	if _, err := rc.Offers.GetOffer(ctx, tripID); err == nil {
		return false, nil // a live offer exists; some node is actively driving it
	} else if err != store.ErrOfferNotFound {
		return false, fmt.Errorf("get offer: %w", err)
	}

	trip, err := rc.Trips.GetTrip(ctx, tripID)
	if err != nil {
		return false, fmt.Errorf("get trip: %w", err)
	}
	if trip.State != statemachine.TripOffered {
		return false, nil // resolved concurrently since the trip was listed
	}

	origin, err := h3index.CellFor(trip.OriginLat, trip.OriginLng, rc.Resolution)
	if err != nil {
		return false, fmt.Errorf("cell for trip origin: %w", err)
	}

	candidates, err := matching.FindCandidates(ctx, rc.Drivers, origin, rc.SearchCfg)
	if err != nil {
		return false, fmt.Errorf("find candidates: %w", err)
	}
	ranked, err := matching.Rank(rc.Estimator, matching.Location{Lat: trip.OriginLat, Lng: trip.OriginLng}, candidates, rc.RankCfg, time.Now())
	if err != nil {
		return false, fmt.Errorf("rank candidates: %w", err)
	}

	if _, _, err := rc.Dispatcher.Run(ctx, tripID, origin, ranked); err != nil {
		return false, fmt.Errorf("run dispatcher: %w", err)
	}
	return true, nil
}
