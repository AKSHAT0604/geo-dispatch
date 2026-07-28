package offers

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/h3-go/v4"

	"github.com/AKSHAT0604/geo-dispatch/internal/events"
	"github.com/AKSHAT0604/geo-dispatch/internal/matching"
	"github.com/AKSHAT0604/geo-dispatch/internal/metrics"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// Config bounds the offer/reoffer loop.
type Config struct {
	OfferWindow time.Duration // how long a driver has to respond
	MaxRounds   int           // total offers attempted before giving up
	BaseBackoff time.Duration // delay before the first reoffer
	MaxBackoff  time.Duration // backoff ceiling
}

// DefaultConfig matches the phase 3 spec: a 15s offer window, five rounds,
// backoff starting at 1s and doubling up to a 10s ceiling.
var DefaultConfig = Config{
	OfferWindow: 15 * time.Second,
	MaxRounds:   5,
	BaseBackoff: 1 * time.Second,
	MaxBackoff:  10 * time.Second,
}

// Dispatcher runs the offer/reoffer loop for a trip against a pre-ranked
// list of candidates.
type Dispatcher struct {
	Offers    *store.OfferStore
	Trips     *store.TripStore
	Drivers   *store.DriverStore
	Hub       *ResponseHub
	Publisher events.Publisher
	Cfg       Config
}

// NewDispatcher returns a Dispatcher. publisher may be nil, in which case
// events are discarded (events.NoopPublisher).
func NewDispatcher(offerStore *store.OfferStore, tripStore *store.TripStore, driverStore *store.DriverStore, hub *ResponseHub, publisher events.Publisher, cfg Config) *Dispatcher {
	if publisher == nil {
		publisher = events.NoopPublisher{}
	}
	return &Dispatcher{Offers: offerStore, Trips: tripStore, Drivers: driverStore, Hub: hub, Publisher: publisher, Cfg: cfg}
}

// Run offers tripID to each ranked candidate in turn, waiting up to
// Cfg.OfferWindow for a response before moving to the next. It stops at
// the first acceptance, or after Cfg.MaxRounds offers with none accepted -
// whichever comes first - and marks the trip UNFULFILLED in that case.
// Failing to match is a valid outcome that must be handled explicitly, not
// by hanging. originCell is the trip's origin cell, carried on every
// published event so consumers never need to recompute geography.
func (d *Dispatcher) Run(ctx context.Context, tripID string, originCell h3.Cell, candidates []matching.RankedCandidate) (matched bool, driverID string, err error) {
	start := time.Now()

	trip, err := d.Trips.GetTrip(ctx, tripID)
	if err != nil {
		return false, "", fmt.Errorf("get trip: %w", err)
	}
	// Run is also the entry point a reconciliation sweep resumes a stuck
	// trip through after a node crash mid-dispatch (see Reconciler): that
	// trip is already OFFERED from its first attempt, and re-validating
	// REQUESTED -> OFFERED against the state machine would reject it. Any
	// other non-REQUESTED, non-OFFERED state means the trip already
	// reached a terminal outcome and must not be re-dispatched.
	switch trip.State {
	case statemachine.TripRequested:
		if len(candidates) == 0 {
			if err := d.Trips.MarkUnfulfilled(ctx, tripID); err != nil {
				return false, "", err
			}
			d.publishTrip(ctx, tripID, originCell, statemachine.TripUnfulfilled, "")
			d.recordOutcome(start, false, 0)
			return false, "", nil
		}
		if err := d.Trips.SetTripState(ctx, tripID, statemachine.TripOffered); err != nil {
			return false, "", fmt.Errorf("mark trip offered: %w", err)
		}
		d.publishTrip(ctx, tripID, originCell, statemachine.TripOffered, "")
	case statemachine.TripOffered:
		if len(candidates) == 0 {
			if err := d.Trips.MarkUnfulfilled(ctx, tripID); err != nil {
				return false, "", err
			}
			d.publishTrip(ctx, tripID, originCell, statemachine.TripUnfulfilled, "")
			d.recordOutcome(start, false, 0)
			return false, "", nil
		}
	default:
		return false, "", fmt.Errorf("trip %s is in terminal state %s, cannot dispatch", tripID, trip.State)
	}

	rounds := d.Cfg.MaxRounds
	if rounds > len(candidates) {
		rounds = len(candidates)
	}

	backoff := d.Cfg.BaseBackoff
	roundsAttempted := 0
	for round := 1; round <= rounds; round++ {
		roundsAttempted = round
		candidate := candidates[round-1]
		driverID := candidate.Driver.DriverID

		if err := d.Drivers.SetState(ctx, driverID, statemachine.DriverOffered); err != nil {
			return false, "", fmt.Errorf("offer to driver %s: %w", driverID, err)
		}
		if err := d.Offers.CreateOffer(ctx, tripID, driverID, round, d.Cfg.OfferWindow); err != nil {
			return false, "", fmt.Errorf("create offer round %d: %w", round, err)
		}
		d.publishOffer(ctx, tripID, driverID, round, originCell, statemachine.OfferPending)

		resp := d.awaitResponse(ctx, tripID)

		switch resp {
		case Accepted:
			if err := d.Offers.SetOfferState(ctx, tripID, driverID, statemachine.OfferAccepted); err != nil {
				return false, "", fmt.Errorf("accept offer: %w", err)
			}
			d.publishOffer(ctx, tripID, driverID, round, originCell, statemachine.OfferAccepted)
			if err := d.Drivers.SetState(ctx, driverID, statemachine.DriverEnRoute); err != nil {
				return false, "", fmt.Errorf("move driver %s en route: %w", driverID, err)
			}
			if err := d.Trips.MarkMatched(ctx, tripID, driverID); err != nil {
				return false, "", fmt.Errorf("mark trip matched: %w", err)
			}
			d.publishTrip(ctx, tripID, originCell, statemachine.TripMatched, driverID)
			d.recordOutcome(start, true, roundsAttempted)
			return true, driverID, nil

		case Declined:
			if err := d.Offers.SetOfferState(ctx, tripID, driverID, statemachine.OfferDeclined); err != nil {
				return false, "", fmt.Errorf("decline offer: %w", err)
			}
			d.publishOffer(ctx, tripID, driverID, round, originCell, statemachine.OfferDeclined)
		default: // NoResponse: the offer window elapsed.
			if err := d.Offers.SetOfferState(ctx, tripID, driverID, statemachine.OfferTimedOut); err != nil {
				return false, "", fmt.Errorf("time out offer: %w", err)
			}
			d.publishOffer(ctx, tripID, driverID, round, originCell, statemachine.OfferTimedOut)
		}

		// Declined or timed out: release the driver back to the pool. It
		// is not retried for this trip - each round advances to the next
		// pre-ranked candidate, so exclusion falls out of the loop
		// structure rather than needing an explicit exclude-list.
		if err := d.Drivers.SetState(ctx, driverID, statemachine.DriverAvailable); err != nil {
			return false, "", fmt.Errorf("release driver %s: %w", driverID, err)
		}

		if round < rounds {
			if err := sleep(ctx, backoff); err != nil {
				return false, "", err
			}
			backoff *= 2
			if backoff > d.Cfg.MaxBackoff {
				backoff = d.Cfg.MaxBackoff
			}
		}
	}

	if err := d.Trips.MarkUnfulfilled(ctx, tripID); err != nil {
		return false, "", err
	}
	d.publishTrip(ctx, tripID, originCell, statemachine.TripUnfulfilled, "")
	d.recordOutcome(start, false, roundsAttempted)
	return false, "", nil
}

// recordOutcome reports the three dispatch metrics that only make sense
// for a valid, resolved outcome (matched or unfulfilled) - not for the
// error returns elsewhere in Run, which aren't a dispatch result at all.
func (d *Dispatcher) recordOutcome(start time.Time, matched bool, roundsAttempted int) {
	metrics.MatchLatency.Observe(time.Since(start).Seconds())
	metrics.OffersPerMatch.Observe(float64(roundsAttempted))
	if matched {
		metrics.MatchesTotal.Inc()
	} else {
		metrics.UnfulfilledTotal.Inc()
	}
}

// publishTrip and publishOffer intentionally ignore publish errors beyond
// logging-worthy failure: losing an analytics event must never fail or
// stall the dispatch loop that produced it. A production build would
// route this through metrics/logging rather than dropping it silently;
// that wiring lives with the services in later phases, not in this
// package's core loop.
func (d *Dispatcher) publishTrip(ctx context.Context, tripID string, cell h3.Cell, state statemachine.TripState, driverID string) {
	_ = d.Publisher.PublishTripLifecycle(ctx, events.TripLifecycleEvent{
		TripID:    tripID,
		State:     string(state),
		Cell:      cell.String(),
		DriverID:  driverID,
		Timestamp: time.Now(),
	})
}

func (d *Dispatcher) publishOffer(ctx context.Context, tripID, driverID string, round int, cell h3.Cell, state statemachine.OfferState) {
	_ = d.Publisher.PublishOffer(ctx, events.OfferEvent{
		TripID:    tripID,
		DriverID:  driverID,
		Round:     round,
		State:     string(state),
		Cell:      cell.String(),
		Timestamp: time.Now(),
	})
}

// awaitResponse blocks until a driver responds to tripID's open offer or
// the offer window elapses, whichever comes first.
func (d *Dispatcher) awaitResponse(ctx context.Context, tripID string) Response {
	respCh := d.Hub.open(tripID)
	defer d.Hub.close(tripID)

	select {
	case resp := <-respCh:
		return resp
	case <-time.After(d.Cfg.OfferWindow):
		return NoResponse
	case <-ctx.Done():
		return NoResponse
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
