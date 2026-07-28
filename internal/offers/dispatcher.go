package offers

import (
	"context"
	"fmt"
	"time"

	"github.com/AKSHAT0604/geo-dispatch/internal/matching"
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
	Offers  *store.OfferStore
	Trips   *store.TripStore
	Drivers *store.DriverStore
	Hub     *ResponseHub
	Cfg     Config
}

// NewDispatcher returns a Dispatcher.
func NewDispatcher(offerStore *store.OfferStore, tripStore *store.TripStore, driverStore *store.DriverStore, hub *ResponseHub, cfg Config) *Dispatcher {
	return &Dispatcher{Offers: offerStore, Trips: tripStore, Drivers: driverStore, Hub: hub, Cfg: cfg}
}

// Run offers tripID to each ranked candidate in turn, waiting up to
// Cfg.OfferWindow for a response before moving to the next. It stops at
// the first acceptance, or after Cfg.MaxRounds offers with none accepted -
// whichever comes first - and marks the trip UNFULFILLED in that case.
// Failing to match is a valid outcome that must be handled explicitly, not
// by hanging.
func (d *Dispatcher) Run(ctx context.Context, tripID string, candidates []matching.RankedCandidate) (matched bool, driverID string, err error) {
	if len(candidates) == 0 {
		return false, "", d.Trips.MarkUnfulfilled(ctx, tripID)
	}

	if err := d.Trips.SetTripState(ctx, tripID, statemachine.TripOffered); err != nil {
		return false, "", fmt.Errorf("mark trip offered: %w", err)
	}

	rounds := d.Cfg.MaxRounds
	if rounds > len(candidates) {
		rounds = len(candidates)
	}

	backoff := d.Cfg.BaseBackoff
	for round := 1; round <= rounds; round++ {
		candidate := candidates[round-1]
		driverID := candidate.Driver.DriverID

		if err := d.Drivers.SetState(ctx, driverID, statemachine.DriverOffered); err != nil {
			return false, "", fmt.Errorf("offer to driver %s: %w", driverID, err)
		}
		if err := d.Offers.CreateOffer(ctx, tripID, driverID, round, d.Cfg.OfferWindow); err != nil {
			return false, "", fmt.Errorf("create offer round %d: %w", round, err)
		}

		resp := d.awaitResponse(ctx, tripID)

		switch resp {
		case Accepted:
			if err := d.Offers.SetOfferState(ctx, tripID, driverID, statemachine.OfferAccepted); err != nil {
				return false, "", fmt.Errorf("accept offer: %w", err)
			}
			if err := d.Drivers.SetState(ctx, driverID, statemachine.DriverEnRoute); err != nil {
				return false, "", fmt.Errorf("move driver %s en route: %w", driverID, err)
			}
			if err := d.Trips.MarkMatched(ctx, tripID, driverID); err != nil {
				return false, "", fmt.Errorf("mark trip matched: %w", err)
			}
			return true, driverID, nil

		case Declined:
			if err := d.Offers.SetOfferState(ctx, tripID, driverID, statemachine.OfferDeclined); err != nil {
				return false, "", fmt.Errorf("decline offer: %w", err)
			}
		default: // NoResponse: the offer window elapsed.
			if err := d.Offers.SetOfferState(ctx, tripID, driverID, statemachine.OfferTimedOut); err != nil {
				return false, "", fmt.Errorf("time out offer: %w", err)
			}
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

	return false, "", d.Trips.MarkUnfulfilled(ctx, tripID)
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
