package offers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/uber/h3-go/v4"

	"github.com/AKSHAT0604/geo-dispatch/internal/events"
	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/matching"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

var testOriginCell = mustCell(17.3850, 78.4867)

func mustCell(lat, lng float64) h3.Cell {
	c, err := h3index.CellFor(lat, lng, h3index.DefaultResolution)
	if err != nil {
		panic(err)
	}
	return c
}

// recordingPublisher captures every published event for assertions,
// instead of talking to a real broker.
type recordingPublisher struct {
	mu     sync.Mutex
	trips  []events.TripLifecycleEvent
	offers []events.OfferEvent
}

func (p *recordingPublisher) PublishDriverLocation(context.Context, events.DriverLocationEvent) error {
	return nil
}

func (p *recordingPublisher) PublishTripLifecycle(_ context.Context, e events.TripLifecycleEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.trips = append(p.trips, e)
	return nil
}

func (p *recordingPublisher) PublishOffer(_ context.Context, e events.OfferEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.offers = append(p.offers, e)
	return nil
}

func newTestDispatcher(t *testing.T, cfg Config, publisher events.Publisher) (*Dispatcher, *store.DriverStore, *store.TripStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	drivers := store.NewDriverStore(rdb, 8)
	trips := store.NewTripStore(rdb)
	offerStore := store.NewOfferStore(rdb)
	hub := NewResponseHub()

	return NewDispatcher(offerStore, trips, drivers, hub, publisher, cfg), drivers, trips
}

// fastConfig keeps offer windows above 1s: go-redis floors EXPIRE
// precision at 1 second and logs a warning below it, and tests should
// exercise the same TTL path production traffic does rather than dodge it.
var fastConfig = Config{
	OfferWindow: 1100 * time.Millisecond,
	MaxRounds:   5,
	BaseBackoff: 50 * time.Millisecond,
	MaxBackoff:  200 * time.Millisecond,
}

// TestDispatcherReoffersToSecondRankedDriverOnTimeout is the phase 3
// definition-of-done integration test: it drives a full offer cycle with a
// non-responding first candidate, asserts reoffer to the second-ranked
// driver, and asserts idempotent trip creation alongside it as part of the
// same end-to-end request lifecycle.
func TestDispatcherReoffersToSecondRankedDriverOnTimeout(t *testing.T) {
	publisher := &recordingPublisher{}
	dispatcher, drivers, trips := newTestDispatcher(t, fastConfig, publisher)
	ctx := context.Background()

	if err := drivers.UpdateLocation(ctx, "driver-1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation driver-1: %v", err)
	}
	if err := drivers.UpdateLocation(ctx, "driver-2", 17.3860, 78.4870); err != nil {
		t.Fatalf("UpdateLocation driver-2: %v", err)
	}

	trip, err := trips.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key-1")
	if err != nil {
		t.Fatalf("CreateTrip: %v", err)
	}

	dup, err := trips.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key-1")
	if err != nil {
		t.Fatalf("CreateTrip (duplicate): %v", err)
	}
	if dup.TripID != trip.TripID {
		t.Fatalf("duplicate idempotency key returned a different trip: %s != %s", dup.TripID, trip.TripID)
	}

	candidates := []matching.RankedCandidate{
		{Driver: &store.DriverRecord{DriverID: "driver-1"}},
		{Driver: &store.DriverRecord{DriverID: "driver-2"}},
	}

	// driver-1 never responds. Once the offer moves on to driver-2,
	// accept it - simulating that driver's app calling back after the
	// first candidate's offer window elapsed.
	go acceptWhenOffered(t, dispatcher, trip.TripID, "driver-2")

	matched, driverID, err := dispatcher.Run(ctx, trip.TripID, testOriginCell, candidates)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !matched || driverID != "driver-2" {
		t.Fatalf("Run = matched=%v driverID=%s, want matched=true driverID=driver-2", matched, driverID)
	}

	d1, err := drivers.GetDriver(ctx, "driver-1")
	if err != nil {
		t.Fatalf("GetDriver driver-1: %v", err)
	}
	if d1.State != statemachine.DriverAvailable {
		t.Fatalf("driver-1 state = %s, want AVAILABLE after timing out", d1.State)
	}

	d2, err := drivers.GetDriver(ctx, "driver-2")
	if err != nil {
		t.Fatalf("GetDriver driver-2: %v", err)
	}
	if d2.State != statemachine.DriverEnRoute {
		t.Fatalf("driver-2 state = %s, want EN_ROUTE after accepting", d2.State)
	}

	finalTrip, err := trips.GetTrip(ctx, trip.TripID)
	if err != nil {
		t.Fatalf("GetTrip: %v", err)
	}
	if finalTrip.State != statemachine.TripMatched || finalTrip.MatchedDriverID != "driver-2" {
		t.Fatalf("trip = %+v, want MATCHED with driver-2", finalTrip)
	}

	publisher.mu.Lock()
	tripStates := make([]string, len(publisher.trips))
	for i, e := range publisher.trips {
		tripStates[i] = e.State
	}
	offerStates := make([]string, len(publisher.offers))
	for i, e := range publisher.offers {
		offerStates[i] = e.DriverID + ":" + e.State
	}
	publisher.mu.Unlock()

	wantTripStates := []string{"OFFERED", "MATCHED"}
	if !equalIDs(tripStates, wantTripStates) {
		t.Fatalf("published trip states = %v, want %v", tripStates, wantTripStates)
	}
	wantOfferStates := []string{
		"driver-1:PENDING", "driver-1:TIMED_OUT",
		"driver-2:PENDING", "driver-2:ACCEPTED",
	}
	if !equalIDs(offerStates, wantOfferStates) {
		t.Fatalf("published offer states = %v, want %v", offerStates, wantOfferStates)
	}
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

func TestDispatcherMarksTripUnfulfilledWhenEveryCandidateTimesOut(t *testing.T) {
	dispatcher, drivers, trips := newTestDispatcher(t, fastConfig, nil)
	ctx := context.Background()

	for _, id := range []string{"driver-1", "driver-2"} {
		if err := drivers.UpdateLocation(ctx, id, 17.3850, 78.4867); err != nil {
			t.Fatalf("UpdateLocation %s: %v", id, err)
		}
	}

	trip, err := trips.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key-2")
	if err != nil {
		t.Fatalf("CreateTrip: %v", err)
	}

	candidates := []matching.RankedCandidate{
		{Driver: &store.DriverRecord{DriverID: "driver-1"}},
		{Driver: &store.DriverRecord{DriverID: "driver-2"}},
	}

	matched, driverID, err := dispatcher.Run(ctx, trip.TripID, testOriginCell, candidates)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if matched {
		t.Fatalf("Run matched=%v driverID=%s, want no match when every candidate times out", matched, driverID)
	}

	finalTrip, err := trips.GetTrip(ctx, trip.TripID)
	if err != nil {
		t.Fatalf("GetTrip: %v", err)
	}
	if finalTrip.State != statemachine.TripUnfulfilled {
		t.Fatalf("trip state = %s, want UNFULFILLED", finalTrip.State)
	}

	for _, id := range []string{"driver-1", "driver-2"} {
		d, err := drivers.GetDriver(ctx, id)
		if err != nil {
			t.Fatalf("GetDriver %s: %v", id, err)
		}
		if d.State != statemachine.DriverAvailable {
			t.Fatalf("%s state = %s, want AVAILABLE after being released", id, d.State)
		}
	}
}

// TestDispatcherTreatsExpiredOfferRecordAsTimeout guards against a real bug
// found running against production Redis (not miniredis - this exact race
// didn't reproduce there): the offer record's Redis TTL and the offer
// window used to be equal, so under real network latency Redis could
// expire the record before the dispatcher's own timeout logic reached
// SetOfferState, turning a normal timeout into a hard RPC error. Forces
// the record to have already expired (via miniredis's virtual clock) well
// before the real timer fires, to prove the grace-buffered TTL and the
// defensive ErrOfferNotFound handling both do their job.
func TestDispatcherTreatsExpiredOfferRecordAsTimeout(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	drivers := store.NewDriverStore(rdb, 8)
	trips := store.NewTripStore(rdb)
	offerStore := store.NewOfferStore(rdb)
	hub := NewResponseHub()
	dispatcher := NewDispatcher(offerStore, trips, drivers, hub, nil, fastConfig)
	ctx := context.Background()

	if err := drivers.UpdateLocation(ctx, "driver-1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation: %v", err)
	}
	trip, err := trips.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key")
	if err != nil {
		t.Fatalf("CreateTrip: %v", err)
	}

	candidates := []matching.RankedCandidate{{Driver: &store.DriverRecord{DriverID: "driver-1"}}}

	go func() {
		time.Sleep(100 * time.Millisecond)
		mr.FastForward(fastConfig.OfferWindow + offerRecordGrace + time.Second)
	}()

	matched, _, err := dispatcher.Run(ctx, trip.TripID, testOriginCell, candidates)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if matched {
		t.Fatalf("Run matched, want unfulfilled since driver-1 never responded")
	}

	finalTrip, err := trips.GetTrip(ctx, trip.TripID)
	if err != nil {
		t.Fatalf("GetTrip: %v", err)
	}
	if finalTrip.State != statemachine.TripUnfulfilled {
		t.Fatalf("trip state = %s, want UNFULFILLED", finalTrip.State)
	}
}

// TestDispatcherSkipsCandidateClaimedByConcurrentDispatch guards against a
// real race found under load testing: candidate search ranks a snapshot of
// AVAILABLE drivers before any offer is made, so two concurrent trips can
// both rank the same driver before either claims it. Losing that race must
// fall through to the next candidate, not abort the whole dispatch.
func TestDispatcherSkipsCandidateClaimedByConcurrentDispatch(t *testing.T) {
	dispatcher, drivers, trips := newTestDispatcher(t, fastConfig, nil)
	ctx := context.Background()

	if err := drivers.UpdateLocation(ctx, "driver-1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation driver-1: %v", err)
	}
	if err := drivers.UpdateLocation(ctx, "driver-2", 17.3860, 78.4870); err != nil {
		t.Fatalf("UpdateLocation driver-2: %v", err)
	}

	// Simulate driver-1 having already been claimed by a concurrent
	// dispatch for a different trip, which won the race to offer it first.
	if err := drivers.SetState(ctx, "driver-1", statemachine.DriverOffered); err != nil {
		t.Fatalf("SetState driver-1 OFFERED (simulating concurrent claim): %v", err)
	}

	trip, err := trips.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key")
	if err != nil {
		t.Fatalf("CreateTrip: %v", err)
	}

	candidates := []matching.RankedCandidate{
		{Driver: &store.DriverRecord{DriverID: "driver-1"}},
		{Driver: &store.DriverRecord{DriverID: "driver-2"}},
	}

	go acceptWhenOffered(t, dispatcher, trip.TripID, "driver-2")

	start := time.Now()
	matched, driverID, err := dispatcher.Run(ctx, trip.TripID, testOriginCell, candidates)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !matched || driverID != "driver-2" {
		t.Fatalf("Run = matched=%v driverID=%s, want matched=true driverID=driver-2", matched, driverID)
	}
	// Skipping the already-claimed driver-1 must not spend a backoff
	// delay: driver-2 should be offered almost immediately.
	if elapsed > fastConfig.BaseBackoff {
		t.Fatalf("Run took %s to reach driver-2, want well under one backoff interval (%s)", elapsed, fastConfig.BaseBackoff)
	}

	offer, err := dispatcher.Offers.GetOffer(ctx, trip.TripID)
	if err != nil {
		t.Fatalf("GetOffer: %v", err)
	}
	if offer.Round != 1 {
		t.Fatalf("driver-2's offer round = %d, want 1: a skipped candidate must not consume a round", offer.Round)
	}
}

func TestDispatcherMarksTripUnfulfilledWithNoCandidates(t *testing.T) {
	dispatcher, _, trips := newTestDispatcher(t, fastConfig, nil)
	ctx := context.Background()

	trip, err := trips.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key-3")
	if err != nil {
		t.Fatalf("CreateTrip: %v", err)
	}

	matched, _, err := dispatcher.Run(ctx, trip.TripID, testOriginCell, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if matched {
		t.Fatalf("Run with no candidates matched, want no match")
	}

	finalTrip, err := trips.GetTrip(ctx, trip.TripID)
	if err != nil {
		t.Fatalf("GetTrip: %v", err)
	}
	if finalTrip.State != statemachine.TripUnfulfilled {
		t.Fatalf("trip state = %s, want UNFULFILLED", finalTrip.State)
	}
}

// acceptWhenOffered polls until tripID's open offer belongs to driverID,
// then accepts it. Polling is necessary because exactly when the
// dispatcher opens a given round isn't observable any other way from
// outside the loop.
func acceptWhenOffered(t *testing.T, d *Dispatcher, tripID, driverID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := d.Offers.GetOffer(context.Background(), tripID)
		if err == nil && rec.DriverID == driverID {
			if d.Hub.Respond(tripID, Accepted) {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("acceptWhenOffered: offer for %s never opened within deadline", driverID)
}
