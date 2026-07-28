package offers

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/matching"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

func newTestReconciler(t *testing.T) (*Reconciler, *store.DriverStore, *store.TripStore, *store.OfferStore, *Dispatcher) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	drivers := store.NewDriverStore(rdb, h3index.DefaultResolution)
	trips := store.NewTripStore(rdb)
	offerStore := store.NewOfferStore(rdb)
	hub := NewResponseHub()
	dispatcher := NewDispatcher(offerStore, trips, drivers, hub, nil, fastConfig)

	reconciler := &Reconciler{
		Trips:      trips,
		Offers:     offerStore,
		Drivers:    drivers,
		Dispatcher: dispatcher,
		Estimator:  matching.NewHaversineEstimator(),
		Resolution: h3index.DefaultResolution,
		SearchCfg:  matching.DefaultSearchConfig,
		RankCfg:    matching.DefaultRankConfig,
	}
	return reconciler, drivers, trips, offerStore, dispatcher
}

// TestReconcilerResumesStuckOfferedTrip is the phase 5 handoff test: it
// simulates a node crashing mid-dispatch (a trip marked OFFERED and
// indexed, but with no offer record - as if the crash happened between
// those two writes) and asserts a fresh Reconciler, standing in for the
// new owner after the ring rebalances, picks it back up and matches it
// purely from what's in Redis.
func TestReconcilerResumesStuckOfferedTrip(t *testing.T) {
	reconciler, drivers, trips, _, dispatcher := newTestReconciler(t)
	ctx := context.Background()

	if err := drivers.UpdateLocation(ctx, "driver-1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation: %v", err)
	}

	trip, err := trips.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key")
	if err != nil {
		t.Fatalf("CreateTrip: %v", err)
	}
	if err := trips.SetTripState(ctx, trip.TripID, statemachine.TripOffered); err != nil {
		t.Fatalf("SetTripState(OFFERED): %v", err)
	}

	if ids, err := trips.OfferedTrips(ctx); err != nil || len(ids) != 1 {
		t.Fatalf("OfferedTrips = %v, %v; want exactly the stuck trip", ids, err)
	}

	go acceptWhenOffered(t, dispatcher, trip.TripID, "driver-1")

	recovered, err := reconciler.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("Sweep recovered = %d, want 1", recovered)
	}

	final, err := trips.GetTrip(ctx, trip.TripID)
	if err != nil {
		t.Fatalf("GetTrip: %v", err)
	}
	if final.State != statemachine.TripMatched || final.MatchedDriverID != "driver-1" {
		t.Fatalf("trip = %+v, want MATCHED with driver-1", final)
	}
}

func TestReconcilerSkipsTripsWithLiveOffers(t *testing.T) {
	reconciler, drivers, trips, offerStore, _ := newTestReconciler(t)
	ctx := context.Background()

	if err := drivers.UpdateLocation(ctx, "driver-1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation: %v", err)
	}
	trip, err := trips.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key")
	if err != nil {
		t.Fatalf("CreateTrip: %v", err)
	}
	if err := trips.SetTripState(ctx, trip.TripID, statemachine.TripOffered); err != nil {
		t.Fatalf("SetTripState: %v", err)
	}
	if err := offerStore.CreateOffer(ctx, trip.TripID, "driver-1", 1, fastConfig.OfferWindow); err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}

	recovered, err := reconciler.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("Sweep recovered = %d, want 0: trip has a live offer, some node already owns it", recovered)
	}
}

func TestReconcilerSweepWithNoStuckTripsRecoversNothing(t *testing.T) {
	reconciler, _, _, _, _ := newTestReconciler(t)
	recovered, err := reconciler.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("Sweep recovered = %d, want 0", recovered)
	}
}
