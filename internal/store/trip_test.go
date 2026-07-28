package store

import (
	"context"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
)

func newTestTripStore(t *testing.T) *TripStore {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewTripStore(rdb)
}

func TestCreateTripWithSameIdempotencyKeyReturnsSameTrip(t *testing.T) {
	store := newTestTripStore(t)
	ctx := context.Background()

	first, err := store.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key-1")
	if err != nil {
		t.Fatalf("CreateTrip (first): %v", err)
	}
	second, err := store.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key-1")
	if err != nil {
		t.Fatalf("CreateTrip (duplicate): %v", err)
	}
	if first.TripID != second.TripID {
		t.Fatalf("duplicate request returned a different trip: %s != %s", first.TripID, second.TripID)
	}

	third, err := store.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key-2")
	if err != nil {
		t.Fatalf("CreateTrip (different key): %v", err)
	}
	if third.TripID == first.TripID {
		t.Fatalf("a different idempotency key produced the same trip ID")
	}
}

func TestCreateTripIsRaceSafeUnderConcurrentDuplicates(t *testing.T) {
	store := newTestTripStore(t)
	ctx := context.Background()

	const n = 20
	tripIDs := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			trip, err := store.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "concurrent-key")
			if err != nil {
				t.Errorf("CreateTrip: %v", err)
				return
			}
			tripIDs[i] = trip.TripID
		}()
	}
	wg.Wait()

	want := tripIDs[0]
	for i, id := range tripIDs {
		if id != want {
			t.Fatalf("concurrent duplicate requests produced different trip IDs: [0]=%s [%d]=%s", want, i, id)
		}
	}
}

func TestTripLifecycleTransitions(t *testing.T) {
	store := newTestTripStore(t)
	ctx := context.Background()

	trip, err := store.CreateTrip(ctx, "rider-1", 17.3850, 78.4867, "idem-key")
	if err != nil {
		t.Fatalf("CreateTrip: %v", err)
	}
	if trip.State != statemachine.TripRequested {
		t.Fatalf("new trip state = %s, want REQUESTED", trip.State)
	}

	if err := store.SetTripState(ctx, trip.TripID, statemachine.TripOffered); err != nil {
		t.Fatalf("SetTripState(OFFERED): %v", err)
	}
	if err := store.MarkMatched(ctx, trip.TripID, "driver-9"); err != nil {
		t.Fatalf("MarkMatched: %v", err)
	}

	got, err := store.GetTrip(ctx, trip.TripID)
	if err != nil {
		t.Fatalf("GetTrip: %v", err)
	}
	if got.State != statemachine.TripMatched {
		t.Fatalf("state = %s, want MATCHED", got.State)
	}
	if got.MatchedDriverID != "driver-9" {
		t.Fatalf("MatchedDriverID = %q, want driver-9", got.MatchedDriverID)
	}

	// MATCHED is terminal.
	if err := store.MarkUnfulfilled(ctx, trip.TripID); err == nil {
		t.Fatalf("MarkUnfulfilled on a MATCHED trip = nil, want error")
	}
}

func TestGetTripUnknownReturnsErrTripNotFound(t *testing.T) {
	store := newTestTripStore(t)
	if _, err := store.GetTrip(context.Background(), "does-not-exist"); err != ErrTripNotFound {
		t.Fatalf("GetTrip(unknown) error = %v, want ErrTripNotFound", err)
	}
}
