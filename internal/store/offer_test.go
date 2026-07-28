package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
)

func newTestOfferStore(t *testing.T) (*OfferStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewOfferStore(rdb), mr
}

func TestCreateOfferThenAccept(t *testing.T) {
	store, _ := newTestOfferStore(t)
	ctx := context.Background()

	if err := store.CreateOffer(ctx, "trip-1", "driver-1", 1, 15*time.Second); err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}

	rec, err := store.GetOffer(ctx, "trip-1")
	if err != nil {
		t.Fatalf("GetOffer: %v", err)
	}
	if rec.DriverID != "driver-1" || rec.Round != 1 || rec.State != statemachine.OfferPending {
		t.Fatalf("GetOffer = %+v, want driver-1/round1/PENDING", rec)
	}

	if err := store.SetOfferState(ctx, "trip-1", "driver-1", statemachine.OfferAccepted); err != nil {
		t.Fatalf("SetOfferState(ACCEPTED): %v", err)
	}
	rec, err = store.GetOffer(ctx, "trip-1")
	if err != nil {
		t.Fatalf("GetOffer after accept: %v", err)
	}
	if rec.State != statemachine.OfferAccepted {
		t.Fatalf("state = %s, want ACCEPTED", rec.State)
	}
}

func TestSetOfferStateRejectsStaleDriver(t *testing.T) {
	store, _ := newTestOfferStore(t)
	ctx := context.Background()

	if err := store.CreateOffer(ctx, "trip-1", "driver-1", 1, 15*time.Second); err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	// The offer moved on to a different driver (a reoffer). A late
	// response from driver-1 must not be able to touch the new round.
	if err := store.CreateOffer(ctx, "trip-1", "driver-2", 2, 15*time.Second); err != nil {
		t.Fatalf("CreateOffer (round 2): %v", err)
	}

	err := store.SetOfferState(ctx, "trip-1", "driver-1", statemachine.OfferAccepted)
	if err != ErrOfferStale {
		t.Fatalf("SetOfferState from stale driver = %v, want ErrOfferStale", err)
	}
}

func TestOfferExpiresWithTTL(t *testing.T) {
	store, mr := newTestOfferStore(t)
	ctx := context.Background()

	if err := store.CreateOffer(ctx, "trip-1", "driver-1", 1, 15*time.Second); err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	mr.FastForward(16 * time.Second)

	if _, err := store.GetOffer(ctx, "trip-1"); err != ErrOfferNotFound {
		t.Fatalf("GetOffer after TTL expiry = %v, want ErrOfferNotFound", err)
	}
}
