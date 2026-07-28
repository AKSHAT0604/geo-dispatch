package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
)

func newTestSurgeStore(t *testing.T) (*SurgeStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewSurgeStore(rdb), mr
}

func TestSurgeStoreSetThenGet(t *testing.T) {
	store, _ := newTestSurgeStore(t)
	ctx := context.Background()
	cell, err := h3index.CellFor(17.3850, 78.4867, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	if err := store.SetMultiplier(ctx, cell, 1.75); err != nil {
		t.Fatalf("SetMultiplier: %v", err)
	}
	got, err := store.GetMultiplier(ctx, cell)
	if err != nil {
		t.Fatalf("GetMultiplier: %v", err)
	}
	if got != 1.75 {
		t.Fatalf("GetMultiplier = %v, want 1.75", got)
	}
}

func TestSurgeStoreDefaultsToBaselineWhenUnset(t *testing.T) {
	store, _ := newTestSurgeStore(t)
	cell, err := h3index.CellFor(17.3850, 78.4867, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	got, err := store.GetMultiplier(context.Background(), cell)
	if err != nil {
		t.Fatalf("GetMultiplier: %v", err)
	}
	if got != 1.0 {
		t.Fatalf("GetMultiplier (unset) = %v, want 1.0", got)
	}
}

func TestSurgeStoreAgesOutAfterTTL(t *testing.T) {
	store, mr := newTestSurgeStore(t)
	ctx := context.Background()
	cell, err := h3index.CellFor(17.3850, 78.4867, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	if err := store.SetMultiplier(ctx, cell, 2.0); err != nil {
		t.Fatalf("SetMultiplier: %v", err)
	}
	mr.FastForward(SurgeTTL + time.Second)

	got, err := store.GetMultiplier(ctx, cell)
	if err != nil {
		t.Fatalf("GetMultiplier after TTL: %v", err)
	}
	if got != 1.0 {
		t.Fatalf("GetMultiplier after TTL = %v, want baseline 1.0", got)
	}
}
