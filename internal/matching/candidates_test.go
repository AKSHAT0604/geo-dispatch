package matching

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

func newTestLookup(t *testing.T) *store.DriverStore {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return store.NewDriverStore(rdb, h3index.DefaultResolution)
}

const (
	baseLat = 17.3850
	baseLng = 78.4867
)

func TestFindCandidatesReturnsEmptyForEmptyCell(t *testing.T) {
	lookup := newTestLookup(t)
	ctx := context.Background()

	origin, err := h3index.CellFor(baseLat, baseLng, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	candidates, err := FindCandidates(ctx, lookup, origin, DefaultSearchConfig)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("FindCandidates over empty area = %d candidates, want 0", len(candidates))
	}
}

func TestFindCandidatesExpandsAcrossRings(t *testing.T) {
	lookup := newTestLookup(t)
	ctx := context.Background()

	origin, err := h3index.CellFor(baseLat, baseLng, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	ring2, err := origin.GridRing(2)
	if err != nil {
		t.Fatalf("GridRing(2): %v", err)
	}
	if len(ring2) == 0 {
		t.Fatalf("GridRing(2) returned no cells")
	}
	target := ring2[0]
	if d, err := h3index.DistanceCells(origin, target); err != nil || d != 2 {
		t.Fatalf("test setup invalid: target is %d cells from origin, want 2 (err=%v)", d, err)
	}

	targetLatLng, err := target.LatLng()
	if err != nil {
		t.Fatalf("target.LatLng: %v", err)
	}
	if err := lookup.UpdateLocation(ctx, "ring2-driver", targetLatLng.Lat, targetLatLng.Lng); err != nil {
		t.Fatalf("UpdateLocation: %v", err)
	}

	cfg := SearchConfig{MinCandidates: 1, MaxK: 1}
	candidates, err := FindCandidates(ctx, lookup, origin, cfg)
	if err != nil {
		t.Fatalf("FindCandidates (MaxK=1): %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("FindCandidates found the ring-2 driver within MaxK=1, want 0 (test setup or ring logic is wrong)")
	}

	cfg = SearchConfig{MinCandidates: 1, MaxK: 4}
	candidates, err = FindCandidates(ctx, lookup, origin, cfg)
	if err != nil {
		t.Fatalf("FindCandidates (MaxK=4): %v", err)
	}
	if len(candidates) != 1 || candidates[0].DriverID != "ring2-driver" {
		t.Fatalf("FindCandidates (MaxK=4) = %v, want [ring2-driver]", candidateIDs(candidates))
	}
}

func TestFindCandidatesExcludesNonAvailableDrivers(t *testing.T) {
	lookup := newTestLookup(t)
	ctx := context.Background()

	origin, err := h3index.CellFor(baseLat, baseLng, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	for _, id := range []string{"busy-1", "busy-2", "busy-3"} {
		if err := lookup.UpdateLocation(ctx, id, baseLat, baseLng); err != nil {
			t.Fatalf("UpdateLocation(%s): %v", id, err)
		}
		if err := lookup.SetState(ctx, id, statemachine.DriverOffered); err != nil {
			t.Fatalf("SetState(%s, OFFERED): %v", id, err)
		}
	}

	candidates, err := FindCandidates(ctx, lookup, origin, DefaultSearchConfig)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("FindCandidates with all drivers busy = %v, want none", candidateIDs(candidates))
	}
}

func TestFindCandidatesStopsAtMinCandidates(t *testing.T) {
	lookup := newTestLookup(t)
	ctx := context.Background()

	origin, err := h3index.CellFor(baseLat, baseLng, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	for _, id := range []string{"a1", "a2", "a3"} {
		if err := lookup.UpdateLocation(ctx, id, baseLat, baseLng); err != nil {
			t.Fatalf("UpdateLocation(%s): %v", id, err)
		}
	}

	cfg := SearchConfig{MinCandidates: 2, MaxK: 4}
	candidates, err := FindCandidates(ctx, lookup, origin, cfg)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	// All three live in the origin cell (k=0), so search stops immediately
	// with all of them, not just MinCandidates worth - the config is a
	// floor on when to stop expanding, not a cap on how many to return.
	if len(candidates) != 3 {
		t.Fatalf("FindCandidates = %v, want all 3 origin-cell drivers", candidateIDs(candidates))
	}
}

func candidateIDs(candidates []*store.DriverRecord) []string {
	ids := make([]string, len(candidates))
	for i, c := range candidates {
		ids[i] = c.DriverID
	}
	return ids
}
