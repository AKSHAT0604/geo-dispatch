package store

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
)

func newTestStore(t *testing.T) (*DriverStore, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewDriverStore(rdb, h3index.DefaultResolution), rdb
}

// TestGetDriversBatchesLookupAndSkipsMissing is the regression test for the
// scaling fix load testing at 50,000 drivers surfaced: candidate search
// was doing one HGETALL per candidate instead of a single pipelined round
// trip per cell, which saturated the Redis connection pool under real
// concurrent load. Asserts the batch call returns exactly the live
// drivers, keyed by ID, and silently omits IDs with no record.
func TestGetDriversBatchesLookupAndSkipsMissing(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	for _, id := range []string{"d1", "d2", "d3"} {
		if err := store.UpdateLocation(ctx, id, 17.3850, 78.4867); err != nil {
			t.Fatalf("UpdateLocation(%s): %v", id, err)
		}
	}

	got, err := store.GetDrivers(ctx, []string{"d1", "d2", "d3", "does-not-exist"})
	if err != nil {
		t.Fatalf("GetDrivers: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetDrivers returned %d records, want 3 (missing ID silently omitted)", len(got))
	}
	for _, id := range []string{"d1", "d2", "d3"} {
		rec, ok := got[id]
		if !ok {
			t.Fatalf("GetDrivers missing expected driver %s", id)
		}
		if rec.DriverID != id {
			t.Fatalf("GetDrivers[%s].DriverID = %q, want %q", id, rec.DriverID, id)
		}
	}
	if _, ok := got["does-not-exist"]; ok {
		t.Fatalf("GetDrivers returned a record for a driver that was never seen")
	}
}

func TestGetDriversWithEmptyInputReturnsNil(t *testing.T) {
	store, _ := newTestStore(t)
	got, err := store.GetDrivers(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetDrivers: %v", err)
	}
	if got != nil {
		t.Fatalf("GetDrivers(nil) = %v, want nil", got)
	}
}

// TestUpdateLocationPreservesConcurrentStateChange guards against a real
// bug load testing caught: UpdateLocation used to read a driver's state in
// Go and write that same value back inside its Lua script, so a location
// ping racing with a dispatcher's SetState call could silently revert
// OFFERED back to AVAILABLE, corrupting an in-flight offer. State must now
// be read and preserved inside the script itself, atomically with the
// write.
func TestUpdateLocationPreservesConcurrentStateChange(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	if err := store.UpdateLocation(ctx, "d1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation (initial): %v", err)
	}
	if err := store.SetState(ctx, "d1", statemachine.DriverOffered); err != nil {
		t.Fatalf("SetState(OFFERED): %v", err)
	}

	// A location ping arriving after the driver was offered - as a
	// driver's client keeps reporting position while awaiting a response -
	// must not revert the state an in-flight offer just set.
	if err := store.UpdateLocation(ctx, "d1", 17.3860, 78.4870); err != nil {
		t.Fatalf("UpdateLocation (after offer): %v", err)
	}

	got, err := store.GetDriver(ctx, "d1")
	if err != nil {
		t.Fatalf("GetDriver: %v", err)
	}
	if got.State != statemachine.DriverOffered {
		t.Fatalf("state after location ping = %s, want OFFERED (ping must not clobber a concurrent state change)", got.State)
	}
}

func TestUpdateLocationMovesDriverBetweenCells(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()

	if err := store.UpdateLocation(ctx, "d1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation (first): %v", err)
	}
	cellA, err := h3index.CellFor(17.3850, 78.4867, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}
	if inA, err := rdb.SIsMember(ctx, "cell:"+cellA.String()+":drivers", "d1").Result(); err != nil || !inA {
		t.Fatalf("driver not indexed under first cell: member=%v err=%v", inA, err)
	}

	// Far enough away to guarantee a different resolution-8 cell.
	if err := store.UpdateLocation(ctx, "d1", 18.5204, 73.8567); err != nil {
		t.Fatalf("UpdateLocation (second): %v", err)
	}
	cellB, err := h3index.CellFor(18.5204, 73.8567, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}
	if cellA == cellB {
		t.Fatalf("test setup invalid: both coordinates map to the same cell")
	}

	if stillInA, err := rdb.SIsMember(ctx, "cell:"+cellA.String()+":drivers", "d1").Result(); err != nil || stillInA {
		t.Fatalf("driver still indexed under old cell after moving: member=%v err=%v", stillInA, err)
	}
	if inB, err := rdb.SIsMember(ctx, "cell:"+cellB.String()+":drivers", "d1").Result(); err != nil || !inB {
		t.Fatalf("driver not indexed under new cell: member=%v err=%v", inB, err)
	}
}

func TestDriversInCellFiltersExpiredDrivers(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()

	lat, lng := 17.3850, 78.4867
	if err := store.UpdateLocation(ctx, "d1", lat, lng); err != nil {
		t.Fatalf("UpdateLocation: %v", err)
	}
	cell, err := h3index.CellFor(lat, lng, h3index.DefaultResolution)
	if err != nil {
		t.Fatalf("CellFor: %v", err)
	}

	// Simulate the driver's location TTL having expired.
	rdb.Del(ctx, "driver:d1")

	ids, err := store.DriversInCell(ctx, cell)
	if err != nil {
		t.Fatalf("DriversInCell: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("DriversInCell = %v, want empty after driver hash expired", ids)
	}

	stillMember, err := rdb.SIsMember(ctx, "cell:"+cell.String()+":drivers", "d1").Result()
	if err != nil {
		t.Fatalf("SIsMember: %v", err)
	}
	if stillMember {
		t.Fatalf("stale driver was not swept from the cell set")
	}
}

func TestSetStateRejectsIllegalTransition(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	if err := store.UpdateLocation(ctx, "d1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation: %v", err)
	}
	// New drivers start AVAILABLE; AVAILABLE -> ON_TRIP is not legal.
	if err := store.SetState(ctx, "d1", statemachine.DriverOnTrip); err == nil {
		t.Fatalf("SetState(AVAILABLE, ON_TRIP) = nil, want error")
	}
	if err := store.SetState(ctx, "d1", statemachine.DriverOffered); err != nil {
		t.Fatalf("SetState(AVAILABLE, OFFERED): %v", err)
	}
}

func TestAvailableSinceSetOnCreateAndRefreshedOnReturnToAvailable(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	if err := store.UpdateLocation(ctx, "d1", 17.3850, 78.4867); err != nil {
		t.Fatalf("UpdateLocation: %v", err)
	}
	first, err := store.GetDriver(ctx, "d1")
	if err != nil {
		t.Fatalf("GetDriver: %v", err)
	}
	if first.AvailableSince.IsZero() {
		t.Fatalf("AvailableSince not set on a newly created driver")
	}

	// A location ping while still AVAILABLE must not reset the idle clock.
	if err := store.UpdateLocation(ctx, "d1", 17.3860, 78.4870); err != nil {
		t.Fatalf("UpdateLocation (second ping): %v", err)
	}
	second, err := store.GetDriver(ctx, "d1")
	if err != nil {
		t.Fatalf("GetDriver: %v", err)
	}
	if !second.AvailableSince.Equal(first.AvailableSince) {
		t.Fatalf("AvailableSince changed on a location ping: %v -> %v", first.AvailableSince, second.AvailableSince)
	}

	if err := store.SetState(ctx, "d1", statemachine.DriverOffered); err != nil {
		t.Fatalf("SetState(AVAILABLE, OFFERED): %v", err)
	}
	time.Sleep(1100 * time.Millisecond) // ensure a distinguishable unix-second timestamp
	if err := store.SetState(ctx, "d1", statemachine.DriverAvailable); err != nil {
		t.Fatalf("SetState(OFFERED, AVAILABLE): %v", err)
	}
	third, err := store.GetDriver(ctx, "d1")
	if err != nil {
		t.Fatalf("GetDriver: %v", err)
	}
	if !third.AvailableSince.After(first.AvailableSince) {
		t.Fatalf("AvailableSince not refreshed on return to AVAILABLE: %v -> %v", first.AvailableSince, third.AvailableSince)
	}
}

// TestDriverSingleIndexingInvariant is the phase 1 definition-of-done test:
// after a large number of random driver moves, every driver must be indexed
// under exactly the cell it last reported, and no other.
func TestDriverSingleIndexingInvariant(t *testing.T) {
	store, rdb := newTestStore(t)
	ctx := context.Background()

	const numDrivers = 25
	const numMoves = 10000
	const baseLat, baseLng = 17.3850, 78.4867
	const boxDegrees = 0.05 // roughly a 5km box at this latitude

	rng := rand.New(rand.NewSource(42))
	driverIDs := make([]string, numDrivers)
	for i := range driverIDs {
		driverIDs[i] = fmt.Sprintf("driver-%d", i)
	}

	visited := make(map[string]map[string]bool) // driverID -> cells it has ever occupied
	current := make(map[string]string)          // driverID -> cell it currently occupies

	for i := 0; i < numMoves; i++ {
		id := driverIDs[rng.Intn(numDrivers)]
		lat := baseLat + (rng.Float64()-0.5)*boxDegrees
		lng := baseLng + (rng.Float64()-0.5)*boxDegrees

		if err := store.UpdateLocation(ctx, id, lat, lng); err != nil {
			t.Fatalf("UpdateLocation(%s): %v", id, err)
		}

		cell, err := h3index.CellFor(lat, lng, h3index.DefaultResolution)
		if err != nil {
			t.Fatalf("CellFor: %v", err)
		}
		cellStr := cell.String()

		if visited[id] == nil {
			visited[id] = map[string]bool{}
		}
		visited[id][cellStr] = true
		current[id] = cellStr
	}

	for id, cells := range visited {
		want := current[id]
		memberCount := 0
		for cellStr := range cells {
			isMember, err := rdb.SIsMember(ctx, "cell:"+cellStr+":drivers", id).Result()
			if err != nil {
				t.Fatalf("SIsMember: %v", err)
			}
			if isMember {
				memberCount++
				if cellStr != want {
					t.Fatalf("driver %s indexed under stale cell %s, want only current cell %s", id, cellStr, want)
				}
			}
		}
		if memberCount != 1 {
			t.Fatalf("driver %s indexed under %d cells, want exactly 1", id, memberCount)
		}
	}
}
