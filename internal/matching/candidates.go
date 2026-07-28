package matching

import (
	"context"
	"fmt"

	"github.com/uber/h3-go/v4"

	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// DriverLookup is the subset of store.DriverStore candidate search needs,
// kept as an interface so tests can run against a fake instead of a real
// Redis-backed store.
type DriverLookup interface {
	DriversInCell(ctx context.Context, cell h3.Cell) ([]string, error)
	GetDriver(ctx context.Context, driverID string) (*store.DriverRecord, error)
}

// SearchConfig bounds how far and how wide candidate search looks before
// giving up.
type SearchConfig struct {
	MinCandidates int // keep expanding the ring until at least this many AVAILABLE drivers are found
	MaxK          int // stop expanding after this many rings regardless of how few were found
}

// DefaultSearchConfig expands until 5 candidates are found, capped at k=4.
// Beyond k=4 the pickup ETA already exceeds a realistic offer window, so
// further expansion can't produce a usable match while the ring's cell
// count keeps growing with the square of k.
var DefaultSearchConfig = SearchConfig{MinCandidates: 5, MaxK: 4}

// FindCandidates expands outward in H3 rings from origin, collecting
// AVAILABLE drivers, until at least cfg.MinCandidates are found or cfg.MaxK
// rings have been searched. It returns whatever was found, including none.
func FindCandidates(ctx context.Context, lookup DriverLookup, origin h3.Cell, cfg SearchConfig) ([]*store.DriverRecord, error) {
	var candidates []*store.DriverRecord
	scanned := make(map[h3.Cell]bool)

	for k := 0; k <= cfg.MaxK; k++ {
		disk, err := h3index.KRing(origin, k)
		if err != nil {
			return nil, fmt.Errorf("k-ring at k=%d: %w", k, err)
		}

		for _, cell := range disk {
			if scanned[cell] {
				continue
			}
			scanned[cell] = true

			ids, err := lookup.DriversInCell(ctx, cell)
			if err != nil {
				return nil, fmt.Errorf("drivers in cell %s: %w", cell, err)
			}
			for _, id := range ids {
				d, err := lookup.GetDriver(ctx, id)
				if err != nil {
					if err == store.ErrDriverNotFound {
						continue // aged out between the set read and this lookup
					}
					return nil, fmt.Errorf("get driver %s: %w", id, err)
				}
				if d.State == statemachine.DriverAvailable {
					candidates = append(candidates, d)
				}
			}
		}

		if len(candidates) >= cfg.MinCandidates {
			break
		}
	}

	return candidates, nil
}
