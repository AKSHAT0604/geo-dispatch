package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/uber/h3-go/v4"
)

// SurgeTTL bounds how long a written multiplier survives without being
// refreshed. If the aggregator stops updating a cell because demand there
// cooled off, the multiplier should age back out to baseline on its own
// rather than linger forever.
const SurgeTTL = 10 * time.Minute

// SurgeStore persists the current surge multiplier per H3 cell.
type SurgeStore struct {
	rdb *redis.Client
}

// NewSurgeStore returns a SurgeStore.
func NewSurgeStore(rdb *redis.Client) *SurgeStore {
	return &SurgeStore{rdb: rdb}
}

func surgeKey(cell h3.Cell) string { return "cell:" + cell.String() + ":surge" }

// SetMultiplier writes cell's current surge multiplier.
func (s *SurgeStore) SetMultiplier(ctx context.Context, cell h3.Cell, multiplier float64) error {
	if err := s.rdb.Set(ctx, surgeKey(cell), multiplier, SurgeTTL).Err(); err != nil {
		return fmt.Errorf("set surge multiplier: %w", err)
	}
	return nil
}

// GetMultiplier returns cell's current surge multiplier, or 1.0 (no surge)
// if none has ever been written or it has aged out.
func (s *SurgeStore) GetMultiplier(ctx context.Context, cell h3.Cell) (float64, error) {
	val, err := s.rdb.Get(ctx, surgeKey(cell)).Result()
	if errors.Is(err, redis.Nil) {
		return 1.0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get surge multiplier: %w", err)
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, fmt.Errorf("parse surge multiplier: %w", err)
	}
	return f, nil
}
