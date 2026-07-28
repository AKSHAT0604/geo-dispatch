// Package store is the Redis access layer for hot dispatch state: driver
// location/state and, in later phases, offer state. Redis is the source of
// truth; anything cached in-process is rebuilt from here.
package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/uber/h3-go/v4"

	"github.com/AKSHAT0604/geo-dispatch/internal/events"
	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
)

// DriverLocationTTL is how long a driver's location record survives without
// a fresh ping. Stale supply is worse than missing supply: a driver whose
// ping stopped 60 seconds ago should not be offered a trip.
const DriverLocationTTL = 30 * time.Second

// ErrDriverNotFound is returned when an operation targets a driver with no
// location record in Redis (either never seen, or its TTL expired).
var ErrDriverNotFound = errors.New("store: driver not found")

// DriverRecord is a driver's last known position and state.
type DriverRecord struct {
	DriverID       string
	Lat, Lng       float64
	Cell           h3.Cell
	State          statemachine.DriverState
	UpdatedAt      time.Time
	AvailableSince time.Time // zero if the driver has never been AVAILABLE
}

// DriverStore persists driver location and state in Redis, keeping each
// driver indexed under exactly one H3 cell set at all times.
type DriverStore struct {
	rdb       *redis.Client
	res       int
	publisher events.Publisher
}

// NewDriverStore returns a DriverStore that indexes drivers at the given H3
// resolution. Events are discarded until SetPublisher is called.
func NewDriverStore(rdb *redis.Client, resolution int) *DriverStore {
	return &DriverStore{rdb: rdb, res: resolution, publisher: events.NoopPublisher{}}
}

// SetPublisher wires up publishing of driver.location events on every
// UpdateLocation call. Kept separate from the constructor so existing
// callers (including every test) are unaffected by adding it.
func (s *DriverStore) SetPublisher(p events.Publisher) {
	if p == nil {
		p = events.NoopPublisher{}
	}
	s.publisher = p
}

func driverKey(driverID string) string { return "driver:" + driverID }
func cellKey(cell h3.Cell) string      { return "cell:" + cell.String() + ":drivers" }

// moveDriverScript atomically removes a driver from its previous cell set
// (if it had one and it changed), adds it to the new cell set, and writes
// its location hash with a fresh TTL. Doing this in one Lua script is what
// guarantees a driver is never indexed under two cells or zero: without it,
// a crash between the SREM and the SADD would leave it dangling one way or
// the other.
var moveDriverScript = redis.NewScript(`
local driverKey  = KEYS[1]
local newCellKey = KEYS[2]
local driverID   = ARGV[1]
local lat        = ARGV[2]
local lng        = ARGV[3]
local newCell    = ARGV[4]
local state      = ARGV[5]
local updatedAt  = ARGV[6]
local ttl        = tonumber(ARGV[7])

local oldCell = redis.call('HGET', driverKey, 'cell')
if oldCell and oldCell ~= newCell then
	redis.call('SREM', 'cell:' .. oldCell .. ':drivers', driverID)
end

redis.call('SADD', newCellKey, driverID)
redis.call('HSET', driverKey, 'lat', lat, 'lng', lng, 'cell', newCell, 'state', state, 'updated_at', updatedAt)
redis.call('EXPIRE', driverKey, ttl)
return 1
`)

// UpdateLocation records a driver's new position, atomically moving it
// between H3 cell sets if the cell changed. A driver not seen before starts
// in AVAILABLE.
func (s *DriverStore) UpdateLocation(ctx context.Context, driverID string, lat, lng float64) error {
	cell, err := h3index.CellFor(lat, lng, s.res)
	if err != nil {
		return fmt.Errorf("cell for (%f, %f): %w", lat, lng, err)
	}

	existingState, err := s.rdb.HGet(ctx, driverKey(driverID), "state").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("read existing state: %w", err)
	}
	isNew := existingState == ""
	state := statemachine.DriverAvailable
	if !isNew {
		state = statemachine.DriverState(existingState)
	}

	if err := moveDriverScript.Run(ctx, s.rdb,
		[]string{driverKey(driverID), cellKey(cell)},
		driverID, lat, lng, cell.String(), string(state), time.Now().Unix(), int(DriverLocationTTL.Seconds()),
	).Err(); err != nil {
		return err
	}

	if isNew {
		// A brand new driver starts AVAILABLE, so its idle clock starts now.
		// HSetNX so a later ping never resets an idle clock already running.
		if err := s.rdb.HSetNX(ctx, driverKey(driverID), "available_since", time.Now().Unix()).Err(); err != nil {
			return fmt.Errorf("set available_since: %w", err)
		}
	}

	_ = s.publisher.PublishDriverLocation(ctx, events.DriverLocationEvent{
		DriverID:  driverID,
		Lat:       lat,
		Lng:       lng,
		Cell:      cell.String(),
		State:     string(state),
		Timestamp: time.Now(),
	})
	return nil
}

// SetState validates and persists a driver state transition. It does not
// touch location or TTL.
func (s *DriverStore) SetState(ctx context.Context, driverID string, to statemachine.DriverState) error {
	current, err := s.rdb.HGet(ctx, driverKey(driverID), "state").Result()
	if errors.Is(err, redis.Nil) {
		return ErrDriverNotFound
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}

	from := statemachine.DriverState(current)
	if err := statemachine.ValidateDriverTransition(from, to); err != nil {
		return err
	}

	if to == statemachine.DriverAvailable {
		// Becoming available (again) restarts the idle clock the fairness
		// ranking term reads from.
		return s.rdb.HSet(ctx, driverKey(driverID), "state", string(to), "available_since", time.Now().Unix()).Err()
	}
	return s.rdb.HSet(ctx, driverKey(driverID), "state", string(to)).Err()
}

// GetDriver returns a driver's full record, or ErrDriverNotFound if it has
// no live location (never pinged, or its TTL expired).
func (s *DriverStore) GetDriver(ctx context.Context, driverID string) (*DriverRecord, error) {
	fields, err := s.rdb.HGetAll(ctx, driverKey(driverID)).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}
	if len(fields) == 0 {
		return nil, ErrDriverNotFound
	}

	lat, err := strconv.ParseFloat(fields["lat"], 64)
	if err != nil {
		return nil, fmt.Errorf("parse lat: %w", err)
	}
	lng, err := strconv.ParseFloat(fields["lng"], 64)
	if err != nil {
		return nil, fmt.Errorf("parse lng: %w", err)
	}
	updatedAtUnix, err := strconv.ParseInt(fields["updated_at"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
	}

	var availableSince time.Time
	if raw, ok := fields["available_since"]; ok && raw != "" {
		unix, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse available_since: %w", err)
		}
		availableSince = time.Unix(unix, 0)
	}

	return &DriverRecord{
		DriverID:       driverID,
		Lat:            lat,
		Lng:            lng,
		Cell:           h3.CellFromString(fields["cell"]),
		State:          statemachine.DriverState(fields["state"]),
		UpdatedAt:      time.Unix(updatedAtUnix, 0),
		AvailableSince: availableSince,
	}, nil
}

// DriversInCell returns the driver IDs currently indexed under cell. A
// member whose location hash has already expired is stale supply: it is
// swept from the set and excluded from the result, so callers never see a
// driver that stopped pinging.
func (s *DriverStore) DriversInCell(ctx context.Context, cell h3.Cell) ([]string, error) {
	ids, err := s.rdb.SMembers(ctx, cellKey(cell)).Result()
	if err != nil {
		return nil, fmt.Errorf("smembers: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := s.rdb.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(ids))
	for _, id := range ids {
		cmds[id] = pipe.Exists(ctx, driverKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("exists pipeline: %w", err)
	}

	live := make([]string, 0, len(ids))
	var stale []interface{}
	for id, cmd := range cmds {
		if cmd.Val() == 1 {
			live = append(live, id)
		} else {
			stale = append(stale, id)
		}
	}
	if len(stale) > 0 {
		if err := s.rdb.SRem(ctx, cellKey(cell), stale...).Err(); err != nil {
			return nil, fmt.Errorf("sweep stale drivers: %w", err)
		}
	}
	return live, nil
}
