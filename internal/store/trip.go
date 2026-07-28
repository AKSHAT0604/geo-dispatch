package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
)

// IdempotencyTTL is how long an idempotency key is remembered. Mobile
// clients retry aggressively on flaky networks; 24 hours comfortably
// outlives any realistic retry storm for a single ride request.
const IdempotencyTTL = 24 * time.Hour

// ErrTripNotFound is returned when an operation targets a trip that
// doesn't exist.
var ErrTripNotFound = errors.New("store: trip not found")

// TripRecord is a ride request and its current lifecycle state.
type TripRecord struct {
	TripID               string
	RiderID              string
	OriginLat, OriginLng float64
	State                statemachine.TripState
	IdempotencyKey       string
	MatchedDriverID      string
	CreatedAt            time.Time
}

// TripStore persists trip intent and lifecycle state in Redis.
type TripStore struct {
	rdb *redis.Client
}

// NewTripStore returns a TripStore.
func NewTripStore(rdb *redis.Client) *TripStore {
	return &TripStore{rdb: rdb}
}

func tripKey(tripID string) string { return "trip:" + tripID }
func idemKey(key string) string    { return "idem:" + key }

// createTripScript performs the idempotency check-and-create atomically:
// without a single script, two concurrent retries of the same request
// could both pass a GET-then-SET race and create two trips for one
// idempotency key.
var createTripScript = redis.NewScript(`
local idemKey    = KEYS[1]
local tripKey     = KEYS[2]
local candidateID = ARGV[1]
local riderID      = ARGV[2]
local lat          = ARGV[3]
local lng           = ARGV[4]
local createdAt     = ARGV[5]
local idemTTL         = tonumber(ARGV[6])
local idempotencyKey  = ARGV[7]

local existing = redis.call('GET', idemKey)
if existing then
	return existing
end

redis.call('SET', idemKey, candidateID, 'EX', idemTTL)
redis.call('HSET', tripKey,
	'rider_id', riderID,
	'origin_lat', lat,
	'origin_lng', lng,
	'state', 'REQUESTED',
	'idempotency_key', idempotencyKey,
	'created_at', createdAt)
return candidateID
`)

// CreateTrip persists trip intent before any matching begins. A duplicate
// call with the same idempotencyKey returns the trip the first call
// created, never a second one.
func (s *TripStore) CreateTrip(ctx context.Context, riderID string, lat, lng float64, idempotencyKey string) (*TripRecord, error) {
	candidateID := uuid.NewString()

	tripID, err := createTripScript.Run(ctx, s.rdb,
		[]string{idemKey(idempotencyKey), tripKey(candidateID)},
		candidateID, riderID, lat, lng, time.Now().Unix(), int(IdempotencyTTL.Seconds()), idempotencyKey,
	).Text()
	if err != nil {
		return nil, fmt.Errorf("create trip script: %w", err)
	}

	return s.GetTrip(ctx, tripID)
}

// GetTrip returns a trip's current record.
func (s *TripStore) GetTrip(ctx context.Context, tripID string) (*TripRecord, error) {
	fields, err := s.rdb.HGetAll(ctx, tripKey(tripID)).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}
	if len(fields) == 0 {
		return nil, ErrTripNotFound
	}

	lat, err := strconv.ParseFloat(fields["origin_lat"], 64)
	if err != nil {
		return nil, fmt.Errorf("parse origin_lat: %w", err)
	}
	lng, err := strconv.ParseFloat(fields["origin_lng"], 64)
	if err != nil {
		return nil, fmt.Errorf("parse origin_lng: %w", err)
	}
	createdAtUnix, err := strconv.ParseInt(fields["created_at"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}

	return &TripRecord{
		TripID:          tripID,
		RiderID:         fields["rider_id"],
		OriginLat:       lat,
		OriginLng:       lng,
		State:           statemachine.TripState(fields["state"]),
		IdempotencyKey:  fields["idempotency_key"],
		MatchedDriverID: fields["matched_driver_id"],
		CreatedAt:       time.Unix(createdAtUnix, 0),
	}, nil
}

// transitionTo validates and persists a trip state change, writing any
// extra hash fields alongside it in the same call.
func (s *TripStore) transitionTo(ctx context.Context, tripID string, to statemachine.TripState, extra ...interface{}) error {
	current, err := s.rdb.HGet(ctx, tripKey(tripID), "state").Result()
	if errors.Is(err, redis.Nil) {
		return ErrTripNotFound
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}

	from := statemachine.TripState(current)
	if err := statemachine.ValidateTripTransition(from, to); err != nil {
		return err
	}

	fields := append([]interface{}{"state", string(to)}, extra...)
	return s.rdb.HSet(ctx, tripKey(tripID), fields...).Err()
}

// SetTripState validates and persists a trip state transition.
func (s *TripStore) SetTripState(ctx context.Context, tripID string, to statemachine.TripState) error {
	return s.transitionTo(ctx, tripID, to)
}

// MarkMatched transitions a trip to MATCHED and records the driver it
// matched with.
func (s *TripStore) MarkMatched(ctx context.Context, tripID, driverID string) error {
	return s.transitionTo(ctx, tripID, statemachine.TripMatched, "matched_driver_id", driverID)
}

// MarkUnfulfilled transitions a trip to UNFULFILLED. Failing to match is a
// valid, explicit outcome, not an error to hide.
func (s *TripStore) MarkUnfulfilled(ctx context.Context, tripID string) error {
	return s.transitionTo(ctx, tripID, statemachine.TripUnfulfilled)
}
