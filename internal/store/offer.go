package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
)

// ErrOfferNotFound is returned when a trip has no currently open offer
// (never offered, or the offer's TTL has already expired).
var ErrOfferNotFound = errors.New("store: offer not found")

// ErrOfferStale is returned when a response targets a driver who is not
// the one the trip's current offer is open for - it already timed out and
// moved on to a different candidate.
var ErrOfferStale = errors.New("store: offer is stale")

// OfferRecord is the offer currently open for a trip, if any. Only one
// offer is ever open per trip at a time.
type OfferRecord struct {
	TripID    string
	DriverID  string
	Round     int
	State     statemachine.OfferState
	CreatedAt time.Time
}

// OfferStore persists the offer currently open for each trip in Redis,
// with a TTL equal to the offer window. The dispatcher's own timer is what
// actually drives timeout behavior (see internal/offers); the TTL exists so
// an offer's liveness can be checked from Redis alone - by another node
// after a handoff, or by an operator during an incident - without needing
// to reach the process running the timer.
type OfferStore struct {
	rdb *redis.Client
}

// NewOfferStore returns an OfferStore.
func NewOfferStore(rdb *redis.Client) *OfferStore {
	return &OfferStore{rdb: rdb}
}

func offerKey(tripID string) string { return "offer:" + tripID }

// CreateOffer opens a new offer for tripID, replacing any previous one.
func (s *OfferStore) CreateOffer(ctx context.Context, tripID, driverID string, round int, ttl time.Duration) error {
	key := offerKey(tripID)
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key,
		"driver_id", driverID,
		"round", round,
		"state", string(statemachine.OfferPending),
		"created_at", time.Now().Unix(),
	)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	return nil
}

// GetOffer returns the offer currently open for tripID.
func (s *OfferStore) GetOffer(ctx context.Context, tripID string) (*OfferRecord, error) {
	fields, err := s.rdb.HGetAll(ctx, offerKey(tripID)).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}
	if len(fields) == 0 {
		return nil, ErrOfferNotFound
	}

	round, err := strconv.Atoi(fields["round"])
	if err != nil {
		return nil, fmt.Errorf("parse round: %w", err)
	}
	createdAtUnix, err := strconv.ParseInt(fields["created_at"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}

	return &OfferRecord{
		TripID:    tripID,
		DriverID:  fields["driver_id"],
		Round:     round,
		State:     statemachine.OfferState(fields["state"]),
		CreatedAt: time.Unix(createdAtUnix, 0),
	}, nil
}

// SetOfferState validates and persists a transition on tripID's current
// offer, but only if it's still open for driverID. A response arriving for
// a driver who is no longer the current candidate - because the offer
// already timed out and moved on - is stale and must not be able to
// resurrect a round that's already over.
func (s *OfferStore) SetOfferState(ctx context.Context, tripID, driverID string, to statemachine.OfferState) error {
	rec, err := s.GetOffer(ctx, tripID)
	if err != nil {
		return err
	}
	if rec.DriverID != driverID {
		return ErrOfferStale
	}
	if err := statemachine.ValidateOfferTransition(rec.State, to); err != nil {
		return err
	}
	return s.rdb.HSet(ctx, offerKey(tripID), "state", string(to)).Err()
}
