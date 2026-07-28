package main

import (
	"context"
	"fmt"
	"time"

	"github.com/AKSHAT0604/geo-dispatch/api/proto/dispatchpb"
	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/matching"
	"github.com/AKSHAT0604/geo-dispatch/internal/offers"
	"github.com/AKSHAT0604/geo-dispatch/internal/statemachine"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// localHandler runs the match-and-offer pipeline for requests whose origin
// cell this node owns. It implements both router.Handler and
// router.OfferHandler; a request that arrives at a node which doesn't own
// the relevant cell never reaches these methods; the router forwards it
// first.
type localHandler struct {
	trips      *store.TripStore
	drivers    *store.DriverStore
	offerStore *store.OfferStore
	dispatcher *offers.Dispatcher
	estimator  matching.ETAEstimator
	resolution int
}

// Handle runs candidate search, ranking, and the full offer loop for a new
// ride request, or - if the request is a retry of one already dispatched
// (idempotent trip creation returns the same trip either way) - waits for
// the original attempt to resolve instead of starting a second one, which
// would race with it over the same offer state.
func (h *localHandler) Handle(ctx context.Context, req *dispatchpb.DispatchRequest) (*dispatchpb.DispatchResponse, error) {
	trip, err := h.trips.CreateTrip(ctx, req.RiderId, req.OriginLat, req.OriginLng, req.IdempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("create trip: %w", err)
	}

	switch trip.State {
	case statemachine.TripMatched, statemachine.TripUnfulfilled:
		return tripToResponse(trip), nil
	case statemachine.TripOffered:
		final, err := h.awaitResolution(ctx, trip.TripID)
		if err != nil {
			return nil, err
		}
		return tripToResponse(final), nil
	}

	origin, err := h3index.CellFor(trip.OriginLat, trip.OriginLng, h.resolution)
	if err != nil {
		return nil, fmt.Errorf("cell for origin: %w", err)
	}

	candidates, err := matching.FindCandidates(ctx, h.drivers, origin, matching.DefaultSearchConfig)
	if err != nil {
		return nil, fmt.Errorf("find candidates: %w", err)
	}
	ranked, err := matching.Rank(h.estimator, matching.Location{Lat: trip.OriginLat, Lng: trip.OriginLng}, candidates, matching.DefaultRankConfig, time.Now())
	if err != nil {
		return nil, fmt.Errorf("rank candidates: %w", err)
	}

	if _, _, err := h.dispatcher.Run(ctx, trip.TripID, origin, ranked); err != nil {
		return nil, fmt.Errorf("dispatch: %w", err)
	}

	final, err := h.trips.GetTrip(ctx, trip.TripID)
	if err != nil {
		return nil, fmt.Errorf("get trip: %w", err)
	}
	return tripToResponse(final), nil
}

// HandleOfferResponse delivers a driver's accept/decline to the dispatch
// loop currently waiting on it via the shared ResponseHub.
func (h *localHandler) HandleOfferResponse(ctx context.Context, req *dispatchpb.RespondToOfferRequest) (*dispatchpb.RespondToOfferResponse, error) {
	tripID, err := h.offerStore.CurrentOfferFor(ctx, req.DriverId)
	if err != nil {
		if err == store.ErrOfferNotFound {
			return &dispatchpb.RespondToOfferResponse{Delivered: false}, nil
		}
		return nil, fmt.Errorf("current offer for driver %s: %w", req.DriverId, err)
	}

	resp := offers.Declined
	if req.Response == dispatchpb.OfferResponse_OFFER_RESPONSE_ACCEPTED {
		resp = offers.Accepted
	}
	return &dispatchpb.RespondToOfferResponse{Delivered: h.dispatcher.Hub.Respond(tripID, resp)}, nil
}

func (h *localHandler) awaitResolution(ctx context.Context, tripID string) (*store.TripRecord, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		trip, err := h.trips.GetTrip(ctx, tripID)
		if err != nil {
			return nil, err
		}
		if trip.State == statemachine.TripMatched || trip.State == statemachine.TripUnfulfilled {
			return trip, nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func tripToResponse(trip *store.TripRecord) *dispatchpb.DispatchResponse {
	return &dispatchpb.DispatchResponse{
		TripId:   trip.TripID,
		Matched:  trip.State == statemachine.TripMatched,
		DriverId: trip.MatchedDriverID,
		State:    string(trip.State),
	}
}
