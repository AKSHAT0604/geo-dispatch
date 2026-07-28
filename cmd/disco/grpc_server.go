package main

import (
	"context"
	"fmt"

	"github.com/AKSHAT0604/geo-dispatch/api/proto/dispatchpb"
	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/router"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// grpcServer is what's registered with the gRPC server. Both RPCs arrive
// without knowing which H3 cell they belong to yet - Dispatch carries a
// lat/lng, and RespondToOffer only carries a driver ID - so this layer
// resolves the cell first and then hands off to router, which decides
// whether this node handles it locally or forwards it.
type grpcServer struct {
	dispatchpb.UnimplementedDispatchServiceServer
	router     *router.Router
	handler    *localHandler
	trips      *store.TripStore
	offerStore *store.OfferStore
	resolution int
}

func (s *grpcServer) Dispatch(ctx context.Context, req *dispatchpb.DispatchRequest) (*dispatchpb.DispatchResponse, error) {
	cell, err := h3index.CellFor(req.OriginLat, req.OriginLng, s.resolution)
	if err != nil {
		return nil, fmt.Errorf("cell for origin: %w", err)
	}
	return s.router.Route(ctx, cell.String(), req)
}

func (s *grpcServer) RespondToOffer(ctx context.Context, req *dispatchpb.RespondToOfferRequest) (*dispatchpb.RespondToOfferResponse, error) {
	tripID, err := s.offerStore.CurrentOfferFor(ctx, req.DriverId)
	if err != nil {
		if err == store.ErrOfferNotFound {
			return &dispatchpb.RespondToOfferResponse{Delivered: false}, nil
		}
		return nil, fmt.Errorf("current offer for driver %s: %w", req.DriverId, err)
	}

	trip, err := s.trips.GetTrip(ctx, tripID)
	if err != nil {
		return nil, fmt.Errorf("get trip %s: %w", tripID, err)
	}

	cell, err := h3index.CellFor(trip.OriginLat, trip.OriginLng, s.resolution)
	if err != nil {
		return nil, fmt.Errorf("cell for trip origin: %w", err)
	}
	return s.router.RouteOfferResponse(ctx, cell.String(), s.handler, req)
}
