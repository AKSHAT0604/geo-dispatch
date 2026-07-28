package main

import (
	"encoding/json"
	"net/http"

	"github.com/AKSHAT0604/geo-dispatch/api/proto/dispatchpb"
)

type server struct {
	discoClient dispatchpb.DispatchServiceClient
}

type tripRequest struct {
	RiderID string  `json:"rider_id"`
	Lat     float64 `json:"lat"`
	Lng     float64 `json:"lng"`
}

type tripResponse struct {
	TripID   string `json:"trip_id"`
	Matched  bool   `json:"matched"`
	DriverID string `json:"driver_id,omitempty"`
	State    string `json:"state"`
}

// handleCreateTrip accepts a ride request. Idempotency-Key follows the
// mobile-client-retry convention of living in a header rather than the
// body, since it identifies the request attempt, not trip data.
func (s *server) handleCreateTrip(w http.ResponseWriter, r *http.Request) {
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		http.Error(w, "missing Idempotency-Key header", http.StatusBadRequest)
		return
	}

	var req tripRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if req.RiderID == "" {
		http.Error(w, "rider_id is required", http.StatusBadRequest)
		return
	}

	resp, err := s.discoClient.Dispatch(r.Context(), &dispatchpb.DispatchRequest{
		RiderId:        req.RiderID,
		OriginLat:      req.Lat,
		OriginLng:      req.Lng,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tripResponse{
		TripID:   resp.TripId,
		Matched:  resp.Matched,
		DriverID: resp.DriverId,
		State:    resp.State,
	})
}
