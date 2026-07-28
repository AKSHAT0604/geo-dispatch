package main

import (
	"encoding/json"
	"net/http"

	"google.golang.org/grpc"

	"github.com/AKSHAT0604/geo-dispatch/api/proto/dispatchpb"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

// server holds supply-service's dependencies. discoClient talks to
// whichever disco node this instance is configured to reach; that node's
// own router forwards RespondToOffer on to wherever the trip is actually
// being dispatched, so supply-service doesn't need to know cluster
// topology itself.
type server struct {
	drivers     *store.DriverStore
	offerStore  *store.OfferStore
	discoClient dispatchpb.DispatchServiceClient
}

func newServer(drivers *store.DriverStore, offerStore *store.OfferStore, discoConn *grpc.ClientConn) *server {
	return &server{
		drivers:     drivers,
		offerStore:  offerStore,
		discoClient: dispatchpb.NewDispatchServiceClient(discoConn),
	}
}

type locationRequest struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// handleLocation records a driver's location ping.
func (s *server) handleLocation(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	var req locationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := s.drivers.UpdateLocation(r.Context(), driverID, req.Lat, req.Lng); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type offerResponse struct {
	TripID string `json:"trip_id"`
}

// handleGetOffer lets a driver's client poll for whatever it's currently
// been offered, since it only ever knows its own driver ID.
func (s *server) handleGetOffer(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	tripID, err := s.offerStore.CurrentOfferFor(r.Context(), driverID)
	if err != nil {
		if err == store.ErrOfferNotFound {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, offerResponse{TripID: tripID})
}

type respondRequest struct {
	Response string `json:"response"` // "ACCEPTED" or "DECLINED"
}

type respondResponse struct {
	Delivered bool `json:"delivered"`
}

// handleRespondOffer forwards a driver's accept/decline to disco, which
// routes it to whichever node is actually running that trip's dispatch
// loop.
func (s *server) handleRespondOffer(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	var req respondRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	respVal := dispatchpb.OfferResponse_OFFER_RESPONSE_DECLINED
	if req.Response == "ACCEPTED" {
		respVal = dispatchpb.OfferResponse_OFFER_RESPONSE_ACCEPTED
	}

	resp, err := s.discoClient.RespondToOffer(r.Context(), &dispatchpb.RespondToOfferRequest{
		DriverId: driverID,
		Response: respVal,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, respondResponse{Delivered: resp.Delivered})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
