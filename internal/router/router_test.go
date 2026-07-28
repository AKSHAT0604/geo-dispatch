package router

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AKSHAT0604/geo-dispatch/api/proto/dispatchpb"
	"github.com/AKSHAT0604/geo-dispatch/internal/hashring"
)

type fakeHandler struct {
	resp   *dispatchpb.DispatchResponse
	err    error
	called bool

	offerResp   *dispatchpb.RespondToOfferResponse
	offerErr    error
	offerCalled bool
}

func (h *fakeHandler) Handle(ctx context.Context, req *dispatchpb.DispatchRequest) (*dispatchpb.DispatchResponse, error) {
	h.called = true
	return h.resp, h.err
}

func (h *fakeHandler) HandleOfferResponse(ctx context.Context, req *dispatchpb.RespondToOfferRequest) (*dispatchpb.RespondToOfferResponse, error) {
	h.offerCalled = true
	return h.offerResp, h.offerErr
}

func TestRouteHandlesLocallyWhenThisNodeOwnsCell(t *testing.T) {
	ring := hashring.New(10)
	ring.AddNode("node-a")

	handler := &fakeHandler{resp: &dispatchpb.DispatchResponse{TripId: "trip-1"}}
	r := New("node-a", ring, handler, nil, nil)

	resp, err := r.Route(context.Background(), "cell-1", &dispatchpb.DispatchRequest{RiderId: "rider-1"})
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !handler.called {
		t.Fatalf("local handler was not called for a cell this node owns")
	}
	if resp.TripId != "trip-1" {
		t.Fatalf("resp = %+v, want TripId=trip-1", resp)
	}
}

type fakeRemoteServer struct {
	dispatchpb.UnimplementedDispatchServiceServer
	resp *dispatchpb.DispatchResponse
}

func (s *fakeRemoteServer) Dispatch(ctx context.Context, req *dispatchpb.DispatchRequest) (*dispatchpb.DispatchResponse, error) {
	return s.resp, nil
}

func (s *fakeRemoteServer) RespondToOffer(ctx context.Context, req *dispatchpb.RespondToOfferRequest) (*dispatchpb.RespondToOfferResponse, error) {
	return &dispatchpb.RespondToOfferResponse{Delivered: true}, nil
}

// TestRouteForwardsOverGRPCWhenAnotherNodeOwnsCell is the phase 5 routing
// test: it runs a real gRPC server on loopback standing in for the owning
// peer, and asserts the router forwards to it - not a mock, an actual
// round trip over the network stack.
func TestRouteForwardsOverGRPCWhenAnotherNodeOwnsCell(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	remote := &fakeRemoteServer{resp: &dispatchpb.DispatchResponse{TripId: "trip-remote", Matched: true, DriverId: "driver-9"}}
	dispatchpb.RegisterDispatchServiceServer(grpcServer, remote)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	ring := hashring.New(10)
	ring.AddNode("node-a")
	ring.AddNode("node-b")

	// Force cell-1 to resolve to whichever node is NOT the local one, so
	// the router is guaranteed to forward rather than handle locally.
	owner, _ := ring.Lookup("cell-1")
	localNode := "node-a"
	if owner == "node-a" {
		localNode = "node-b"
	}

	handler := &fakeHandler{} // must not be called
	peerAddr := lis.Addr().String()
	dial := func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	r := New(localNode, ring, handler, dial, func(nodeID string) (string, bool) {
		if nodeID == owner {
			return peerAddr, true
		}
		return "", false
	})

	resp, err := r.Route(context.Background(), "cell-1", &dispatchpb.DispatchRequest{RiderId: "rider-1"})
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if handler.called {
		t.Fatalf("local handler was called for a cell owned by a peer")
	}
	if resp.TripId != "trip-remote" || !resp.Matched || resp.DriverId != "driver-9" {
		t.Fatalf("resp = %+v, want the remote server's response", resp)
	}
}

func TestRouteErrorsWhenPeerAddressUnknown(t *testing.T) {
	ring := hashring.New(10)
	ring.AddNode("node-a")
	ring.AddNode("node-b")

	owner, _ := ring.Lookup("cell-1")
	localNode := "node-a"
	if owner == "node-a" {
		localNode = "node-b"
	}

	r := New(localNode, ring, &fakeHandler{}, nil, func(string) (string, bool) { return "", false })
	if _, err := r.Route(context.Background(), "cell-1", &dispatchpb.DispatchRequest{}); err == nil {
		t.Fatalf("Route with unknown peer address = nil error, want error")
	}
}

func TestRouteErrorsWhenRingIsEmpty(t *testing.T) {
	ring := hashring.New(10)
	r := New("node-a", ring, &fakeHandler{}, nil, nil)
	if _, err := r.Route(context.Background(), "cell-1", &dispatchpb.DispatchRequest{}); err == nil {
		t.Fatalf("Route with empty ring = nil error, want error")
	}
}

func TestRouteOfferResponseHandlesLocallyWhenThisNodeOwnsCell(t *testing.T) {
	ring := hashring.New(10)
	ring.AddNode("node-a")

	handler := &fakeHandler{offerResp: &dispatchpb.RespondToOfferResponse{Delivered: true}}
	r := New("node-a", ring, handler, nil, nil)

	resp, err := r.RouteOfferResponse(context.Background(), "cell-1", handler, &dispatchpb.RespondToOfferRequest{DriverId: "driver-1"})
	if err != nil {
		t.Fatalf("RouteOfferResponse: %v", err)
	}
	if !handler.offerCalled {
		t.Fatalf("local offer handler was not called for a cell this node owns")
	}
	if !resp.Delivered {
		t.Fatalf("resp.Delivered = false, want true")
	}
}

func TestRouteOfferResponseForwardsOverGRPCWhenAnotherNodeOwnsCell(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	remote := &fakeRemoteServer{}
	dispatchpb.RegisterDispatchServiceServer(grpcServer, remote)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	ring := hashring.New(10)
	ring.AddNode("node-a")
	ring.AddNode("node-b")

	owner, _ := ring.Lookup("cell-1")
	localNode := "node-a"
	if owner == "node-a" {
		localNode = "node-b"
	}

	handler := &fakeHandler{} // must not be called
	peerAddr := lis.Addr().String()
	dial := func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	r := New(localNode, ring, handler, dial, func(nodeID string) (string, bool) {
		if nodeID == owner {
			return peerAddr, true
		}
		return "", false
	})

	resp, err := r.RouteOfferResponse(context.Background(), "cell-1", handler, &dispatchpb.RespondToOfferRequest{DriverId: "driver-1"})
	if err != nil {
		t.Fatalf("RouteOfferResponse: %v", err)
	}
	if handler.offerCalled {
		t.Fatalf("local offer handler was called for a cell owned by a peer")
	}
	if !resp.Delivered {
		t.Fatalf("resp.Delivered = false, want true (from the remote server)")
	}
}
