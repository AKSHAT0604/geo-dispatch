// Package router decides, for a given H3 cell, whether this node should
// handle a dispatch request locally or forward it over gRPC to whichever
// node in the cluster actually owns that cell.
package router

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AKSHAT0604/geo-dispatch/api/proto/dispatchpb"
	"github.com/AKSHAT0604/geo-dispatch/internal/hashring"
)

// Handler runs the full match-and-offer pipeline for a request whose
// origin cell this node owns. Implemented by the disco service's own
// wiring; kept as an interface here so Router doesn't depend on matching,
// offers, or store directly.
type Handler interface {
	Handle(ctx context.Context, req *dispatchpb.DispatchRequest) (*dispatchpb.DispatchResponse, error)
}

// OfferHandler delivers a driver's offer response for a trip whose origin
// cell this node owns.
type OfferHandler interface {
	HandleOfferResponse(ctx context.Context, req *dispatchpb.RespondToOfferRequest) (*dispatchpb.RespondToOfferResponse, error)
}

// PeerDialer returns a gRPC client connection to the node listening at
// addr. Kept as a function type so tests can point it at an in-process
// server instead of needing real cluster addresses.
type PeerDialer func(addr string) (*grpc.ClientConn, error)

// PeerAddrLookup resolves a node ID to the "host:port" its DispatchService
// listens on.
type PeerAddrLookup func(nodeID string) (string, bool)

// DefaultDialer dials a peer with an insecure gRPC connection. The cluster
// is assumed to run on a trusted internal network, matching the scope of
// the rest of this project.
func DefaultDialer(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// Router looks up the owning node for a cell and either runs the request
// locally (this node owns it) or forwards it over gRPC to whichever node
// does.
type Router struct {
	localNode string
	ring      *hashring.Ring
	handler   Handler
	dial      PeerDialer
	peerAddr  PeerAddrLookup
}

// New returns a Router. dial defaults to DefaultDialer if nil.
func New(localNode string, ring *hashring.Ring, handler Handler, dial PeerDialer, peerAddr PeerAddrLookup) *Router {
	if dial == nil {
		dial = DefaultDialer
	}
	return &Router{localNode: localNode, ring: ring, handler: handler, dial: dial, peerAddr: peerAddr}
}

// Route decides who owns cellID and dispatches req there: locally if this
// node owns it, over gRPC to the owning node's DispatchService otherwise.
func (r *Router) Route(ctx context.Context, cellID string, req *dispatchpb.DispatchRequest) (*dispatchpb.DispatchResponse, error) {
	owner, ok := r.ring.Lookup(cellID)
	if !ok {
		return nil, fmt.Errorf("router: no node owns cell %s - ring is empty", cellID)
	}

	if owner == r.localNode {
		return r.handler.Handle(ctx, req)
	}

	addr, ok := r.peerAddr(owner)
	if !ok {
		return nil, fmt.Errorf("router: no known gRPC address for owning node %s", owner)
	}

	conn, err := r.dial(addr)
	if err != nil {
		return nil, fmt.Errorf("router: dial %s (node %s): %w", addr, owner, err)
	}
	defer conn.Close()

	client := dispatchpb.NewDispatchServiceClient(conn)
	resp, err := client.Dispatch(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("router: forward to node %s: %w", owner, err)
	}
	return resp, nil
}

// RouteOfferResponse looks up the owning node for cellID - the origin cell
// of the trip currently offered to the driver in req - and delivers req
// there: locally via offerHandler if this node owns it, over gRPC to the
// owning node otherwise. A driver's client only knows its own driver ID,
// not which node is running that trip's dispatch loop, so it can call any
// node and rely on this to find the right one.
func (r *Router) RouteOfferResponse(ctx context.Context, cellID string, offerHandler OfferHandler, req *dispatchpb.RespondToOfferRequest) (*dispatchpb.RespondToOfferResponse, error) {
	owner, ok := r.ring.Lookup(cellID)
	if !ok {
		return nil, fmt.Errorf("router: no node owns cell %s - ring is empty", cellID)
	}

	if owner == r.localNode {
		return offerHandler.HandleOfferResponse(ctx, req)
	}

	addr, ok := r.peerAddr(owner)
	if !ok {
		return nil, fmt.Errorf("router: no known gRPC address for owning node %s", owner)
	}

	conn, err := r.dial(addr)
	if err != nil {
		return nil, fmt.Errorf("router: dial %s (node %s): %w", addr, owner, err)
	}
	defer conn.Close()

	client := dispatchpb.NewDispatchServiceClient(conn)
	resp, err := client.RespondToOffer(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("router: forward to node %s: %w", owner, err)
	}
	return resp, nil
}
