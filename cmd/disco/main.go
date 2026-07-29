// Command disco runs the matcher: the only component that decides
// assignments. Each instance is a node in the consistent hash ring over
// H3 cells (internal/hashring, internal/membership); a request for a cell
// this node doesn't own is forwarded over gRPC to whichever node does
// (internal/router).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/AKSHAT0604/geo-dispatch/api/proto/dispatchpb"
	"github.com/AKSHAT0604/geo-dispatch/internal/events"
	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/matching"
	"github.com/AKSHAT0604/geo-dispatch/internal/membership"
	"github.com/AKSHAT0604/geo-dispatch/internal/offers"
	"github.com/AKSHAT0604/geo-dispatch/internal/router"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

func main() {
	httpPort := envOr("DISCO_PORT", "8083")
	grpcPort := envOrInt("DISCO_GRPC_PORT", 9083)
	gossipPort := envOrInt("DISCO_GOSSIP_PORT", 7946)
	bindAddr := envOr("DISCO_BIND_ADDR", "0.0.0.0")
	advertiseAddr := envOr("DISCO_ADVERTISE_ADDR", "127.0.0.1")
	nodeName := envOr("DISCO_NODE_NAME", fmt.Sprintf("disco-%d", grpcPort))
	seeds := splitNonEmpty(os.Getenv("DISCO_SEEDS"), ",")
	redisAddr := envOr("REDIS_ADDR", "127.0.0.1:6379")
	kafkaBrokers := splitNonEmpty(envOr("KAFKA_BROKERS", "localhost:9092"), ",")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("disco: connect to redis at %s: %v", redisAddr, err)
	}

	drivers := store.NewDriverStore(rdb, h3index.DefaultResolution)
	trips := store.NewTripStore(rdb)
	offerStore := store.NewOfferStore(rdb)

	var publisher events.Publisher = events.NoopPublisher{}
	if len(kafkaBrokers) > 0 {
		kp := events.NewKafkaPublisher(kafkaBrokers)
		defer kp.Close()
		publisher = kp
		drivers.SetPublisher(kp)
	}

	hub := offers.NewResponseHub()
	dispatcher := offers.NewDispatcher(offerStore, trips, drivers, hub, publisher, offers.DefaultConfig)
	// Surge/congestion data isn't wired into a live feed yet (see
	// docs/DECISIONS.md #4); an empty in-memory provider defaults every
	// cell to free-flow, which is equivalent to the haversine baseline
	// until that feed exists.
	estimator := matching.NewCongestionAwareEstimator(matching.NewInMemoryCongestionProvider(), h3index.DefaultResolution)

	handler := &localHandler{
		trips:      trips,
		drivers:    drivers,
		offerStore: offerStore,
		dispatcher: dispatcher,
		estimator:  estimator,
		resolution: h3index.DefaultResolution,
	}

	member, err := membership.New(membership.Config{
		NodeName: nodeName,
		BindAddr: bindAddr,
		BindPort: gossipPort,
		Seeds:    seeds,
		GRPCAddr: fmt.Sprintf("%s:%d", advertiseAddr, grpcPort),
	})
	if err != nil {
		log.Fatalf("disco: join cluster: %v", err)
	}
	defer member.Shutdown()

	rt := router.New(nodeName, member.Ring(), handler, nil, member.PeerGRPCAddr)

	grpcSrv := grpc.NewServer()
	dispatchpb.RegisterDispatchServiceServer(grpcSrv, &grpcServer{
		router:     rt,
		handler:    handler,
		trips:      trips,
		offerStore: offerStore,
		resolution: h3index.DefaultResolution,
	})

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("disco: listen on gRPC port %d: %v", grpcPort, err)
	}
	go func() {
		log.Printf("disco: node=%s gRPC listening on :%d, gossip on :%d", nodeName, grpcPort, gossipPort)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("disco: grpc server stopped: %v", err)
		}
	}()

	reconcileCtx, reconcileCancel := context.WithCancel(context.Background())
	defer reconcileCancel()
	go runReconciler(reconcileCtx, &offers.Reconciler{
		Trips:      trips,
		Offers:     offerStore,
		Drivers:    drivers,
		Dispatcher: dispatcher,
		Estimator:  estimator,
		Resolution: h3index.DefaultResolution,
		SearchCfg:  matching.DefaultSearchConfig,
		RankCfg:    matching.DefaultRankConfig,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	// /debug/ring is operational visibility into cluster membership as
	// this node's ring currently sees it - useful for confirming a
	// rebalance actually happened after a node join or failure, not just
	// for local testing.
	mux.HandleFunc("/debug/ring", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"local_node":     nodeName,
			"gossip_members": member.Members(),
			"ring_nodes":     member.Ring().Nodes(),
		})
	})
	httpSrv := &http.Server{Addr: ":" + httpPort, Handler: mux}
	go func() {
		log.Printf("disco: http listening on :%s", httpPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("disco: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Printf("disco: shutting down")
	reconcileCancel()
	grpcSrv.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("disco: http shutdown error: %v", err)
	}
	if err := member.Leave(2 * time.Second); err != nil {
		log.Printf("disco: leave error: %v", err)
	}
}

// runReconciler periodically sweeps for trips left OFFERED with no live
// offer - the handoff recovery path - so a node picking up cells from one
// that crashed resumes any work it left mid-flight.
func runReconciler(ctx context.Context, rc *offers.Reconciler) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recovered, err := rc.Sweep(ctx)
			if err != nil {
				log.Printf("disco: reconciler sweep: %v", err)
				continue
			}
			if recovered > 0 {
				log.Printf("disco: reconciler resumed %d stuck trip(s)", recovered)
			}
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func splitNonEmpty(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
