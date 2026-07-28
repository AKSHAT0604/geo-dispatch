// Command demand runs the demand-service: accepts ride requests, assigns
// an idempotency key, and forwards to disco for matching. It does not
// decide matches itself - disco is the only component that does.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AKSHAT0604/geo-dispatch/api/proto/dispatchpb"
)

func main() {
	port := envOr("DEMAND_PORT", "8082")
	discoAddr := envOr("DISCO_ADDR", "127.0.0.1:9083")

	discoConn, err := grpc.NewClient(discoAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("demand-service: dial disco at %s: %v", discoAddr, err)
	}
	defer discoConn.Close()

	srv := &server{discoClient: dispatchpb.NewDispatchServiceClient(discoConn)}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("POST /trips", srv.handleCreateTrip)

	httpSrv := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		log.Printf("demand-service listening on :%s (disco: %s)", port, discoAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("demand-service: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("demand-service: shutdown error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
