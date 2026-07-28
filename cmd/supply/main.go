// Command supply runs the supply-service: owns driver location and state.
// Drivers ping their position here, poll for whatever they've been
// offered, and respond to it; the actual dispatch decision is disco's.
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
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AKSHAT0604/geo-dispatch/internal/events"
	"github.com/AKSHAT0604/geo-dispatch/internal/h3index"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
)

func main() {
	port := envOr("SUPPLY_PORT", "8081")
	redisAddr := envOr("REDIS_ADDR", "127.0.0.1:6379")
	discoAddr := envOr("DISCO_ADDR", "127.0.0.1:9083")
	kafkaBrokers := envOr("KAFKA_BROKERS", "")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("supply-service: connect to redis at %s: %v", redisAddr, err)
	}

	drivers := store.NewDriverStore(rdb, h3index.DefaultResolution)
	if kafkaBrokers != "" {
		kp := events.NewKafkaPublisher([]string{kafkaBrokers})
		defer kp.Close()
		drivers.SetPublisher(kp)
	}
	offerStore := store.NewOfferStore(rdb)

	discoConn, err := grpc.NewClient(discoAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("supply-service: dial disco at %s: %v", discoAddr, err)
	}
	defer discoConn.Close()

	srv := newServer(drivers, offerStore, discoConn)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("POST /drivers/{id}/location", srv.handleLocation)
	mux.HandleFunc("GET /drivers/{id}/offer", srv.handleGetOffer)
	mux.HandleFunc("POST /drivers/{id}/offer/respond", srv.handleRespondOffer)

	httpSrv := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		log.Printf("supply-service listening on :%s (disco: %s)", port, discoAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("supply-service: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("supply-service: shutdown error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
