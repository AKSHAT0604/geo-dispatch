// Command surge runs the surge-aggregator: a Kafka consumer of
// driver.location and trip.lifecycle that maintains a rolling
// supply/demand ratio per H3 cell and writes the resulting multiplier back
// to Redis for disco to read when quoting a trip. Pricing stays a pure
// function of that ratio; nothing here touches matching or ranking.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/uber/h3-go/v4"

	"github.com/AKSHAT0604/geo-dispatch/internal/events"
	"github.com/AKSHAT0604/geo-dispatch/internal/store"
	"github.com/AKSHAT0604/geo-dispatch/internal/surge"
)

func main() {
	port := envOr("SURGE_PORT", "8084")
	redisAddr := envOr("REDIS_ADDR", "127.0.0.1:6379")
	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("surge-aggregator: connect to redis at %s: %v", redisAddr, err)
	}
	surgeStore := store.NewSurgeStore(rdb)
	aggregator := surge.NewAggregator(surge.DefaultWindow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumeLoop(ctx, kafkaBrokers, events.TopicTripLifecycle, "surge-aggregator", func(body []byte) {
		var e events.TripLifecycleEvent
		if err := json.Unmarshal(body, &e); err != nil {
			return
		}
		cell := h3.CellFromString(e.Cell)
		if !cell.IsValid() {
			return
		}
		aggregator.RecordTrip(cell, e.TripID, e.State, e.Timestamp)
	})

	go consumeLoop(ctx, kafkaBrokers, events.TopicDriverLocation, "surge-aggregator", func(body []byte) {
		var e events.DriverLocationEvent
		if err := json.Unmarshal(body, &e); err != nil {
			return
		}
		cell := h3.CellFromString(e.Cell)
		if !cell.IsValid() {
			return
		}
		aggregator.RecordDriver(cell, e.DriverID, e.State, e.Timestamp)
	})

	go writeMultipliersLoop(ctx, aggregator, surgeStore)
	go func() {
		ticker := time.NewTicker(surge.DefaultWindow)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				aggregator.Prune(time.Now())
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	httpSrv := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		log.Printf("surge-aggregator listening on :%s (kafka: %v)", port, kafkaBrokers)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("surge-aggregator: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("surge-aggregator: shutdown error: %v", err)
	}
}

// writeMultipliersLoop periodically recomputes and persists the surge
// multiplier for every cell the aggregator currently holds state for.
// Recomputing on a fixed tick rather than on every event keeps Redis
// writes bounded regardless of event volume.
func writeMultipliersLoop(ctx context.Context, aggregator *surge.Aggregator, surgeStore *store.SurgeStore) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for _, cell := range aggregator.Cells() {
				multiplier := aggregator.Multiplier(cell, now, surge.DefaultMultiplierConfig)
				if err := surgeStore.SetMultiplier(ctx, cell, multiplier); err != nil {
					log.Printf("surge-aggregator: write multiplier for %s: %v", cell, err)
				}
			}
		}
	}
}

// consumeLoop reads messages from topic and hands each message body to
// handle, retrying the whole reader on error rather than exiting - the
// aggregator should keep serving its current state and reconnect on its
// own once Kafka becomes available.
func consumeLoop(ctx context.Context, brokers []string, topic, groupID string, handle func(body []byte)) {
	for {
		if err := runReader(ctx, brokers, topic, groupID, handle); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("surge-aggregator: kafka reader for %s error, retrying in 5s: %v", topic, err)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

func runReader(ctx context.Context, brokers []string, topic, groupID string, handle func(body []byte)) error {
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: groupID})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		handle(msg.Value)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
