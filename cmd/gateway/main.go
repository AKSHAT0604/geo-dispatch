// Command gateway pushes trip lifecycle events to a browser-based map over
// WebSocket: it consumes the trip.lifecycle Kafka topic and rebroadcasts
// each event as JSON to every connected client.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"

	"github.com/AKSHAT0604/geo-dispatch/internal/events"
	"github.com/AKSHAT0604/geo-dispatch/internal/gateway"
)

//go:embed static
var staticFS embed.FS

// upgrader accepts connections from any origin: this gateway serves a demo
// map, not an authenticated production surface, so origin checking is out
// of scope.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	port := envOr("GATEWAY_PORT", "8086")
	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")

	hub := gateway.NewHub()

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("gateway: upgrade: %v", err)
			return
		}
		hub.Register(conn)
		defer hub.Unregister(conn)
		// The gateway only ever pushes; reading is solely to detect the
		// client disconnecting (a closed/errored read is the signal).
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	mux.Handle("/", http.FileServer(http.FS(static)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumeTripEvents(ctx, brokers, hub)

	srv := &http.Server{Addr: ":" + port, Handler: mux}
	log.Printf("gateway listening on :%s (kafka brokers: %v)", port, brokers)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway: %v", err)
	}
}

// consumeTripEvents reads trip.lifecycle and rebroadcasts each event to
// every connected WebSocket client. It retries on read errors (a broker
// that isn't up yet, or a transient disconnect) rather than exiting, since
// the gateway should keep serving its static map and reconnect on its own
// once Kafka becomes available.
func consumeTripEvents(ctx context.Context, brokers []string, hub *gateway.Hub) {
	for {
		if err := runConsumer(ctx, brokers, hub); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("gateway: kafka consumer error, retrying in 5s: %v", err)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

func runConsumer(ctx context.Context, brokers []string, hub *gateway.Hub) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   events.TopicTripLifecycle,
		GroupID: "gateway",
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var event events.TripLifecycleEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("gateway: skipping malformed trip event: %v", err)
			continue
		}
		if err := hub.Broadcast(event); err != nil {
			log.Printf("gateway: broadcast: %v", err)
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
