package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

func newTestServer(t *testing.T, hub *Hub) (wsURL string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		hub.Register(conn)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

type testEvent struct {
	TripID string `json:"trip_id"`
	State  string `json:"state"`
}

// TestHubBroadcastsToConnectedClient is an integration test over a real
// WebSocket connection (httptest server + gorilla/websocket client), not a
// mock: it proves a message handed to Broadcast actually arrives on the
// wire.
func TestHubBroadcastsToConnectedClient(t *testing.T) {
	hub := NewHub()
	wsURL := newTestServer(t, hub)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	waitForCount(t, hub, 1)

	if err := hub.Broadcast(testEvent{TripID: "trip-1", State: "MATCHED"}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	var got testEvent
	if err := json.Unmarshal(msg, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.TripID != "trip-1" || got.State != "MATCHED" {
		t.Fatalf("got %+v, want trip-1/MATCHED", got)
	}
}

func TestHubBroadcastsToMultipleClients(t *testing.T) {
	hub := NewHub()
	wsURL := newTestServer(t, hub)

	const n = 3
	conns := make([]*websocket.Conn, n)
	for i := range conns {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		defer conn.Close()
		conns[i] = conn
	}
	waitForCount(t, hub, n)

	if err := hub.Broadcast(testEvent{TripID: "trip-2", State: "OFFERED"}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	for i, conn := range conns {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("client %d ReadMessage: %v", i, err)
		}
		var got testEvent
		if err := json.Unmarshal(msg, &got); err != nil {
			t.Fatalf("client %d unmarshal: %v", i, err)
		}
		if got.TripID != "trip-2" {
			t.Fatalf("client %d got %+v, want trip-2", i, got)
		}
	}
}

func TestUnregisterStopsFurtherDelivery(t *testing.T) {
	hub := NewHub()
	wsURL := newTestServer(t, hub)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	waitForCount(t, hub, 1)

	hub.mu.Lock()
	var registered *websocket.Conn
	for c := range hub.clients {
		registered = c
	}
	hub.mu.Unlock()
	hub.Unregister(registered)

	if got := hub.Count(); got != 0 {
		t.Fatalf("Count after unregister = %d, want 0", got)
	}
	conn.Close()
}

func waitForCount(t *testing.T, hub *Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.Count() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d registered clients, have %d", want, hub.Count())
}
