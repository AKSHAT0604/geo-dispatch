// Package gateway broadcasts lifecycle events to connected WebSocket
// clients, so a browser-based map (Phase 7's polish) can show trips and
// drivers updating in real time.
package gateway

import (
	"encoding/json"
	"sync"

	"github.com/gorilla/websocket"
)

// Hub tracks connected WebSocket clients and broadcasts messages to all of
// them. Safe for concurrent use.
type Hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
}

// NewHub returns an empty Hub.
func NewHub() *Hub {
	return &Hub{clients: make(map[*websocket.Conn]bool)}
}

// Register adds a client connection to the broadcast set.
func (h *Hub) Register(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = true
}

// Unregister removes and closes a client connection.
func (h *Hub) Unregister(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		c.Close()
	}
}

// Count returns the number of currently registered clients.
func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// Broadcast marshals v as JSON and sends it to every connected client. A
// client whose write fails is assumed dead and is unregistered rather than
// retried - a stale WebSocket connection isn't recoverable by retrying a
// single message.
func (h *Hub) Broadcast(v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if err := c.WriteMessage(websocket.TextMessage, body); err != nil {
			delete(h.clients, c)
			c.Close()
		}
	}
	return nil
}
