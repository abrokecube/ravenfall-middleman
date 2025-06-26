package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	// WebSocket upgrader configuration
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			// Allow all origins for WebSocket connections
			// In production, you might want to restrict this to specific origins
			return true
		},
	}

	// Map to store all connected WebSocket clients
	wsClients   = make(map[*websocket.Conn]bool)
	wsClientsMu sync.RWMutex
)

// broadcastMessage sends a message to all connected WebSocket clients
func broadcastMessage(messageType int, data []byte) {
	wsClientsMu.RLock()
	defer wsClientsMu.RUnlock()

	for client := range wsClients {
		if err := client.WriteMessage(messageType, data); err != nil {
			log.Printf("Error broadcasting to WebSocket client: %v", err)
			client.Close()
			delete(wsClients, client)
		}
	}
}

// handleWebSocket handles WebSocket connections for message broadcasting
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade to WebSocket: %v", err)
		return
	}
	defer conn.Close()

	// Register client
	wsClientsMu.Lock()
	wsClients[conn] = true
	wsClientsMu.Unlock()

	log.Printf("New WebSocket client connected: %s", conn.RemoteAddr())

	// Keep the connection alive and handle incoming messages
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}
	}

	// Unregister client
	wsClientsMu.Lock()
	delete(wsClients, conn)
	wsClientsMu.Unlock()
	log.Printf("WebSocket client disconnected: %s", conn.RemoteAddr())
}
