package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ProcessorConnector manages a single, persistent WebSocket connection to the message processor.
// It handles connection, reconnection, and dispatching of messages.
var processorConnector = &ProcessorConnector{}

// ProcessorConnector handles the connection to the message processor.
type ProcessorConnector struct {
	conn         *websocket.Conn
	mutex        sync.Mutex
	responseSubs sync.Map // map[string]chan<- []byte
	url          string
	isConnected  bool
}

// Init initializes the connector with the processor URL and starts the connection loop.
func (c *ProcessorConnector) Init(url string) {
	c.url = url
	go c.connectionLoop()
}

// connectionLoop continuously tries to connect and read from the WebSocket.
func (c *ProcessorConnector) connectionLoop() {
	for {
		c.ensureConnected()
		time.Sleep(5 * time.Second) // Cooldown before retrying connection
	}
}

// ensureConnected establishes a connection if not already connected.
func (c *ProcessorConnector) ensureConnected() {
	c.mutex.Lock()
	if c.isConnected {
		c.mutex.Unlock()
		return
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(c.url, nil)
	if err != nil {
		log.Printf("ERROR: Failed to connect to processor: %v", err)
		c.mutex.Unlock()
		return
	}

	log.Println("INFO: Successfully connected to message processor")
	c.conn = conn
	c.isConnected = true
	c.mutex.Unlock()

	// Start reader loop
	c.readLoop()

	// If readLoop exits, it means connection is lost
	c.mutex.Lock()
	c.isConnected = false
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	log.Println("INFO: Disconnected from message processor. Will attempt to reconnect.")
	c.mutex.Unlock()
}

// readLoop reads messages from the WebSocket and dispatches them.
func (c *ProcessorConnector) readLoop() {
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("ERROR: WebSocket read error: %v", err)
			return // Exit to trigger reconnection
		}

		var resp ResponseWrapper
		if err := json.Unmarshal(msg, &resp); err != nil {
			log.Printf("ERROR: Failed to unmarshal processor response: %v", err)
			continue
		}

		if sub, ok := c.responseSubs.Load(resp.CorrelationID); ok {
			if ch, ok := sub.(chan []byte); ok {
				ch <- msg
			} else {
				log.Printf("ERROR: Invalid channel type for correlation ID: %s", resp.CorrelationID)
			}
		} else {
			log.Printf("WARN: Received response for unknown correlation ID: %s", resp.CorrelationID)
		}
	}
}

// Send sends a message to the processor and waits for a response.
func (c *ProcessorConnector) Send(messageData []byte, correlationID string) ([]byte, error) {
	c.mutex.Lock()
	if !c.isConnected || c.conn == nil {
		c.mutex.Unlock()
		return nil, fmt.Errorf("not connected to processor")
	}
	c.mutex.Unlock()

	responseChan := make(chan []byte, 1)
	c.responseSubs.Store(correlationID, responseChan)
	defer c.responseSubs.Delete(correlationID)

	c.mutex.Lock()
	err := c.conn.WriteMessage(websocket.TextMessage, messageData)
	c.mutex.Unlock()

	if err != nil {
		return nil, fmt.Errorf("failed to send message to processor: %w", err)
	}

	select {
	case response := <-responseChan:
		return response, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout waiting for response from processor (correlation ID: %s)", correlationID)
	}
}
