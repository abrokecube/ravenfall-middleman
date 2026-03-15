package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ProcessorConnector manages an array of persistent WebSocket connections to the message processors.
// It handles connection, reconnection, and sequential dispatching of messages.
var processorConnector = &ProcessorConnector{}

// ProcessorClient handles the connection to a single message processor.
type ProcessorClient struct {
	conn         *websocket.Conn
	mutex        sync.Mutex
	responseSubs sync.Map // map[string]chan<- []byte
	url          string
	isConnected  bool
}

// ProcessorConnector holds all active processor clients.
type ProcessorConnector struct {
	clients []*ProcessorClient
	mutex   sync.RWMutex
}

// Init initializes the connector with the processor URLs and starts the connection loops.
func (c *ProcessorConnector) Init(urls []string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, url := range urls {
		client := &ProcessorClient{url: url}
		c.clients = append(c.clients, client)
		go client.connectionLoop()
	}
}

// connectionLoop continuously tries to connect and read from the WebSocket.
func (c *ProcessorClient) connectionLoop() {
	for {
		c.ensureConnected()
		time.Sleep(5 * time.Second) // Cooldown before retrying connection
	}
}

// ensureConnected establishes a connection if not already connected.
func (c *ProcessorClient) ensureConnected() {
	c.mutex.Lock()
	if c.isConnected {
		c.mutex.Unlock()
		return
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(c.url, nil)
	if err != nil {
		log.Printf("ERROR: Failed to connect to processor at %s: %v", c.url, err)
		c.mutex.Unlock()
		return
	}

	log.Printf("INFO: Successfully connected to message processor at %s", c.url)
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
	log.Printf("INFO: Disconnected from message processor at %s. Will attempt to reconnect.", c.url)
	c.mutex.Unlock()
}

// readLoop reads messages from the WebSocket and dispatches them.
func (c *ProcessorClient) readLoop() {
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("ERROR: WebSocket read error on %s: %v", c.url, err)
			return // Exit to trigger reconnection
		}

		var resp ResponseWrapper
		if err := json.Unmarshal(msg, &resp); err != nil {
			log.Printf("ERROR: Failed to unmarshal processor response from %s: %v", c.url, err)
			continue
		}

		if sub, ok := c.responseSubs.Load(resp.CorrelationID); ok {
			if ch, ok := sub.(chan []byte); ok {
				ch <- msg
			} else {
				log.Printf("ERROR: Invalid channel type for correlation ID: %s", resp.CorrelationID)
			}
		} else {
			log.Printf("WARN: Received response for unknown correlation ID from %s: %s", c.url, resp.CorrelationID)
		}
	}
}

// Send sends a message through the processor pipeline and waits for a response.
func (c *ProcessorConnector) Send(initialMessageData []byte, correlationID string) ([]byte, error) {
	c.mutex.RLock()
	clients := c.clients
	c.mutex.RUnlock()

	if len(clients) == 0 {
		return nil, fmt.Errorf("no processors configured")
	}

	currentMessageData := initialMessageData
	var lastRawMessage json.RawMessage

	// Extract the starting raw message to track it through the pipeline
	var wrapper MessageWrapper
	if err := json.Unmarshal(initialMessageData, &wrapper); err == nil {
		lastRawMessage = wrapper.Message
	}

	for _, client := range clients {
		client.mutex.Lock()
		if !client.isConnected || client.conn == nil {
			client.mutex.Unlock()
			continue // Skip disconnected processors and continue pipeline
		}

		responseChan := make(chan []byte, 1)
		client.responseSubs.Store(correlationID, responseChan)

		err := client.conn.WriteMessage(websocket.TextMessage, currentMessageData)
		client.mutex.Unlock()

		if err != nil {
			log.Printf("ERROR: Failed to send message to processor %s: %v", client.url, err)
			client.responseSubs.Delete(correlationID)
			continue
		}

		var response []byte
		select {
		case response = <-responseChan:
		case <-time.After(5 * time.Second):
			log.Printf("ERROR: Timeout waiting for response from processor %s (correlation ID: %s)", client.url, correlationID)
			client.responseSubs.Delete(correlationID)
			continue // Skip this processor on timeout
		}

		client.responseSubs.Delete(correlationID)

		var procResp struct {
			ProcessorResponse
			CorrelationID string `json:"correlationId"`
			Error         string `json:"error,omitempty"`
		}

		if err := json.Unmarshal(response, &procResp); err != nil {
			log.Printf("ERROR: Failed to parse processor response from %s: %v", client.url, err)
			continue
		}

		if procResp.CorrelationID != correlationID {
			log.Printf("WARN: Mismatched correlation ID from %s", client.url)
			continue
		}

		if procResp.Block {
			log.Printf("DEBUG: Message blocked by processor at %s", client.url)
			return response, nil // Return the block response immediately to halt pipeline
		}

		if procResp.Error != "" {
			log.Printf("ERROR: Processor %s returned an error: %s", client.url, procResp.Error)
			continue // Ignore error and continue pipeline with unmodified payload
		}

		if len(procResp.Message) > 0 && string(procResp.Message) != "null" {
			// Update the running message
			lastRawMessage = procResp.Message

			// Repackage the new message into the wrapper for the NEXT processor in the pipeline
			if err := json.Unmarshal(currentMessageData, &wrapper); err == nil {
				wrapper.Message = procResp.Message
				if newData, err := json.Marshal(wrapper); err == nil {
					currentMessageData = newData
				}
			}
		}
	}

	// Synthesize a final response to hand back to proxyconnection.go
	finalResp := struct {
		Block         bool            `json:"block"`
		Message       json.RawMessage `json:"message"`
		CorrelationID string          `json:"correlationId"`
	}{
		Block:         false,
		Message:       lastRawMessage,
		CorrelationID: correlationID,
	}

	return json.Marshal(finalResp)
}
