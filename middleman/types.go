package main

import (
	"encoding/json"
	"time"
)

// ClientMessage defines the structure of the JSON message from the client.
type ClientMessage struct {
	Identifier string `json:"Identifier"`
}

type ServerMessage struct {
	Identifier    string `json:"Identifier"`
	CorrelationID string `json:"CorrelationId"`
}

// ServerConfig holds the configuration for a target server.
type ServerConfig struct {
	Host string
	Port int
}

// APIRequest defines the structure for API requests.
type APIRequest struct {
	ConnectionID  string `json:"connectionId"`
	Data          string `json:"data"`
	Timeout       int    `json:"timeout"`
	ExpectedCount int    `json:"expectedCount"` // Number of expected responses (0 = single response)
}

// ResponseCollector holds the collected responses
type ResponseCollector struct {
	Responses []json.RawMessage
	Count     int
	Expected  int
	Complete  bool
	Ch        chan struct{} // Closed when complete
}

// MessageProcessorConfig holds the configuration for the message processor.
type MessageProcessorConfig struct {
	Enabled bool   `json:"enabled"`
	URL     string `json:"url"`
}

// SocketProxy manages all proxy connections.
// usedCorrelationID tracks when a correlation ID was marked as used
type usedCorrelationID struct {
	timestamp time.Time
}

// MessageWrapper is used to wrap messages with metadata
// when sending to the processor
type MessageWrapper struct {
	Source        string          `json:"source"`        // "CLIENT" "SERVER" "API-CLIENT" "API-SERVER"
	ClientAddr    string          `json:"clientAddr"`    // Client's remote address
	ServerAddr    string          `json:"serverAddr"`    // Server's address (if connected)
	ConnectionID  string          `json:"connectionId"`  // Unique ID for this connection
	CorrelationID string          `json:"correlationId"` // Unique ID to match requests and responses
	IsAPI         bool            `json:"isApi"`         // True if the message originated from the API
	Timestamp     string          `json:"timestamp"`     // When the message was sent in RFC3339 format
	Message       json.RawMessage `json:"message"`       // The original message
}

// ProcessorResponse represents the response from the message processor
type ProcessorResponse struct {
	Block   bool            `json:"block"`   // If true, the message should be blocked
	Message json.RawMessage `json:"message"` // The processed message (if not blocked)
}

// ResponseWrapper is the general wrapper for messages coming from the processor.
// It is used to extract the correlation ID before full unmarshaling.
type ResponseWrapper struct {
	CorrelationID string `json:"correlationId"`
}
