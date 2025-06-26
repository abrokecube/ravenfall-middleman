package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type SocketProxy struct {
	connections          map[string]*ProxyConnection
	listeners            map[int]net.Listener
	listenerMutex        sync.Mutex // Protects the listeners map
	connMutex            sync.Mutex // Protects the connections map
	mappings             map[int]ServerConfig
	defaultIdleTimeout   time.Duration
	noIdentifierTimeout  time.Duration
	identifierTimeouts   map[string]time.Duration
	enableMessageLogging bool
	processorCfg         MessageProcessorConfig
	responseChannels     map[string]*ResponseCollector
	responseMutex        sync.Mutex
	usedCorrelationIDs   map[string]usedCorrelationID
	usedIDsMutex         sync.RWMutex
}

// NewSocketProxy creates a new SocketProxy.
// broadcastMessageToClients sends the message to all connected WebSocket clients
func (p *SocketProxy) broadcastMessageToClients(direction, clientAddr string, clientPort int, data []byte) {
	msg := MessageWrapper{
		Source:       direction,
		ClientAddr:   clientAddr,
		ServerAddr:   fmt.Sprintf("localhost:%d", clientPort),
		ConnectionID: fmt.Sprintf("%s_%d", clientAddr, clientPort),
		Timestamp:    time.Now().Format(time.RFC3339),
		Message:      json.RawMessage(data),
	}
	msgBytes, err := json.Marshal(msg)
	if err == nil {
		broadcastMessage(websocket.TextMessage, msgBytes)
	} else {
		log.Printf("Error marshaling message for WebSocket: %v", err)
	}
}

// logMessage formats and logs a message with its direction and connection info
func (p *SocketProxy) logMessage(direction, clientAddr string, clientPort int, data []byte) {
	// Always broadcast the message to WebSocket clients, regardless of logging setting
	p.broadcastMessageToClients(direction, clientAddr, clientPort, data)

	if !p.enableMessageLogging {
		return
	}

	// Try to pretty print JSON if possible
	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, data, "", "  ")
	var message string
	if err != nil {
		// If not valid JSON, just show as string
		message = string(data)
	} else {
		message = prettyJSON.String()
	}

	log.Printf("[%s] %s:%d\n%s\n", direction, clientAddr, clientPort, message)
}

func NewSocketProxy(config *Config) *SocketProxy {
	// Set default API port if not specified
	if config.APIPort == 0 {
		config.APIPort = 8080
	}

	mappings := make(map[int]ServerConfig)
	for _, mapping := range config.ProxyMappings {
		mappings[mapping.ClientPort] = ServerConfig{
			Host: mapping.ServerHost,
			Port: mapping.ServerPort,
		}
	}

	identifierTimeouts := make(map[string]time.Duration)
	if config.IdentifierTimeouts != nil {
		for identifier, timeout := range config.IdentifierTimeouts {
			identifierTimeouts[identifier] = time.Duration(timeout) * time.Second
		}
	}

	noIdentifierTimeout := time.Duration(config.NoIdentifierTimeoutSeconds) * time.Second

	proxy := &SocketProxy{
		connections:          make(map[string]*ProxyConnection),
		listeners:            make(map[int]net.Listener),
		mappings:             mappings,
		defaultIdleTimeout:   time.Duration(config.DefaultTimeoutSeconds) * time.Second,
		noIdentifierTimeout:  noIdentifierTimeout,
		identifierTimeouts:   identifierTimeouts,
		enableMessageLogging: config.EnableMessageLogging,
		processorCfg:         config.MessageProcessor,
		responseChannels:     make(map[string]*ResponseCollector),
		usedCorrelationIDs:   make(map[string]usedCorrelationID),
	}

	// Start the cleanup goroutine for old correlation IDs (clean up every hour, keep IDs for 1 hour)
	proxy.startCorrelationIDCleanup(1*time.Hour, 1*time.Hour)

	return proxy
}

// Start initializes and starts all proxy servers.
func (p *SocketProxy) Start() {
	log.Println("Starting socket proxy...")

	// Initialize the listeners map if it's nil
	p.listenerMutex.Lock()
	if p.listeners == nil {
		p.listeners = make(map[int]net.Listener)
	}
	p.listenerMutex.Unlock()

	for clientPort, serverConfig := range p.mappings {
		go func(port int, config ServerConfig) {
			listenAddr := fmt.Sprintf("localhost:%d", port)
			listener, err := net.Listen("tcp", listenAddr)
			if err != nil {
				log.Printf("Failed to start server on port %d: %v", port, err)
				return
			}

			// Store the listener with mutex protection
			p.listenerMutex.Lock()
			p.listeners[port] = listener
			p.listenerMutex.Unlock()

			log.Printf("Proxy listening on port %d -> %s:%d", port, config.Host, config.Port)

			for {
				clientConn, err := listener.Accept()
				if err != nil {
					if isClosedConnError(err) {
						break
					}
					log.Printf("Failed to accept client connection on port %d: %v", port, err)
					continue
				}
				go p.handleClient(clientConn, port, config)
			}

			// Clean up the listener from the map when done
			p.listenerMutex.Lock()
			delete(p.listeners, port)
			p.listenerMutex.Unlock()
		}(clientPort, serverConfig)
	}

	log.Println("Socket proxy started successfully")
	go p.cleanupIdleConnections()
}

func (p *SocketProxy) handleClient(clientConn net.Conn, clientPort int, serverConfig ServerConfig) {
	clientAddr := clientConn.RemoteAddr().String()
	log.Printf("Client %s connected to port %d", clientAddr, clientPort)

	// Generate a connection ID using remote address, client port, and server port
	connectionID := fmt.Sprintf("%s_%d_%d", serverConfig.Host, clientPort, serverConfig.Port)

	proxyConn := &ProxyConnection{
		connectionID: connectionID,
		clientConn:   clientConn,
		clientPort:   clientPort,
		serverConfig: serverConfig,
		expiries:     []time.Time{},
		processorCfg: p.processorCfg,
		wsMutex:      sync.Mutex{},
		mutex:        sync.Mutex{},
	}

	// Store the connection in the proxy's connection map
	p.connMutex.Lock()
	if p.connections == nil {
		p.connections = make(map[string]*ProxyConnection)
	}
	p.connections[connectionID] = proxyConn
	p.connMutex.Unlock()

	defer func() {
		proxyConn.disconnectFromServer()
		proxyConn.disconnectFromMessageProcessor()
		p.connMutex.Lock()
		delete(p.connections, connectionID)
		p.connMutex.Unlock()
		clientConn.Close()
		log.Printf("Client %s disconnected from port %d", clientAddr, clientPort)
	}()

	buf := make([]byte, 4096)
	for {
		// Set a read deadline to prevent blocking forever
		if err := clientConn.SetReadDeadline(time.Now().Add(p.defaultIdleTimeout)); err != nil {
			log.Printf("Error setting read deadline on client connection: %v", err)
			break
		}

		n, err := clientConn.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // It's a timeout, continue listening
			}
			if err != io.EOF && !isClosedConnError(err) {
				log.Printf("Error reading from client %s: %v", clientAddr, err)
			}
			break
		}

		// Log the received message
		p.logMessage("CLIENT -> SERVER", clientAddr, clientPort, buf[:n])

		var msg ClientMessage
		parsedWithIdentifier := false
		if jsonErr := json.Unmarshal(buf[:n], &msg); jsonErr == nil {
			if msg.Identifier != "" {
				if timeout, ok := p.identifierTimeouts[msg.Identifier]; ok {
					// log.Printf("Identifier '%s' found, starting a timer for %v", msg.Identifier, timeout)
					proxyConn.addExpiry(timeout)
					parsedWithIdentifier = true
				} else {
					proxyConn.addExpiry(p.defaultIdleTimeout)
					parsedWithIdentifier = true
				}
			}
		}

		if !parsedWithIdentifier {
			proxyConn.addExpiry(p.noIdentifierTimeout)
		}

		if !proxyConn.isConnectedToServer() {
			if !proxyConn.connectToServer() {
				log.Printf("Failed to connect to server for client %s", clientAddr)
				break
			}
			ctx, cancel := context.WithCancel(context.Background())
			proxyConn.cancelForward = cancel
			go proxyConn.forwardServerToClient(clientConn, clientAddr, ctx, p)
		}

		processedData := buf[:n]
		if proxyConn.processorCfg.Enabled {
			// log.Printf("DEBUG: Sending to processor: %s", string(processedData))
			processedResponse, blocked, err := proxyConn.forwardToProcessor(processedData, "client")
			if err != nil {
				log.Printf("Error processing message: %v. Forwarding original message.", err)
				// Keep the original message if there was an error
				processedData = buf[:n]
			} else if blocked {
				log.Printf("Message blocked by processor")
				// Skip sending this message to the server and continue to the next message
				continue
			} else {
				// Only use the processed data if we got a valid response
				if len(processedResponse) > 0 {
					processedData = processedResponse
					// log.Printf("DEBUG: Using processed message: %s", string(processedData))
				} else {
					log.Printf("WARNING: Empty response from processor, using original message")
				}
			}
		}

		// log.Printf("DEBUG: Forwarding to server: %s", string(processedData))
		if _, err := proxyConn.serverConn.Write(processedData); err != nil {
			log.Printf("Error writing to server: %v", err)
			proxyConn.disconnectFromServer()
			break
		}
	}
}

func (p *SocketProxy) cleanupIdleConnections() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		p.connMutex.Lock()
		for connectionID, proxyConn := range p.connections {
			if proxyConn.isConnectedToServer() && proxyConn.shouldDisconnect() {
				log.Printf("Connection %s has no active timers, disconnecting from server", connectionID)
				proxyConn.disconnectFromServer()
			}
		}
		p.connMutex.Unlock()
	}
}

func (p *SocketProxy) Stop() {
	log.Println("Stopping socket proxy...")

	// Create a copy of the listeners map to avoid holding the lock while closing
	p.listenerMutex.Lock()
	listenersCopy := make(map[int]net.Listener, len(p.listeners))
	for port, listener := range p.listeners {
		listenersCopy[port] = listener
	}
	p.listeners = make(map[int]net.Listener) // Clear the map while holding the lock
	p.listenerMutex.Unlock()

	// Close all listeners
	for port, listener := range listenersCopy {
		log.Printf("Closing listener on port %d", port)
		if err := listener.Close(); err != nil {
			log.Printf("Error closing listener on port %d: %v", port, err)
		}
	}

	log.Println("Socket proxy stopped")
}

// getResponseCollector creates a new response collector for a correlation ID
func (p *SocketProxy) getResponseCollector(correlationID string, expectedCount int) *ResponseCollector {
	p.responseMutex.Lock()
	defer p.responseMutex.Unlock()

	if p.responseChannels == nil {
		p.responseChannels = make(map[string]*ResponseCollector)
	}

	collector := &ResponseCollector{
		Expected:  expectedCount,
		Ch:        make(chan struct{}),
		Responses: []string{},
	}

	p.responseChannels[correlationID] = collector
	return collector
}

// getExistingCollector gets an existing response collector if it exists
func (p *SocketProxy) getExistingCollector(correlationID string) (*ResponseCollector, bool) {
	p.responseMutex.Lock()
	defer p.responseMutex.Unlock()

	if collector, exists := p.responseChannels[correlationID]; exists {
		return collector, true
	}
	return nil, false
}

// cleanupResponseChannel removes a response collector when it's no longer needed
// isCorrelationIDUsed checks if a correlation ID has already been used
func (p *SocketProxy) isCorrelationIDUsed(correlationID string) bool {
	p.usedIDsMutex.RLock()
	defer p.usedIDsMutex.RUnlock()
	_, exists := p.usedCorrelationIDs[correlationID]
	return exists
}

// markCorrelationIDUsed marks a correlation ID as used with the current timestamp
func (p *SocketProxy) markCorrelationIDUsed(correlationID string) {
	p.usedIDsMutex.Lock()
	defer p.usedIDsMutex.Unlock()

	if p.usedCorrelationIDs == nil {
		p.usedCorrelationIDs = make(map[string]usedCorrelationID)
	}
	p.usedCorrelationIDs[correlationID] = usedCorrelationID{
		timestamp: time.Now(),
	}
}

// cleanupOldCorrelationIDs removes correlation IDs that are older than maxAge
func (p *SocketProxy) cleanupOldCorrelationIDs(maxAge time.Duration) {
	p.usedIDsMutex.Lock()
	defer p.usedIDsMutex.Unlock()

	if p.usedCorrelationIDs == nil {
		return
	}

	cutoff := time.Now().Add(-maxAge)
	for id, entry := range p.usedCorrelationIDs {
		if entry.timestamp.Before(cutoff) {
			delete(p.usedCorrelationIDs, id)
		}
	}
}

// startCorrelationIDCleanup starts a background goroutine to clean up old correlation IDs
func (p *SocketProxy) startCorrelationIDCleanup(cleanupInterval, keepDuration time.Duration) {
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()

		for range ticker.C {
			p.cleanupOldCorrelationIDs(keepDuration)
		}
	}()
}

func (p *SocketProxy) cleanupResponseChannel(correlationID string) {
	p.responseMutex.Lock()
	defer p.responseMutex.Unlock()

	if collector, exists := p.responseChannels[correlationID]; exists {
		if !collector.Complete {
			close(collector.Ch)
		}
		delete(p.responseChannels, correlationID)

		// Mark this correlation ID as used
		p.markCorrelationIDUsed(correlationID)
	}
}

func (p *SocketProxy) handleReconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is accepted", http.StatusMethodNotAllowed)
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	p.connMutex.Lock()
	conn, ok := p.connections[req.ConnectionID]
	p.connMutex.Unlock()

	if !ok {
		http.Error(w, "Connection not found", http.StatusNotFound)
		return
	}

	log.Printf("API: Forcing reconnect for %s", req.ConnectionID)
	conn.disconnectFromServer()
	if !conn.connectToServer() {
		http.Error(w, "Failed to reconnect to server", http.StatusInternalServerError)
		return
	}
	if req.Timeout > 0 {
		conn.addExpiry(time.Duration(req.Timeout) * time.Second)
	} else {
		conn.addExpiry(p.defaultIdleTimeout)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Reconnected successfully"))
}

func (p *SocketProxy) handleSendToClient(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is accepted", http.StatusMethodNotAllowed)
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	p.connMutex.Lock()
	conn, ok := p.connections[req.ConnectionID]
	p.connMutex.Unlock()

	if !ok {
		http.Error(w, "Connection not found", http.StatusNotFound)
		return
	}

	log.Printf("API: Sending message to client %s", req.ConnectionID)
	if err := conn.writeToClient(conn.clientConn, conn.clientConn.RemoteAddr().String(), []byte(req.Data), p); err != nil {
		http.Error(w, "Failed to send message to client", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Message sent to client"))
}

func (p *SocketProxy) handleSendToServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is accepted", http.StatusMethodNotAllowed)
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	p.connMutex.Lock()
	conn, ok := p.connections[req.ConnectionID]
	p.connMutex.Unlock()

	if !ok {
		http.Error(w, "Connection not found", http.StatusNotFound)
		return
	}

	log.Printf("API: Sending message to server for %s", req.ConnectionID)
	if !conn.isConnectedToServer() {
		log.Printf("API: Server disconnected, attempting to reconnect for %s", req.ConnectionID)
		if !conn.connectToServer() {
			http.Error(w, "Failed to reconnect to server", http.StatusInternalServerError)
			return
		}
	}

	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if _, err := conn.serverConn.Write(ensureNewline([]byte(req.Data))); err != nil {
		http.Error(w, "Failed to send message to server", http.StatusInternalServerError)
		return
	}
	p.logMessage("sent-by-api", conn.clientConn.RemoteAddr().String(), conn.clientPort, ensureNewline([]byte(req.Data)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Message sent to server"))
}

func (p *SocketProxy) handleSendAndWaitResponse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is accepted", http.StatusMethodNotAllowed)
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Generate a new correlation ID
	correlationID := generateCorrelationID()

	// Set default values
	if req.ExpectedCount < 0 {
		req.ExpectedCount = 0 // Default to single response
	}

	timeout := time.Duration(30) * time.Second // Default timeout
	if req.Timeout > 0 {
		timeout = time.Duration(req.Timeout) * time.Second
	}

	// Get the connection
	p.connMutex.Lock()
	conn, exists := p.connections[req.ConnectionID]
	p.connMutex.Unlock()

	if !exists {
		http.Error(w, "Connection not found", http.StatusNotFound)
		return
	}

	// Create response collector
	collector := p.getResponseCollector(correlationID, req.ExpectedCount)
	defer p.cleanupResponseChannel(correlationID)

	// Send the message to the server
	if !conn.isConnectedToServer() {
		log.Printf("API: Server disconnected, attempting to reconnect for %s", req.ConnectionID)
		if !conn.connectToServer() {
			http.Error(w, "Failed to reconnect to server", http.StatusInternalServerError)
			return
		}
	}

	conn.mutex.Lock()
	_, err := conn.serverConn.Write(ensureNewline([]byte(req.Data)))
	conn.mutex.Unlock()

	if err != nil {
		http.Error(w, "Failed to send message to server", http.StatusInternalServerError)
		return
	}

	p.logMessage("sent-by-api-waiting", conn.clientConn.RemoteAddr().String(), conn.clientPort, ensureNewline([]byte(req.Data)))

	// Wait for response(s) with timeout
	select {
	case <-collector.Ch:
		// All expected responses received or single response if ExpectedCount was 0
		response := map[string]interface{}{
			"correlationId": correlationID,
			"responses":     collector.Responses,
			"complete":      collector.Complete,
			"count":         collector.Count,
			"expectedCount": req.ExpectedCount,
			"timeout":       false,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)

	case <-time.After(timeout):
		// Return whatever we have so far
		response := map[string]interface{}{
			"correlationId": correlationID,
			"responses":     collector.Responses,
			"complete":      false,
			"count":         collector.Count,
			"expectedCount": req.ExpectedCount,
			"timeout":       true,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}
