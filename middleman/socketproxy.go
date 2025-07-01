package main

import (
	"bytes"
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
	config               *Config // Reference to the main config
	responseChannels     map[string]*ResponseCollector
	responseMutex        sync.Mutex
	usedCorrelationIDs   map[string]usedCorrelationID
	usedIDsMutex         sync.RWMutex
}

// broadcastMessageToClients sends the message to all connected WebSocket clients
func (p *SocketProxy) broadcastMessageToClients(source, clientAddr string, clientPort, serverPort int, data []byte) {
	// Extract just the IP address from the client address
	clientIP := clientAddr
	if host, _, err := net.SplitHostPort(clientAddr); err == nil {
		clientIP = host
	}

	msg := MessageWrapper{
		Source:       source,
		ClientAddr:   clientAddr,
		ServerAddr:   fmt.Sprintf("localhost:%d", serverPort),
		ConnectionID: fmt.Sprintf("%s_%d_%d", clientIP, clientPort, serverPort),
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
func (p *SocketProxy) logMessage(source, clientAddr string, clientPort int, data []byte) {
	// Always broadcast the message to WebSocket clients, regardless of logging setting
	// We need to find the connection to get the server port
	p.connMutex.Lock()
	defer p.connMutex.Unlock()

	// Try to find a matching connection to get the server port
	serverPort := 0 // Default to 0 if we can't find the connection
	for _, conn := range p.connections {
		if conn.clientConn.RemoteAddr().String() == clientAddr && conn.clientPort == clientPort {
			serverPort = conn.serverConfig.Port
			break
		}
	}

	p.broadcastMessageToClients(source, clientAddr, clientPort, serverPort, data)

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

	log.Printf("[from %s] %s:%d\n%s\n", source, clientAddr, clientPort, message)
}

// NewSocketProxy creates a new SocketProxy.
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
		config:               config,
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
		clientAddr:   clientConn.RemoteAddr(),
		clientPort:   clientConn.LocalAddr().(*net.TCPAddr).Port,
		serverConfig: serverConfig,
		config:       p.config,
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
		p.logMessage("CLIENT", clientAddr, clientPort, buf[:n])

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
			if !proxyConn.connectToServer(p) {
				log.Printf("Failed to connect to server for client %s", clientAddr)
				break
			}
		}

		processedData := buf[:n]
		if proxyConn.config != nil && proxyConn.config.MessageProcessor.Enabled {
			// log.Printf("DEBUG: Sending to processor: %s", string(processedData))
			processedResponse, blocked, err := proxyConn.forwardToProcessor(processedData, "client", false)
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

// sendJSONResponse sends a JSON response with the given status code and data
func sendJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

// sendErrorResponse sends a JSON error response
func sendErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	sendJSONResponse(w, statusCode, map[string]interface{}{
		"success": false,
		"error":   message,
	})
}

func (p *SocketProxy) handleReconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is accepted")
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	p.connMutex.Lock()
	conn, ok := p.connections[req.ConnectionID]
	p.connMutex.Unlock()

	if !ok {
		sendErrorResponse(w, http.StatusNotFound, "Connection not found")
		return
	}

	log.Printf("API: Forcing reconnect for %s", req.ConnectionID)
	conn.disconnectFromServer()
	if !conn.connectToServer(p) {
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to reconnect to server")
		return
	}
	if req.Timeout > 0 {
		conn.addExpiry(time.Duration(req.Timeout) * time.Second)
	} else {
		conn.addExpiry(p.defaultIdleTimeout)
	}

	response := map[string]interface{}{
		"success": true,
		"message": "Reconnected successfully",
	}
	sendJSONResponse(w, http.StatusOK, response)
}

// ConnectionStatus represents the status of a connection
type ConnectionStatus struct {
	ClientConnected bool   `json:"clientConnected"`
	ServerConnected bool   `json:"serverConnected"`
	TimeUntilClose  int64  `json:"timeUntilClose"` // in seconds
	ConnectionID    string `json:"connectionId"`
}

// handleConnectionStatus returns the status of a connection
func (p *SocketProxy) handleConnectionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET method is accepted")
		return
	}

	connectionID := r.URL.Query().Get("connectionId")
	if connectionID == "" {
		sendErrorResponse(w, http.StatusBadRequest, "connectionId parameter is required")
		return
	}

	p.connMutex.Lock()
	conn, ok := p.connections[connectionID]
	p.connMutex.Unlock()

	if !ok {
		sendErrorResponse(w, http.StatusNotFound, "Connection not found")
		return
	}

	status := ConnectionStatus{
		ClientConnected: true, // If we have the connection, client is connected
		ServerConnected: conn.isConnectedToServer(),
		ConnectionID:    connectionID,
	}

	// Calculate time until connection expires
	expiresAt := conn.GetExpiresAt()
	if !expiresAt.IsZero() {
		timeRemaining := time.Until(expiresAt).Seconds()
		if timeRemaining > 0 {
			status.TimeUntilClose = int64(timeRemaining)
		} else {
			status.TimeUntilClose = 0
		}
	} else {
		status.TimeUntilClose = -1 // No expiration set
	}

	sendJSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"status":  status,
	})
}

// handleEnsureConnected ensures the connection to the server is active.
// If the connection is not active, it will attempt to reconnect.
// If the connection is already active, it will remain connected.
func (p *SocketProxy) handleEnsureConnected(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is accepted")
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	p.connMutex.Lock()
	conn, ok := p.connections[req.ConnectionID]
	p.connMutex.Unlock()

	if !ok {
		sendErrorResponse(w, http.StatusNotFound, "Connection not found")
		return
	}

	// Check if we need to reconnect
	reconnected := false
	if !conn.isConnectedToServer() {
		log.Printf("API: Connection %s is not connected, attempting to reconnect.", req.ConnectionID)
		if !conn.connectToServer(p) {
			sendErrorResponse(w, http.StatusInternalServerError, "Failed to reconnect to server")
			return
		}
		reconnected = true
	}

	// Update the connection timeout if specified
	if req.Timeout > 0 {
		conn.addExpiry(time.Duration(req.Timeout) * time.Second)
	} else {
		conn.addExpiry(p.defaultIdleTimeout)
	}

	response := map[string]interface{}{
		"success":     true,
		"message":     "Connection is active",
		"reconnected": reconnected,
		"connected":   conn.isConnectedToServer(),
	}
	sendJSONResponse(w, http.StatusOK, response)
}

func (p *SocketProxy) handleSendToClient(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is accepted")
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	p.connMutex.Lock()
	conn, ok := p.connections[req.ConnectionID]
	p.connMutex.Unlock()

	if !ok {
		sendErrorResponse(w, http.StatusNotFound, "Connection not found")
		return
	}

	message := ensureNewline([]byte(req.Data))

	log.Printf("API: Sending message to client %s", req.ConnectionID)
	// If message processor is enabled, forward through it
	if conn.config != nil && conn.config.MessageProcessor.Enabled {
		processed, blocked, err := conn.forwardToProcessor(message, "API-server", true)
		if err != nil {
			log.Printf("Error processing API message: %v, forwarding original message", err)
		} else if blocked {
			sendErrorResponse(w, http.StatusForbidden, "Message blocked by processor")
			return
		} else if len(processed) > 0 {
			// Use the processed message if we got one back
			message = processed
		}
	}

	if err := conn.writeToClient(conn.clientConn, conn.clientConn.RemoteAddr().String(), message, p); err != nil {
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to send message to client")
		return
	}
	p.logMessage("API-SERVER", conn.clientConn.RemoteAddr().String(), conn.clientPort, message)

	response := map[string]interface{}{
		"success": true,
		"message": "Message sent to client",
	}
	sendJSONResponse(w, http.StatusOK, response)
}

func (p *SocketProxy) handleSendToServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is accepted")
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	p.connMutex.Lock()
	conn, ok := p.connections[req.ConnectionID]
	p.connMutex.Unlock()

	if !ok {
		sendErrorResponse(w, http.StatusNotFound, "Connection not found")
		return
	}

	log.Printf("API: Sending message to server for %s", req.ConnectionID)
	if !conn.isConnectedToServer() {
		log.Printf("API: Connection %s is not connected, attempting to reconnect.", req.ConnectionID)
		if !conn.connectToServer(p) {
			sendErrorResponse(w, http.StatusInternalServerError, "Failed to reconnect to server")
			return
		}
	}

	message := ensureNewline([]byte(req.Data))

	// If message processor is enabled, forward through it
	if conn.config != nil && conn.config.MessageProcessor.Enabled {
		processed, blocked, err := conn.forwardToProcessor(message, "API-client", true)
		if err != nil {
			log.Printf("Error processing API message: %v, forwarding original message", err)
		} else if blocked {
			sendErrorResponse(w, http.StatusForbidden, "Message blocked by processor")
			return
		} else if len(processed) > 0 {
			// Use the processed message if we got one back
			message = processed
		}
	}

	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if _, err := conn.serverConn.Write(message); err != nil {
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to send message to server")
		return
	}
	p.logMessage("API-CLIENT", conn.clientConn.RemoteAddr().String(), conn.clientPort, ensureNewline([]byte(req.Data)))

	response := map[string]interface{}{
		"success": true,
		"message": "Message sent to server",
	}
	sendJSONResponse(w, http.StatusOK, response)
}

func (p *SocketProxy) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET method is allowed")
		return
	}

	// Create a safe copy of the config to return
	configCopy := *p.config
	// Don't expose sensitive information if any
	// For example: configCopy.SomeSensitiveField = ""

	sendJSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"config":  configCopy,
	})
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
		sendErrorResponse(w, http.StatusNotFound, "Connection not found")
		return
	}

	// Create response collector
	collector := p.getResponseCollector(correlationID, req.ExpectedCount)
	defer p.cleanupResponseChannel(correlationID)

	// Send the message to the server
	if !conn.isConnectedToServer() {
		log.Printf("API: Server disconnected, attempting to reconnect for %s", req.ConnectionID)
		if !conn.connectToServer(p) {
			http.Error(w, "Failed to reconnect to server", http.StatusInternalServerError)
			return
		}
	}

	conn.mutex.Lock()
	_, err := conn.serverConn.Write(ensureNewline([]byte(req.Data)))
	conn.mutex.Unlock()

	if err != nil {
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to send message to server")
		return
	}

	p.logMessage("API-CLIENT", conn.clientConn.RemoteAddr().String(), conn.clientPort, ensureNewline([]byte(req.Data)))

	// Wait for response(s) with timeout
	select {
	case <-collector.Ch:
		// All expected responses received or single response if ExpectedCount was 0
		response := map[string]interface{}{
			"success":       true,
			"correlationId": correlationID,
			"responses":     collector.Responses,
			"complete":      collector.Complete,
			"count":         collector.Count,
			"expectedCount": req.ExpectedCount,
			"timeout":       false,
		}
		sendJSONResponse(w, http.StatusOK, response)

	case <-time.After(timeout):
		// Return whatever we have so far
		response := map[string]interface{}{
			"success":       true,
			"correlationId": correlationID,
			"responses":     collector.Responses,
			"complete":      false,
			"count":         collector.Count,
			"expectedCount": req.ExpectedCount,
			"timeout":       true,
		}
		sendJSONResponse(w, http.StatusOK, response)
	}
}
