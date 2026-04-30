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
	mappings             map[string]ServerConfig
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

func (p *SocketProxy) broadcastMessageToClients(source MessageSource, clientAddr string, clientPort, serverPort int, data []byte) {
	// Fetch the connection ID from the configured mappings
	connectionID := "unknown"
	for _, mapping := range p.mappings {
		if mapping.ClientPort == clientPort {
			connectionID = mapping.ConnectionID
			break
		}
	}

	msg := MessageWrapper{
		Source:       source,
		ClientAddr:   clientAddr,
		ServerAddr:   fmt.Sprintf("localhost:%d", serverPort),
		ConnectionID: connectionID,
		IsAPI:        source == SourceAPIClient || source == SourceAPIServer,
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
func (p *SocketProxy) logMessage(source MessageSource, clientAddr string, clientPort int, data []byte) {
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

	connectionID := "unknown"
	for _, mapping := range p.mappings {
		if mapping.ClientPort == clientPort {
			connectionID = mapping.ConnectionID
			break
		}
	}

	log.Printf("[from %s] [%s] %s:%d\n%s\n", source, connectionID, clientAddr, clientPort, message)
}

// NewSocketProxy creates a new SocketProxy.
func NewSocketProxy(config *Config) *SocketProxy {
	// Set default API port if not specified
	if config.APIPort == 0 {
		config.APIPort = 8080
	}

	mappings := make(map[string]ServerConfig)
	for _, mapping := range config.ProxyMappings {
		mappings[mapping.ConnectionID] = ServerConfig{
			ConnectionID: mapping.ConnectionID,
			ClientHost:   mapping.ClientHost,
			ClientPort:   mapping.ClientPort,
			Host:         mapping.ServerHost,
			Port:         mapping.ServerPort,
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

	for connectionID, serverConfig := range p.mappings {
		go func(connID string, port int, config ServerConfig) {
			listenAddr := fmt.Sprintf("%s:%d", config.ClientHost, port)
			listener, err := net.Listen("tcp", listenAddr)
			if err != nil {
				log.Printf("[%s] Failed to start server on port %d: %v", connID, port, err)
				return
			}

			// Store the listener with mutex protection
			p.listenerMutex.Lock()
			p.listeners[port] = listener
			p.listenerMutex.Unlock()

			log.Printf("[%s] Proxy listening on port %d -> %s:%d", connID, port, config.Host, config.Port)

			for {
				clientConn, err := listener.Accept()
				if err != nil {
					if isClosedConnError(err) {
						break
					}
					log.Printf("[%s] Failed to accept client connection on port %d: %v", connID, port, err)
					continue
				}
				go p.handleClient(clientConn, port, config)
			}

			// Clean up the listener from the map when done
			p.listenerMutex.Lock()
			delete(p.listeners, port)
			p.listenerMutex.Unlock()
		}(connectionID, serverConfig.ClientPort, serverConfig)
	}

	log.Println("Socket proxy started successfully")
	go p.cleanupIdleConnections()
}

func (p *SocketProxy) handleClient(clientConn net.Conn, clientPort int, serverConfig ServerConfig) {
	clientAddr := clientConn.RemoteAddr().String()
	// Use the explicit connection ID configured for this proxy mapping
	connectionID := serverConfig.ConnectionID

	log.Printf("[%s] Client %s connected to port %d", connectionID, clientAddr, clientPort)

	proxyConn := &ProxyConnection{
		connectionID: connectionID,
		clientConn:   clientConn,
		clientAddr:   clientConn.RemoteAddr(),
		clientPort:   clientConn.LocalAddr().(*net.TCPAddr).Port,
		serverConfig: serverConfig,
		config:       p.config,
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
		p.connMutex.Lock()
		delete(p.connections, connectionID)
		p.connMutex.Unlock()
		clientConn.Close()
		log.Printf("[%s] Client %s disconnected from port %d", connectionID, clientAddr, clientPort)
	}()

	buf := make([]byte, 4096)
	for {
		// Set a read deadline to prevent blocking forever
		if err := clientConn.SetReadDeadline(time.Now().Add(p.defaultIdleTimeout)); err != nil {
			log.Printf("[%s] Error setting read deadline on client connection: %v", connectionID, err)
			break
		}

		n, err := clientConn.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // It's a timeout, continue listening
			}
			if err != io.EOF && !isClosedConnError(err) {
				log.Printf("[%s] Error reading from client %s: %v", connectionID, clientAddr, err)
			}
			break
		}

		// Log the received message
		p.logMessage(SourceClient, clientAddr, clientPort, buf[:n])

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
				log.Printf("[%s] Failed to connect to server for client %s", connectionID, clientAddr)
				continue
			}
		}

		processedData := buf[:n]
		if proxyConn.config != nil && proxyConn.config.MessageProcessor.Enabled {
			// log.Printf("DEBUG: Sending to processor: %s", string(processedData))
			processedResponse, blocked, err := proxyConn.forwardToProcessor(processedData, SourceClient, false)
			if err != nil {
				log.Printf("[%s] Error processing message: %v. Forwarding original message.", connectionID, err)
				// Keep the original message if there was an error
				processedData = buf[:n]
			} else if blocked {
				log.Printf("[%s] Message blocked by processor", connectionID)
				// Skip sending this message to the server and continue to the next message
				continue
			} else {
				// Only use the processed data if we got a valid response
				if len(processedResponse) > 0 {
					processedData = processedResponse
					// log.Printf("DEBUG: Using processed message: %s", string(processedData))
				} else {
					log.Printf("[%s] WARNING: Empty response from processor, using original message", connectionID)
				}
			}
		}

		// log.Printf("DEBUG: Forwarding to server: %s", string(processedData))
		maxRetries := p.config.MaxWriteRetries
		if maxRetries <= 0 {
			maxRetries = 1 // Default to 1 attempt if not configured
		}

		var writeErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			proxyConn.mutex.Lock()
			if proxyConn.serverConn != nil {
				_, writeErr = proxyConn.serverConn.Write(processedData)
				proxyConn.mutex.Unlock() // Unlock right after using shared resource

				if writeErr == nil {
					break // Success
				}

				if attempt < maxRetries-1 {
					log.Printf("[%s] Error writing to server (attempt %d/%d), reconnecting: %v", connectionID, attempt+1, maxRetries, writeErr)
					proxyConn.disconnectFromServer()
					proxyConn.connectToServer(p)
				}
			} else {
				proxyConn.mutex.Unlock() // Unlock if connection is already nil
				writeErr = fmt.Errorf("server connection is closed")

				if attempt < maxRetries-1 {
					log.Printf("[%s] Server connection was closed (attempt %d/%d), reconnecting...", connectionID, attempt+1, maxRetries)
					proxyConn.connectToServer(p)
				}
			}
		}

		if writeErr != nil {
			log.Printf("[%s] Failed to write to server after %d attempts: %v", connectionID, maxRetries, writeErr)
			proxyConn.disconnectFromServer()
			continue
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
				log.Printf("[%s] Connection has no active timers, disconnecting from server", connectionID)
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
		Responses: []json.RawMessage{},
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

func sendSuccessResponse(w http.ResponseWriter, message string) {
	sendJSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": message,
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

	sendSuccessResponse(w, "Reconnected successfully")
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
		processed, blocked, err := conn.forwardToProcessor(message, SourceAPIServer, true)
		if err != nil {
			log.Printf("[%s] Error processing API message: %v, forwarding original message", req.ConnectionID, err)
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
	p.logMessage(SourceAPIServer, conn.clientConn.RemoteAddr().String(), conn.clientPort, message)

	sendSuccessResponse(w, "Message sent to client")
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
		processed, blocked, err := conn.forwardToProcessor(message, SourceAPIClient, true)
		if err != nil {
			log.Printf("[%s] Error processing API message: %v, forwarding original message", req.ConnectionID, err)
		} else if blocked {
			sendErrorResponse(w, http.StatusForbidden, "Message blocked by processor")
			return
		} else if len(processed) > 0 {
			// Use the processed message if we got one back
			message = processed
		}
	}

	maxRetries := p.config.MaxWriteRetries
	if maxRetries <= 0 {
		maxRetries = 1 // Default to 1 attempt if not configured
	}

	var writeErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		conn.mutex.Lock()
		if conn.serverConn != nil {
			_, writeErr = conn.serverConn.Write(message)
			conn.mutex.Unlock()

			if writeErr == nil {
				break // Success
			}

			if attempt < maxRetries-1 {
				log.Printf("[%s] API Error writing to server (attempt %d/%d), reconnecting: %v", req.ConnectionID, attempt+1, maxRetries, writeErr)
				conn.disconnectFromServer()
				conn.connectToServer(p)
			}
		} else {
			conn.mutex.Unlock()
			writeErr = fmt.Errorf("server connection is closed")

			if attempt < maxRetries-1 {
				log.Printf("[%s] API Server connection was closed (attempt %d/%d), reconnecting...", req.ConnectionID, attempt+1, maxRetries)
				conn.connectToServer(p)
			}
		}
	}

	if writeErr != nil {
		log.Printf("[%s] API Failed to send message to server after %d attempts: %v", req.ConnectionID, maxRetries, writeErr)
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to send message to server")
		return
	}
	p.logMessage(SourceAPIClient, conn.clientConn.RemoteAddr().String(), conn.clientPort, ensureNewline([]byte(req.Data)))

	sendSuccessResponse(w, "Message sent to server")
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
		sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is accepted")
		return
	}

	var req APIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Parse the request data to check for CorrelationId
	var dataMap map[string]interface{}
	var correlationID string

	if err := json.Unmarshal([]byte(req.Data), &dataMap); err == nil {
		// If CorrelationId exists in the data and is a non-empty string, use it
		if id, ok := dataMap["CorrelationId"].(string); ok && id != "" {
			correlationID = id
		} else {
			correlationID = generateCorrelationID()
		}
	} else {
		// If we can't parse the JSON, generate a new correlation ID
		correlationID = generateCorrelationID()
	}

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
			sendErrorResponse(w, http.StatusInternalServerError, "Failed to reconnect to server")
			return
		}
	}

	maxRetries := p.config.MaxWriteRetries
	if maxRetries <= 0 {
		maxRetries = 1 // Default to 1 attempt if not configured
	}

	var writeErr error
	msgData := ensureNewline([]byte(req.Data))
	for attempt := 0; attempt < maxRetries; attempt++ {
		conn.mutex.Lock()
		if conn.serverConn != nil {
			_, writeErr = conn.serverConn.Write(msgData)
			conn.mutex.Unlock()

			if writeErr == nil {
				break // Success
			}

			if attempt < maxRetries-1 {
				log.Printf("[%s] API Error writing to server and wait response (attempt %d/%d), reconnecting: %v", req.ConnectionID, attempt+1, maxRetries, writeErr)
				conn.disconnectFromServer()
				conn.connectToServer(p)
			}
		} else {
			conn.mutex.Unlock()
			writeErr = fmt.Errorf("server connection is closed")

			if attempt < maxRetries-1 {
				log.Printf("[%s] API Server connection was closed for send and wait response (attempt %d/%d), reconnecting...", req.ConnectionID, attempt+1, maxRetries)
				conn.connectToServer(p)
			}
		}
	}

	if writeErr != nil {
		log.Printf("[%s] API Failed to send message to server and wait response after %d attempts: %v", req.ConnectionID, maxRetries, writeErr)
		sendErrorResponse(w, http.StatusInternalServerError, "Failed to send message to server")
		return
	}

	p.logMessage(SourceAPIClient, conn.clientConn.RemoteAddr().String(), conn.clientPort, ensureNewline([]byte(req.Data)))

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
