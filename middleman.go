package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
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
	Responses []string
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

// Config holds the full configuration from the JSON file.
type Config struct {
	EnableMessageLogging       bool           `json:"enableMessageLogging"`
	DefaultTimeoutSeconds      int            `json:"defaultTimeoutSeconds"`
	NoIdentifierTimeoutSeconds int            `json:"noIdentifierTimeoutSeconds"`
	IdentifierTimeouts         map[string]int `json:"identifier_timeouts"`
	APIPort                    int            `json:"apiPort"`
	ProxyMappings              []struct {
		ClientPort int    `json:"clientPort"`
		ServerHost string `json:"serverHost"`
		ServerPort int    `json:"serverPort"`
	} `json:"proxy_mappings"`
	MessageProcessor MessageProcessorConfig `json:"messageProcessor"`
}

// loadConfig reads the configuration file and returns the configuration.
func loadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// ProxyConnection manages the connection between a client and a server.
type ProxyConnection struct {
	connectionID  string
	clientConn    net.Conn
	clientPort    int
	serverConfig  ServerConfig
	serverConn    net.Conn
	mutex         sync.Mutex
	cancelForward context.CancelFunc
	expiries      []time.Time
	wsConn        *websocket.Conn
	wsMutex       sync.Mutex
	processorCfg  MessageProcessorConfig
}

// SocketProxy manages all proxy connections.
// usedCorrelationID tracks when a correlation ID was marked as used
type usedCorrelationID struct {
	timestamp time.Time
}

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
// logMessage formats and logs a message with its direction and connection info
func (p *SocketProxy) logMessage(direction, clientAddr string, clientPort int, data []byte) {
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

func (pc *ProxyConnection) connectToServer() bool {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	serverAddr := net.JoinHostPort(pc.serverConfig.Host, strconv.Itoa(pc.serverConfig.Port))
	log.Printf("Connecting to server %s", serverAddr)
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Printf("Failed to connect to server %s: %v", serverAddr, err)
		return false
	}
	pc.serverConn = conn
	log.Printf("Connected to server %s", serverAddr)
	return true
}

func (pc *ProxyConnection) disconnectFromServer() {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	if pc.serverConn != nil {
		log.Printf("Disconnecting from server %s:%d", pc.serverConfig.Host, pc.serverConfig.Port)
		if tcpConn, ok := pc.serverConn.(*net.TCPConn); ok {
			tcpConn.SetLinger(0)
		}
		pc.serverConn.Close()
		pc.serverConn = nil
	}
}

func (pc *ProxyConnection) disconnectFromMessageProcessor() {
	pc.wsMutex.Lock()
	defer pc.wsMutex.Unlock()

	if pc.cancelForward != nil {
		pc.cancelForward()
		pc.cancelForward = nil
	}

	if pc.wsConn != nil {
		log.Printf("Disconnecting from message processor for %s", pc.connectionID)
		pc.wsConn.Close()
		pc.wsConn = nil
	}
}

func (pc *ProxyConnection) isConnectedToServer() bool {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	return pc.serverConn != nil
}

// addExpiry adds a new expiry timer to the connection.
func (pc *ProxyConnection) addExpiry(duration time.Duration) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	expiry := time.Now().Add(duration)
	pc.expiries = append(pc.expiries, expiry)
}

// writeToClient writes data to the client connection with error handling and logging
func (pc *ProxyConnection) writeToClient(conn net.Conn, clientAddr string, data []byte, proxy *SocketProxy) error {
	if len(data) == 0 {
		return nil
	}

	// Log the message being sent to the client
	proxy.logMessage("SERVER -> CLIENT", clientAddr, pc.clientPort, data)

	// Write the data to the client
	_, err := conn.Write(data)
	if err != nil && !isClosedConnError(err) {
		log.Printf("Error writing to client %s: %v", clientAddr, err)
	}
	return err
}

// processServerMessage processes a complete message from the server
func (pc *ProxyConnection) processServerMessage(clientConn net.Conn, message []byte, proxy *SocketProxy) {
	if len(message) == 0 {
		return
	}

	// Log the received message
	if proxy.enableMessageLogging {
		proxy.logMessage("SERVER -> CLIENT", clientConn.RemoteAddr().String(), pc.clientPort, message)
	}

	// Parse the message as JSON to check for both Identifier and CorrelationID
	var msg ServerMessage
	if err := json.Unmarshal(message, &msg); err == nil {
		// Handle Identifier for expiry
		if msg.Identifier != "" {
			if timeout, ok := proxy.identifierTimeouts[msg.Identifier]; ok {
				pc.addExpiry(timeout)
			} else {
				pc.addExpiry(proxy.defaultIdleTimeout)
			}
		} else {
			// If no identifier, use the no-identifier timeout
			pc.addExpiry(proxy.noIdentifierTimeout)
		}

		// Handle CorrelationID for response tracking
		if correlationID := msg.CorrelationID; correlationID != "" {
			// First check if this correlation ID has already been used
			if proxy.isCorrelationIDUsed(correlationID) {
				log.Printf("Dropping message with used correlation ID: %s", correlationID)
				return // Don't forward to client
			}

			// Check if there's a waiting collector for this correlation ID
			if collector, exists := proxy.getExistingCollector(correlationID); exists && !collector.Complete {
				// Add to responses
				collector.Responses = append(collector.Responses, string(message))
				collector.Count++

				// Check if we've received all expected responses
				if collector.Expected > 0 && collector.Count >= collector.Expected {
					collector.Complete = true
					close(collector.Ch)
					proxy.cleanupResponseChannel(correlationID)
				} else if collector.Expected == 0 {
					// If expected count is 0, treat as single response
					collector.Complete = true
					close(collector.Ch)
					proxy.cleanupResponseChannel(correlationID)
				}
				return // Don't forward to client
			}
		}
	}

	// If message processor is enabled, forward through it
	if pc.processorCfg.Enabled {
		processed, blocked, err := pc.forwardToProcessor(message, "server")
		if err != nil {
			log.Printf("Error processing message: %v, falling back to direct forwarding", err)
			// Fall back to direct forwarding on error
			_, _ = clientConn.Write(message)
			return
		} else if blocked {
			log.Printf("Message blocked by processor")
			return
		} else {
			// Only update message if we got a new one
			if len(processed) > 0 {
				message = processed
			}
		}
	}

	// Forward the (possibly processed) message to the client
	_, _ = clientConn.Write(message)
}

// forwardServerToClient forwards messages from the server to the client
func (pc *ProxyConnection) forwardServerToClient(clientConn net.Conn, clientAddr string, ctx context.Context, proxy *SocketProxy) {
	// defer clientConn.Close()

	// Buffer for reading from the server
	buffer := make([]byte, 4096)
	// Buffer for accumulating partial messages
	var msgBuffer []byte

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if pc.serverConn == nil {
				return
			}

			// Set read deadline to allow for periodic context checks
			if err := pc.serverConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
				if !isClosedConnError(err) {
					log.Printf("Error setting read deadline for client %s: %v", clientAddr, err)
				}
				return
			}

			n, err := pc.serverConn.Read(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// This is a timeout, just continue to the next iteration
					continue
				}

				if err != io.EOF && !isClosedConnError(err) {
					log.Printf("Error reading from server for client %s: %v", clientAddr, err)
				}

				// Write any remaining buffered data before exiting
				if len(msgBuffer) > 0 {
					pc.writeToClient(clientConn, clientAddr, msgBuffer, proxy)
				}
				return
			}

			// Add new data to message buffer
			msgBuffer = append(msgBuffer, buffer[:n]...)

			// Process complete messages (ending with newline)
			for {
				// Find the first newline in the buffer
				newlinePos := bytes.IndexByte(msgBuffer, '\n')
				if newlinePos == -1 {
					// No complete message yet, keep buffering
					break
				}

				// Extract the complete message (including the newline)
				msgEnd := newlinePos + 1
				msg := make([]byte, msgEnd)
				copy(msg, msgBuffer[:msgEnd])

				// Process the complete message
				pc.processServerMessage(clientConn, msg, proxy)

				// Remove processed message from buffer
				msgBuffer = msgBuffer[msgEnd:]
			}

			// If buffer is getting too large, process it as is to prevent memory issues
			if len(msgBuffer) > 1024*1024 { // 1MB max buffer size
				pc.processServerMessage(clientConn, msgBuffer, proxy)
				msgBuffer = nil
			}
		}
	}
}

// generateCorrelationID creates a unique ID for correlating requests and responses
func generateCorrelationID() string {
	timestamp := time.Now().UnixNano()
	randBytes := make([]byte, 4)
	if _, err := rand.Read(randBytes); err != nil {
		// Fallback to using timestamp only if we can't generate random bytes
		return fmt.Sprintf("%x", timestamp)
	}
	return fmt.Sprintf("%x-%x", timestamp, randBytes)
}

// isClosedConnError reports whether err is an error from use of a closed network connection.
func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}

	str := err.Error()
	if str == "use of closed network connection" ||
		str == "read: connection reset by peer" ||
		str == "write: broken pipe" ||
		str == "write: connection reset by peer" {
		return true
	}

	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Err != nil {
			if opErr.Err.Error() == "use of closed network connection" {
				return true
			}
			if se, ok := opErr.Err.(*os.SyscallError); ok {
				if se.Err == syscall.ECONNRESET || se.Err == syscall.EPIPE {
					return true
				}
			}
		}
	}

	return false
}

// shouldDisconnect checks if the connection should be disconnected due to expired timers
func (pc *ProxyConnection) shouldDisconnect() bool {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	now := time.Now()
	var remainingExpiries []time.Time
	hasActiveTimer := false

	for _, expiry := range pc.expiries {
		if expiry.After(now) {
			hasActiveTimer = true
			remainingExpiries = append(remainingExpiries, expiry)
		}
	}

	pc.expiries = remainingExpiries
	return !hasActiveTimer
}

// ensureNewline ensures the byte slice ends with a newline
func ensureNewline(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] != '\n' {
		return append(data, '\n')
	}
	return data
}

// MessageWrapper is used to wrap messages with metadata
// when sending to the processor
type MessageWrapper struct {
	Source       string          `json:"source"`        // "client" or "server"
	ClientAddr   string          `json:"client_addr"`   // Client's remote address
	ServerAddr   string          `json:"server_addr"`   // Server's address (if connected)
	ConnectionID string          `json:"connection_id"` // Unique ID for this connection
	Timestamp    string          `json:"timestamp"`     // When the message was sent
	Message      json.RawMessage `json:"message"`       // The original message
}

// ProcessorResponse represents the response from the message processor
type ProcessorResponse struct {
	Block   bool            `json:"block"`   // If true, the message should be blocked
	Message json.RawMessage `json:"message"` // The processed message (if not blocked)
}

// forwardToProcessor sends a message to the external processor and returns the response.
// If the processor returns with block=true, the message will be blocked and not forwarded.
// The response is guaranteed to end with a newline if not blocked.
// source indicates where the message originated from ("client" or "server")
// Returns:
//   - processed message (if not blocked)
//   - error if processing failed
//   - bool indicating if the message should be blocked
func (pc *ProxyConnection) forwardToProcessor(message []byte, source string) ([]byte, bool, error) {
	// Get the current timestamp in ISO 8601 format
	timestamp := time.Now().UTC().Format(time.RFC3339)

	// Get the client and server addresses
	clientAddr := ""
	serverAddr := ""

	// If we have a direct client connection, use its address
	if pc.clientConn != nil {
		clientAddr = pc.clientConn.RemoteAddr().String()
	} else {
		// Otherwise use the client port if available
		if pc.clientPort > 0 {
			clientAddr = fmt.Sprintf("unknown_client:%d", pc.clientPort)
		} else {
			clientAddr = "unknown_client"
		}
	}

	// Add server address if connected
	if pc.serverConn != nil {
		serverAddr = pc.serverConn.RemoteAddr().String()
	} else {
		serverAddr = fmt.Sprintf("%s:%d", pc.serverConfig.Host, pc.serverConfig.Port)
	}
	pc.wsMutex.Lock()
	defer pc.wsMutex.Unlock()

	var err error
	maxRetries := 0
	var response []byte

	for i := 0; i <= maxRetries; i++ {
		// If no connection exists, create one
		if pc.wsConn == nil {
			log.Printf("DEBUG: Creating new WebSocket connection to processor")
			dialer := websocket.Dialer{
				HandshakeTimeout: 5 * time.Second,
			}
			conn, _, err := dialer.Dial(pc.processorCfg.URL, nil)
			if err != nil {
				log.Printf("ERROR: Failed to connect to processor (attempt %d/%d): %v", i+1, maxRetries+1, err)
				if i == maxRetries {
					return nil, true, fmt.Errorf("failed to connect to processor after %d attempts: %w", maxRetries+1, err)
				}
				time.Sleep(time.Second * time.Duration(1))
				continue
			}
			pc.wsConn = conn

			// Set up ping/pong handlers
			pc.wsConn.SetPingHandler(func(appData string) error {
				err := pc.wsConn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
				if err == websocket.ErrCloseSent {
					return nil
				} else if e, ok := err.(net.Error); ok && e.Timeout() {
					return nil
				}
				return err
			})

			// Set read deadline
			pc.wsConn.SetReadDeadline(time.Now().Add(60 * time.Second))
			pc.wsConn.SetPongHandler(func(string) error {
				pc.wsConn.SetReadDeadline(time.Now().Add(60 * time.Second))
				return nil
			})

			log.Println("DEBUG: Successfully connected to processor")
		}

		// Wrap the message with source and connection information
		msgWrapper := MessageWrapper{
			Source:       source,
			ClientAddr:   clientAddr,
			ServerAddr:   serverAddr,
			ConnectionID: pc.connectionID,
			Timestamp:    timestamp,
			Message:      json.RawMessage(message),
		}

		// Convert to JSON
		var messageData []byte
		messageData, err = json.Marshal(msgWrapper)
		if err != nil {
			log.Printf("ERROR: Failed to marshal message wrapper: %v", err)
			return nil, true, fmt.Errorf("failed to marshal message wrapper: %w", err)
		}

		// Send the message
		// log.Printf("DEBUG: Sending message to processor: %s", string(messageData))
		err = pc.wsConn.WriteMessage(websocket.TextMessage, messageData)
		if err != nil {
			log.Printf("ERROR: Failed to send message (attempt %d/%d): %v", i+1, maxRetries+1, err)
			pc.wsConn.Close()
			pc.wsConn = nil
			if i == maxRetries {
				return nil, true, fmt.Errorf("failed to send message after %d attempts: %w", maxRetries+1, err)
			}
			time.Sleep(time.Second * time.Duration(1))
			continue
		}

		// Set a read deadline
		if err = pc.wsConn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			log.Printf("ERROR: Failed to set read deadline: %v", err)
			pc.wsConn.Close()
			pc.wsConn = nil
			return nil, false, fmt.Errorf("failed to set read deadline: %w", err)
		}

		// Read response
		_, response, err = pc.wsConn.ReadMessage()
		if err != nil {
			log.Printf("ERROR: Failed to read response (attempt %d/%d): %v", i+1, maxRetries+1, err)
			pc.wsConn.Close()
			pc.wsConn = nil
			if i == maxRetries {
				return nil, false, fmt.Errorf("failed to read response after %d attempts: %w", maxRetries+1, err)
			}
			time.Sleep(time.Second * time.Duration(1))
			continue
		}

		// log.Printf("DEBUG: Successfully received response from processor: %s", string(response))

		// Parse the processor response
		var procResp ProcessorResponse
		if err := json.Unmarshal(response, &procResp); err != nil {
			log.Printf("ERROR: Failed to parse processor response: %v", err)
			return nil, false, fmt.Errorf("failed to parse processor response: %w", err)
		}

		// If message should be blocked, return empty response with block=true
		if procResp.Block {
			log.Printf("DEBUG: Message blocked by processor")
			return nil, true, nil
		}

		// If no message returned, use the original message
		if len(procResp.Message) == 0 {
			return ensureNewline(message), false, nil
		}

		// Return the processed message with newline
		return ensureNewline(procResp.Message), false, nil
	}

	return nil, false, fmt.Errorf("unexpected error in forwardToProcessor")
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

func main() {
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	proxy := NewSocketProxy(config)

	// Set up the API endpoints
	http.HandleFunc("/api/reconnect", proxy.handleReconnect)
	http.HandleFunc("/api/send-to-client", proxy.handleSendToClient)
	http.HandleFunc("/api/send-to-server", proxy.handleSendToServer)
	http.HandleFunc("/api/send-and-wait-response", proxy.handleSendAndWaitResponse)
	apiAddr := fmt.Sprintf(":%d", config.APIPort)
	go func() {
		log.Printf("Starting API server on %s", apiAddr)
		if err := http.ListenAndServe(apiAddr, nil); err != nil {
			log.Fatalf("Failed to start API server: %v", err)
		}
	}()

	proxy.Start()

	// Block forever
	select {}
}
