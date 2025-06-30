package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ProxyConnection manages the connection between a client and a server.
type ProxyConnection struct {
	connectionID   string
	clientConn     net.Conn
	clientAddr     net.Addr
	clientPort     int
	serverConfig   ServerConfig
	serverConn     net.Conn
	mutex          sync.Mutex
	cancelForward  context.CancelFunc
	expiresAt      time.Time // When the connection will expire (zero time means no expiration)
	wsConn         *websocket.Conn
	wsMutex        sync.Mutex
	wsWriteMutex   sync.Mutex // Protects writes to the WebSocket connection
	config         *Config    // Reference to the main configuration
	wsResponseChan chan []byte
	wsErrorChan    chan error
	wsReaderCancel context.CancelFunc
}

func (pc *ProxyConnection) connectToServer(p *SocketProxy) bool {
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

	ctx, cancel := context.WithCancel(context.Background())
	pc.cancelForward = cancel
	go pc.forwardServerToClient(pc.clientConn, pc.clientAddr.String(), ctx, p)

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

	// Cancel the reader goroutine context
	if pc.wsReaderCancel != nil {
		pc.wsReaderCancel()
		pc.wsReaderCancel = nil
	}

	// Close the websocket connection, which will unblock the reader
	if pc.wsConn != nil {
		log.Println("Disconnecting from message processor")
		pc.wsConn.Close()
		pc.wsConn = nil
	}

	// Close the response channel to unblock any waiting receivers
	if pc.wsResponseChan != nil {
		// This needs to be done carefully to avoid panics.
		// In this design, the disconnect function owns the closing of the response channel.
		close(pc.wsResponseChan)
		pc.wsResponseChan = nil
	}

	// The reader goroutine is responsible for closing the error channel.
	// We just nil it out here to prevent any lingering writes from a misbehaving goroutine.
	if pc.wsErrorChan != nil {
		pc.wsErrorChan = nil
	}
}

// wsReader is a dedicated goroutine for reading messages from the WebSocket connection.
// It handles control frames automatically and forwards data messages and errors
// to the provided channels.
func (pc *ProxyConnection) wsReader(ctx context.Context) {
	// Goroutine will exit when the function returns.
	// The caller is responsible for ensuring the connection is closed, which will
	// cause ReadMessage to return an error, thus terminating the loop.
	defer func() {
		// Ensure error channel is closed on exit to signal completion/error.
		if pc.wsErrorChan != nil {
			close(pc.wsErrorChan)
		}
	}()

	for {
		// ReadMessage is a blocking call. It will also handle control frames
		// (like pings) by invoking the handlers we've set up.
		_, message, err := pc.wsConn.ReadMessage()
		if err != nil {
			// An error occurred. This could be a normal closure or an
			// unexpected error. Send it to the error channel if the channel is not nil.
			// We must check for ctx.Done to avoid blocking forever if the receiver has gone away.
			select {
			case pc.wsErrorChan <- err:
			case <-ctx.Done():
			}
			return // Stop reading on any error.
		}

		// We received a data message. Send it to the response channel.
		// We must check for ctx.Done to avoid blocking forever if the receiver has gone away.
		select {
		case pc.wsResponseChan <- message:
			// Message sent successfully.
		case <-ctx.Done():
			// Context was canceled while trying to send.
			return
		}
	}
}

// safeWriteMessage writes a message to the WebSocket connection in a thread-safe manner.
func (pc *ProxyConnection) safeWriteMessage(messageType int, data []byte) error {
	pc.wsWriteMutex.Lock()
	defer pc.wsWriteMutex.Unlock()
	return pc.wsConn.WriteMessage(messageType, data)
}

func (pc *ProxyConnection) isConnectedToServer() bool {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	return pc.serverConn != nil
}

// addExpiry updates the connection's expiration time if the new duration is longer than the current one.
func (pc *ProxyConnection) addExpiry(duration time.Duration) {
	if duration <= 0 {
		return
	}
	
	expiryTime := time.Now().Add(duration)
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	
	// Only update if the new expiry is in the future and later than the current expiry
	if expiryTime.After(time.Now()) && (pc.expiresAt.IsZero() || expiryTime.After(pc.expiresAt)) {
		pc.expiresAt = expiryTime
	}
}

// GetExpiresAt returns the time when this connection will expire
func (pc *ProxyConnection) GetExpiresAt() time.Time {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	return pc.expiresAt
}

// writeToClient writes data to the client connection with error handling and logging
func (pc *ProxyConnection) writeToClient(conn net.Conn, clientAddr string, data []byte, proxy *SocketProxy) error {
	if len(data) == 0 {
		return nil
	}

	// Log the message being sent to the client

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
	proxy.logMessage("SERVER", clientConn.RemoteAddr().String(), pc.clientPort, message)

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
	if pc.config != nil && pc.config.MessageProcessor.Enabled {
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
					proxy.logMessage("SERVER", clientAddr, pc.clientPort, msgBuffer)
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

// shouldDisconnect checks if the connection should be disconnected due to expired timers
func (pc *ProxyConnection) shouldDisconnect() bool {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	// If timeouts are disabled in the config, never disconnect due to timeout
	if pc.config != nil && pc.config.DisableTimeout {
		return false
	}

	// If expiresAt is zero, there's no expiration set
	if pc.expiresAt.IsZero() {
		return false
	}

	// Check if the expiration time has passed
	return time.Now().After(pc.expiresAt)
}

// forwardToProcessor sends a message to the external processor and returns the response.
// If the processor returns with block=true, the message will be blocked and not forwarded.
// The response is guaranteed to end with a newline if not blocked.
// source indicates where the message originated from ("client" or "server")
// Returns:
//   - error if processing failed
//   - bool indicating if the message should be blocked
func (pc *ProxyConnection) forwardToProcessor(message []byte, source string) ([]byte, bool, error) {
	timestamp := time.Now().Format(time.RFC3339)
	var clientAddr, serverAddr string
	if pc.clientConn != nil {
		clientAddr = pc.clientConn.RemoteAddr().String()
	} else {
		if pc.clientPort > 0 {
			clientAddr = fmt.Sprintf("unknown_client:%d", pc.clientPort)
		} else {
			clientAddr = "unknown_client"
		}
	}
	if pc.serverConn != nil {
		serverAddr = pc.serverConn.RemoteAddr().String()
	} else {
		serverAddr = fmt.Sprintf("%s:%d", pc.serverConfig.Host, pc.serverConfig.Port)
	}

	var err error
	maxRetries := 0
	var response []byte
	var messageData []byte

	for i := 0; i <= maxRetries; i++ {
		var responseChan chan []byte
		var errorChan chan error
		var conn *websocket.Conn
		var setupErr error

		// Critical section to check/create connection under a lock
		func() {
			pc.wsMutex.Lock()
			defer pc.wsMutex.Unlock()

			if pc.wsConn == nil {
				log.Printf("DEBUG: Creating new WebSocket connection to processor")
				dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
				var dialErr error
				if pc.config == nil || pc.config.MessageProcessor.URL == "" {
					setupErr = fmt.Errorf("no processor URL configured")
					return
				}
				conn, _, dialErr = dialer.Dial(pc.config.MessageProcessor.URL, nil)
				if dialErr != nil {
					log.Printf("ERROR: Failed to connect to processor (attempt %d/%d): %v", i+1, maxRetries+1, dialErr)
					setupErr = dialErr // Propagate error
					return
				}
				pc.wsConn = conn

				// Setup handlers
				pc.wsConn.SetPingHandler(func(appData string) error {
					pc.wsMutex.Lock()
					defer pc.wsMutex.Unlock()
					if pc.wsConn == nil {
						return nil
					}
					// Use the safe write method for pong responses
					return pc.safeWriteMessage(websocket.PongMessage, []byte(appData))
				})
				// pc.wsConn.SetReadDeadline(time.Now().Add(60 * time.Second))
				pc.wsConn.SetPongHandler(func(string) error {
					pc.wsMutex.Lock()
					defer pc.wsMutex.Unlock()
					if pc.wsConn == nil {
						return nil
					}
					pc.wsConn.SetReadDeadline(time.Now().Add(60 * time.Second))
					return nil
				})

				// Create channels and start reader
				pc.wsResponseChan = make(chan []byte)
				pc.wsErrorChan = make(chan error, 1)
				var readerCtx context.Context
				readerCtx, pc.wsReaderCancel = context.WithCancel(context.Background())
				go pc.wsReader(readerCtx)
				log.Println("DEBUG: Successfully connected to processor and started reader goroutine")
			}

			// Get connection and channels to use outside the lock
			conn = pc.wsConn
			responseChan = pc.wsResponseChan
			errorChan = pc.wsErrorChan
		}()

		if setupErr != nil {
			if i == maxRetries {
				return nil, true, fmt.Errorf("failed to connect to processor after %d attempts: %w", maxRetries+1, setupErr)
			}
			time.Sleep(time.Second * time.Duration(1))
			continue
		}

		// --- Unlocked section for I/O ---

		msgWrapper := MessageWrapper{
			Source: source, ClientAddr: clientAddr, ServerAddr: serverAddr,
			ConnectionID: pc.connectionID, Timestamp: timestamp, Message: json.RawMessage(message),
		}
		messageData, err = json.Marshal(msgWrapper)
		if err != nil {
			log.Printf("ERROR: Failed to marshal message wrapper: %v", err)
			return nil, true, fmt.Errorf("failed to marshal message wrapper: %w", err)
		}

		if err = pc.safeWriteMessage(websocket.TextMessage, messageData); err != nil {
			log.Printf("ERROR: Failed to send message (attempt %d/%d): %v", i+1, maxRetries+1, err)
			pc.disconnectFromMessageProcessor()
			if i == maxRetries {
				return nil, true, fmt.Errorf("failed to send message after %d attempts: %w", maxRetries+1, err)
			}
			time.Sleep(time.Second * time.Duration(1))
			continue
		}

		select {
		case response = <-responseChan:
			// Got a response, proceed
		case err = <-errorChan:
			log.Printf("ERROR: WebSocket read error (attempt %d/%d): %v", i+1, maxRetries+1, err)
			pc.disconnectFromMessageProcessor()
			if i == maxRetries {
				return nil, true, fmt.Errorf("failed to read message after %d attempts: %w", maxRetries+1, err)
			}
			time.Sleep(time.Second * time.Duration(1))
			continue
		case <-time.After(10 * time.Second):
			log.Printf("ERROR: Timeout waiting for response from processor (attempt %d/%d)", i+1, maxRetries+1)
			pc.disconnectFromMessageProcessor()
			if i == maxRetries {
				return nil, true, fmt.Errorf("timed out waiting for response after %d attempts", maxRetries+1)
			}
			time.Sleep(time.Second * time.Duration(1))
			continue
		}

		break // Success, exit retry loop
	}

	var procResp ProcessorResponse
	if err = json.Unmarshal(response, &procResp); err != nil {
		log.Printf("ERROR: Failed to parse processor response: %v", err)
		return nil, false, fmt.Errorf("failed to parse processor response: %w", err)
	}
	if procResp.Block {
		log.Printf("DEBUG: Message blocked by processor")
		return nil, true, nil
	}
	if len(procResp.Message) == 0 {
		return message, false, nil
	}
	return ensureNewline(procResp.Message), false, nil
}
