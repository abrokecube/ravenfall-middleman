package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// ClientMessage defines the structure of the JSON message from the client.
type ClientMessage struct {
	Identifier string `json:"Identifier"`
}

// ServerConfig holds the configuration for a target server.
type ServerConfig struct {
	Host string
	Port int
}

// Config holds the full configuration from the JSON file.
type Config struct {
	EnableMessageLogging       bool           `json:"enableMessageLogging"`
	DefaultTimeoutSeconds      int            `json:"defaultTimeoutSeconds"`
	NoIdentifierTimeoutSeconds int            `json:"noIdentifierTimeoutSeconds"`
	IdentifierTimeouts         map[string]int `json:"identifier_timeouts"`
	ProxyMappings              []struct {
		ClientPort int    `json:"clientPort"`
		ServerHost string `json:"serverHost"`
		ServerPort int    `json:"serverPort"`
	} `json:"proxy_mappings"`
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
	clientPort    int
	serverConfig  ServerConfig
	serverConn    net.Conn
	mutex         sync.Mutex
	cancelForward context.CancelFunc
	expiries      []time.Time
}

// SocketProxy manages all proxy connections.
type SocketProxy struct {
	connections          map[string]*ProxyConnection
	listeners            map[int]net.Listener
	connMutex            sync.Mutex
	mappings             map[int]ServerConfig
	defaultIdleTimeout   time.Duration
	noIdentifierTimeout  time.Duration
	identifierTimeouts   map[string]time.Duration
	enableMessageLogging bool
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

	return &SocketProxy{
		connections:          make(map[string]*ProxyConnection),
		listeners:            make(map[int]net.Listener),
		mappings:             mappings,
		defaultIdleTimeout:   time.Duration(config.DefaultTimeoutSeconds) * time.Second,
		noIdentifierTimeout:  noIdentifierTimeout,
		identifierTimeouts:   identifierTimeouts,
		enableMessageLogging: config.EnableMessageLogging,
	}
}

// Start initializes and starts all proxy servers.
func (p *SocketProxy) Start() {
	log.Println("Starting socket proxy...")

	for clientPort, serverConfig := range p.mappings {
		go func(port int, config ServerConfig) {
			listenAddr := fmt.Sprintf("localhost:%d", port)
			listener, err := net.Listen("tcp", listenAddr)
			if err != nil {
				log.Printf("Failed to start server on port %d: %v", port, err)
				return
			}
			p.listeners[port] = listener
			log.Printf("Proxy listening on port %d -> %s:%d", port, config.Host, config.Port)

			for {
				clientConn, err := listener.Accept()
				if err != nil {
					log.Printf("Failed to accept client connection on port %d: %v", port, err)
					// Check if the listener was closed
					if _, ok := p.listeners[port]; !ok {
						break
					}
					continue
				}
				go p.handleClient(clientConn, port, config)
			}
		}(clientPort, serverConfig)
	}

	log.Println("Socket proxy started successfully")
	go p.cleanupIdleConnections()
}

func (p *SocketProxy) handleClient(clientConn net.Conn, clientPort int, serverConfig ServerConfig) {
	clientAddr := clientConn.RemoteAddr().String()
	log.Printf("Client %s connected to port %d", clientAddr, clientPort)

	proxyConn := &ProxyConnection{
		clientPort:   clientPort,
		serverConfig: serverConfig,
		expiries:     []time.Time{},
	}

	connectionID := fmt.Sprintf("%s_%d", clientAddr, clientPort)
	p.connMutex.Lock()
	p.connections[connectionID] = proxyConn
	p.connMutex.Unlock()

	defer func() {
		proxyConn.disconnectFromServer()
		p.connMutex.Lock()
		delete(p.connections, connectionID)
		p.connMutex.Unlock()
		clientConn.Close()
		log.Printf("Client %s connection closed", clientAddr)
	}()

	buf := make([]byte, 4096)
	for {
		clientConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, err := clientConn.Read(buf)

		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			continue
		}

		if err != nil {
			if err != io.EOF {
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
					log.Printf("Identifier '%s' found, starting a timer for %v", msg.Identifier, timeout)
					proxyConn.addExpiry(timeout)
					parsedWithIdentifier = true
				} else {
					// Message has an identifier but it's not in our timeout map, use default timeout
					proxyConn.addExpiry(p.defaultIdleTimeout)
					parsedWithIdentifier = true
				}
			}
		}

		if !parsedWithIdentifier {
			// Message has no Identifier field, use the no-identifier timeout
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

		if _, err := proxyConn.serverConn.Write(buf[:n]); err != nil {
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
		// Set Linger to 0 to send an RST on close
		if tcpConn, ok := pc.serverConn.(*net.TCPConn); ok {
			tcpConn.SetLinger(0)
		}
		pc.serverConn.Close()
		pc.serverConn = nil
	}
	if pc.cancelForward != nil {
		pc.cancelForward()
		pc.cancelForward = nil
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
	// log.Printf("Added expiry timer: %v from now (expires at: %v) for connection to %s:%d",
	// 	duration.Round(time.Millisecond),
	// 	expiry.Format("15:04:05.000"),
	// 	pc.serverConfig.Host,
	// 	pc.serverConfig.Port)
}

// shouldDisconnect checks if the connection should be disconnected based on its timers.
// It prunes expired timers and returns true if no active timers are left.
func (pc *ProxyConnection) shouldDisconnect() bool {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	now := time.Now()
	activeExpiries := []time.Time{}
	for _, expiry := range pc.expiries {
		if expiry.After(now) {
			activeExpiries = append(activeExpiries, expiry)
		}
	}

	pc.expiries = activeExpiries
	return len(pc.expiries) == 0
}

func (pc *ProxyConnection) forwardServerToClient(clientConn net.Conn, clientAddr string, ctx context.Context, proxy *SocketProxy) {
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Server-to-client forwarding cancelled for %s", clientAddr)
			return
		default:
			if !pc.isConnectedToServer() {
				return
			}

			// Set a read deadline to prevent blocking forever
			if err := pc.serverConn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
				log.Printf("Error setting read deadline: %v", err)
				return
			}

			n, err := pc.serverConn.Read(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Check if context is done before continuing
					select {
					case <-ctx.Done():
						return
					default:
						continue
					}
				}

				// Don't log errors if the connection was closed intentionally
				if err != io.EOF && !isClosedConnError(err) {
					log.Printf("Error reading from server for client %s: %v", clientAddr, err)
				}
				return
			}

			// Log the received message from server to client
			proxy.logMessage("SERVER -> CLIENT", clientAddr, pc.clientPort, buf[:n])

			if _, err := clientConn.Write(buf[:n]); err != nil {
				if !isClosedConnError(err) {
					log.Printf("Error writing to client %s: %v", clientAddr, err)
				}
				return
			}
		}
	}
}

// isClosedConnError reports whether err is an error from use of a closed network connection.
func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific error messages that indicate a closed connection
	str := err.Error()
	if str == "use of closed network connection" ||
		str == "read: connection reset by peer" ||
		str == "write: broken pipe" ||
		str == "write: connection reset by peer" {
		return true
	}

	// Check for net.OpError with specific error conditions
	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Err != nil {
			if opErr.Err.Error() == "use of closed network connection" {
				return true
			}
			// Check for other common closed connection errors
			if se, ok := opErr.Err.(*os.SyscallError); ok {
				if se.Err == syscall.ECONNRESET || se.Err == syscall.EPIPE {
					return true
				}
			}
		}
	}

	return false
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
	for port, listener := range p.listeners {
		listener.Close()
		delete(p.listeners, port)
	}
	log.Println("Socket proxy stopped")
}

func main() {
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	proxy := NewSocketProxy(config)
	proxy.Start()

	// Block forever
	select {}
}
