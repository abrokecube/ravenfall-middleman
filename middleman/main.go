package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if len(config.MessageProcessor.URLs) > 0 {
		log.Printf("Initializing message processor connections to %v", config.MessageProcessor.URLs)
		processorConnector.Init(config.MessageProcessor.URLs)
	} else {
		log.Println("WARN: Message processor URLs not configured. Processor features will be disabled.")
	}

	proxy := NewSocketProxy(config)

	// Set up the API endpoints
	http.HandleFunc("/api/reconnect", proxy.handleReconnect)
	http.HandleFunc("/api/send-to-client", proxy.handleSendToClient)
	http.HandleFunc("/api/send-to-server", proxy.handleSendToServer)
	http.HandleFunc("/api/send-and-wait-response", proxy.handleSendAndWaitResponse)
	http.HandleFunc("/api/ensure-connected", proxy.handleEnsureConnected)
	http.HandleFunc("/api/connection-status", proxy.handleConnectionStatus)
	http.HandleFunc("/api/config", proxy.handleGetConfig)
	// Add WebSocket endpoint for message streaming
	http.HandleFunc("/ws", handleWebSocket)

	apiAddr := fmt.Sprintf(":%d", config.APIPort)
	go func() {
		log.Printf("Starting API server on %s", apiAddr)
		log.Printf("WebSocket endpoint available at ws://localhost%s/ws", apiAddr)
		if err := http.ListenAndServe(apiAddr, nil); err != nil {
			log.Fatalf("Failed to start API server: %v", err)
		}
	}()

	proxy.Start()

	// Block forever
	select {}
}
