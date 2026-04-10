package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Config holds the full configuration from the JSON file.
type Config struct {
	EnableMessageLogging       bool           `json:"enableMessageLogging"`
	DisableTimeout             bool           `json:"disableTimeout"`
	DefaultTimeoutSeconds      int            `json:"defaultTimeoutSeconds"`
	NoIdentifierTimeoutSeconds int            `json:"noIdentifierTimeoutSeconds"`
	IdentifierTimeouts         map[string]int `json:"identifierTimeouts"`
	APIPort                    int            `json:"apiPort"`
	ProxyMappings              []struct {
		ConnectionID string `json:"connectionId"`
		ClientHost   string `json:"clientHost"`
		ClientPort   int    `json:"clientPort"`
		ServerHost   string `json:"serverHost"`
		ServerPort   int    `json:"serverPort"`
	} `json:"proxyMappings"`
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
