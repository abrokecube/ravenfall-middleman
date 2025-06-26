package main

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"
)

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

// ensureNewline ensures the byte slice ends with a newline
func ensureNewline(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] != '\n' {
		return append(data, '\n')
	}
	return data
}
