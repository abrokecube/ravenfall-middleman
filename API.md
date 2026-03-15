# Ravenfall Middleman API Documentation

This document describes the REST API provided by the Ravenfall Middleman service for managing WebSocket connections and message routing.

## Connection IDs

Connection IDs are automatically generated when a client connects to the middleman. They follow this format:

```
<client-ip>_<client-port>_<server-port>
```

For example: `192.168.1.100_8041_4041`

Where:
- `client-ip` - The IP address of the client that connected
- `client-port` - The port on the middleman that the client connected to
- `server-port` - The port of the target server that the middleman is proxying to

## Base URL

All API endpoints are relative to the base URL where the middleman service is running (default: `http://localhost:8080`).

## Authentication

The API does not currently require authentication. All endpoints are accessible without credentials.

## Endpoints

### 1. Reconnect a Connection

Force a reconnection for the specified WebSocket connection.

```http
POST /api/reconnect
```

**Request Body:**

```json
{
  "connectionId": "string",
  "timeout": 0
}
```

**Parameters:**
- `connectionId` (string, required): The ID of the connection to reconnect
- `timeout` (int, optional): Time in seconds before the connection times out. If not provided, uses the default timeout.

**Response (200 OK):**

```json
{
  "success": true,
  "message": "Reconnected successfully"
}
```

---

### 2. Send Message to Client

Send a message to a specific connected client.

```http
POST /api/send-to-client
```

**Request Body:**

```json
{
  "connectionId": "string",
  "data": "string"
}
```

**Parameters:**
- `connectionId` (string, required): The ID of the target client connection
- `data` (string, required): The message data to send to the client

**Response (200 OK):**

```json
{
  "success": true,
  "message": "Message sent to client"
}
```

---

### 3. Send Message to Server

Send a message to the server through a specific connection.

```http
POST /api/send-to-server
```

**Request Body:**

```json
{
  "connectionId": "string",
  "data": "string"
}
```

**Parameters:**
- `connectionId` (string, required): The ID of the connection to use
- `data` (string, required): The message data to send to the server

**Response (200 OK):**

```json
{
  "success": true,
  "message": "Message sent to server"
}
```

---

### 4. Send and Wait for Response

Send a message to the server and wait for one or more responses. Blocks matching messages from being sent to the client.

```http
POST /api/send-and-wait-response
```

**Request Body:**

```json
{
  "connectionId": "string",
  "data": "string",
  "timeout": 30,
  "expectedCount": 0
}
```

**Parameters:**
- `connectionId` (string, required): The ID of the connection to use
- `data` (string, required): The message data to send to the server
- `timeout` (int, optional, default=30): Maximum time in seconds to wait for responses
- `expectedCount` (int, optional, default=0): Number of expected responses (0 means wait for a single response)

**Response (200 OK):**

```json
{
  "success": true,
  "correlationId": "string",
  "responses": ["string"],
  "complete": true,
  "count": 1,
  "expectedCount": 1,
  "timeout": false
}
```

**Fields:**
- `success`: Always true.
- `correlationId`: The ID used to correlate the request with responses
- `responses`: Array of received responses
- `complete`: Whether all expected responses were received
- `count`: Number of responses received
- `expectedCount`: Number of responses that were expected
- `timeout`: Present and true if the request timed out

---

### 5. Ensure Connection

Ensures that a connection to the server is active. If the connection is not active, it attempts to reconnect.

```http
POST /api/ensure-connected
```

**Request Body:**

```json
{
  "connectionId": "string",
  "timeout": 0
}
```

**Parameters:**
- `connectionId` (string, required): The ID of the connection to check/reconnect
- `timeout` (int, optional): Time in seconds to extend the connection expiry.

**Response:**

```json
{
  "success": true,
  "message": "Connection is active",
  "reconnected": false,
  "connected": true
}
```

---

### 6. Get Connection Status

Retrieve the status of a specific connection.

```http
GET /api/connection-status?connectionId=<connectionId>
```

**Parameters:**
- `connectionId` (string, required): The ID of the connection to check

**Response:**

```json
{
  "success": true,
  "status": {
    "clientConnected": true,
    "serverConnected": true,
    "timeUntilClose": 60,
    "connectionId": "string"
  }
}
```

---

### 7. Get Configuration

Retrieve the current configuration of the middleman service.

```http
GET /api/config
```

**Response:**

```json
{
  "success": true,
  "config": {
    ...
  }
}
```

---

### 8. WebSocket Message Stream

Real-time stream of all messages passing through the middleman.

```
ws://localhost:8080/ws
```

**Message Format:**

Messages received on this WebSocket are JSON objects with the following structure:

```json
{
  "source": "CLIENT",
  "clientAddr": "127.0.0.1:54321",
  "serverAddr": "localhost:4040",
  "connectionId": "127.0.0.1_54321_4040",
  "correlationId": "",
  "isApi": false,
  "timestamp": "2023-10-27T10:00:00Z",
  "message": { ... }
}
```

- `source`: Origin of the message ("CLIENT", "SERVER", "API-CLIENT", "API-SERVER")
- `message`: The actual message content (JSON)

---

## Message Processor Server Integration

The Middleman service can optionally integrate with an external Message Processor Server via a persistent WebSocket connection. This allows the processor to inspect, intercept, and modify messages in real-time as they flow between the client and the server.

### Configuration
The processor is configured in the `config.json` via the `messageProcessor` object:
```json
"messageProcessor": {
  "enabled": true,
  "url": "ws://localhost:9000/process"
}
```

### Connection Management
- The Middleman maintains a persistent WebSocket connection to the processor URL (`processor_connector.go`).
- If disconnected, it runs a background loop attempting to reconnect with a 5-second backoff.
- The connection uses a 10-second request timeout for the initial handshake.

### Message Flow Lifecycle
1. **Interception**: When a message arrives (from `CLIENT`, `SERVER`, or triggered via the `API`), the Middleman checks if the message processor is enabled in the configuration (`proxyconnection.go`).
2. **Wrapper Generation**: The raw message payload is wrapped inside a JSON `MessageWrapper`.
   - The wrapper includes source metadata (`Source`), client and server network addresses, `ConnectionID`, a uniquely generated `CorrelationID` (using the connection ID and Unix timestamp), an `IsAPI` boolean flag, and a timestamp in `RFC3339` format.
3. **Dispatch**: The serialized `MessageWrapper` is pushed as a text message over the WebSocket to the Message Processor. A wait channel is temporarily allocated for exactly that `CorrelationID` in a lock-free sync map.
4. **Processing & Timeout**: 
   - The middleman will wait up to **5 seconds** for the external processor to respond with the matching `CorrelationID`.
   - If the processor fails to respond within the timeframe, it logs a timeout warning and **falls back to forwarding the unmodified original message** to avoid hanging the connection.
5. **Response Handling**: The external processor must send back a `ProcessorResponse` with the same `CorrelationID`.
   - If `block: true` is included in the response, the middleman immediately drops the message and does not forward it.
   - If `error` parameter is provided, the middleman logs the error and forwards the original unmodified payload.
   - If the `message` object is provided in the response (and not null), the middleman extracts this mutated payload and forwards it down the original connection instead of the original payload.

### Wrapper Schema Examples
When sending a message to the processor:
```json
{
  "source": "CLIENT",
  "clientAddr": "192.168.1.100:54321",
  "serverAddr": "localhost:4040",
  "connectionId": "192.168.1.100_54321_4040",
  "correlationId": "192.168.1.100_54321_4040_1698400800000000000",
  "isApi": false,
  "timestamp": "2023-10-27T10:00:00Z",
  "message": { "Identifier": "Login" }
}
```

When receiving a successful response from the processor:
```json
{
  "correlationId": "192.168.1.100_54321_4040_1698400800000000000",
  "block": false,
  "message": { "Identifier": "Login", "InjectedField": true }
}
```

When receiving an error response from the processor (falls back to forwarding unmodified payload):
```json
{
  "correlationId": "192.168.1.100_54321_4040_1698400800000000000",
  "error": "Failed to authenticate against the user service",
  "block": false,
  "message": null
}
```

---

## Error Responses

### 400 Bad Request

```json
{
  "success": false,
  "error": "string"
}
```

### 404 Not Found

```json
{
  "success": false,
  "error": "Connection not found"
}
```

### 405 Method Not Allowed

```json
{
  "success": false,
  "error": "Only POST method is accepted"
}
```

### 500 Internal Server Error

```json
{
  "success": false,
  "error": "string"
}
```

## Examples

### Reconnecting a Connection

```bash
curl -X POST http://localhost:8080/api/reconnect \
  -H "Content-Type: application/json" \
  -d '{"connectionId": "abc123", "timeout": 60}'
```

### Sending a Message to Client

```bash
curl -X POST http://localhost:8080/api/send-to-client \
  -H "Content-Type: application/json" \
  -d '{"connectionId": "abc123", "data": "Hello, client!"}'
```

### Sending a Message to Server and Waiting for Response

```bash
curl -X POST http://localhost:8080/api/send-and-wait-response \
  -H "Content-Type: application/json" \
  -d '{"connectionId": "abc123", "data": "PING", "timeout": 10}'
```

### Ensuring Connection

```bash
curl -X POST http://localhost:8080/api/ensure-connected \
  -H "Content-Type: application/json" \
  -d '{"connectionId": "abc123", "timeout": 300}'
```

### Checking Connection Status

```bash
curl http://localhost:8080/api/connection-status?connectionId=abc123
```
