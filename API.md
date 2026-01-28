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

**Response:**

```
200 OK
Reconnected successfully
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

**Response:**

```
200 OK
Message sent to client
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

**Response:**

```
200 OK
Message sent to server
```

---

### 4. Send and Wait for Response

Send a message to the server and wait for one or more responses.

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
  "correlationId": "string",
  "responses": ["string"],
  "complete": true,
  "count": 1,
  "expectedCount": 1,
  "timeout": false
}
```

**Fields:**
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
  "client_addr": "127.0.0.1:54321",
  "server_addr": "localhost:4040",
  "connection_id": "127.0.0.1_54321_4040",
  "correlation_id": "",
  "is_api": false,
  "timestamp": "2023-10-27T10:00:00Z",
  "message": { ... }
}
```

- `source`: Origin of the message ("CLIENT", "SERVER", "API-CLIENT", "API-SERVER")
- `message`: The actual message content (JSON)

---

## Error Responses

### 400 Bad Request

```json
{
  "error": "string"
}
```

### 404 Not Found

```json
{
  "error": "Connection not found"
}
```

### 405 Method Not Allowed

```
Only POST method is accepted
```

### 500 Internal Server Error

```json
{
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
