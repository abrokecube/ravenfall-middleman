this project contains 99% ai generated code. written in go because ai couldn't give me working python logic to correctly handle disconnecting from the server

# Ravenfall Middleman

Ravenfall Middleman is a proxy server designed to intercept, monitor, and modify traffic between Ravenfall and RavenBot.

## Features

- **TCP Socket Proxying**: Seamlessly proxies traffic between a client (RavenBot) and a target server (Ravenfall).
- **Message Interception**: Inspects messages in real-time.
- **Message Modification**: Optionally forwards messages to an external "Message Processor" service for modification or blocking before they reach their destination.
- **REST API**: Provides an API to control connections, send messages programmatically, and query status.
- **WebSocket Stream**: Exposes a real-time stream of all traffic for monitoring tools.
- **Configurable**: Configurable via a JSON file.
- **Auto Disconnect**: Automatically disconnects from the server after a period of inactivity to optimize resource usage.

## Components

The project consists of three main components:

1.  **Middleman (Go)**: The core proxy server.
2.  **Message Processor (Python)**: An optional service to process and modify messages.
3.  **Listener (Python)**: A utility script to monitor traffic via the Middleman's WebSocket stream.

## Prerequisites

- **Go**: Required to build the Middleman.
- **Python 3.12+**: Required for the Processor and Listener.

## Installation & Usage

### 1. Middleman (Core)

Navigate to the `middleman` directory:

```bash
cd middleman
```

Build the project:

```bash
go build -o middleman.exe
```

Run the executable:

```bash
./middleman.exe
```

The middleman will start based on the configuration in `config.json`. By default, it listens on port `8041` and proxies to `localhost:4041`. The API is available at `http://localhost:8080`.

### 2. Message Processor (Optional)

If you want to modify or block messages programmatically, use the Message Processor.

Navigate to the `processor` directory:

```bash
cd processor
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the processor:

```bash
python message_processor.py
```

Ensure `config.json` in the `middleman` directory has `messageProcessor.enabled` set to `true`.

### 3. Listener (Optional)

To watch traffic in real-time from your terminal:

Navigate to the `listener` directory:

```bash
cd listener
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the listener:

```bash
python websocket_client.py
```

## Configuration

The Middleman is configured via `middleman/config.json`:

```json
{
    "enableMessageLogging": true,
    "disableTimeout": false,
    "defaultTimeoutSeconds": 15,
    "noIdentifierTimeoutSeconds": 5,
    "apiPort": 8080,
    "identifier_timeouts": {
        "island_info": 30
    },
    "proxy_mappings": [
        {
            "clientPort": 8041,
            "serverHost": "localhost",
            "serverPort": 4041
        }
    ],
    "messageProcessor": {
        "enabled": false,
        "url": "ws://localhost:8000/process"
    }
}
```

- **proxy_mappings**: Defines the ports the middleman listens on (`clientPort`) and where it forwards traffic (`serverHost`, `serverPort`).
- **messageProcessor**: Configures the connection to the external Message Processor service.
- **apiPort**: The port for the REST API.

## Connection Management & Timeouts

The Middleman includes an automatic disconnection feature to optimize resource usage. Ravenfall can consume significant CPU resources when a client is connected, even if idle. To mitigate this, the Middleman disconnects from the server after a period of inactivity.

This behavior is controlled by the following configuration options:

- **disableTimeout**: Set to `true` to completely disable the timeout feature (keep connections open indefinitely).
- **defaultTimeoutSeconds**: The default time (in seconds) to keep the connection open after receiving a message with a known identifier.
- **noIdentifierTimeoutSeconds**: The time (in seconds) to keep the connection open after receiving a message *without* an identifier.
- **identifier_timeouts**: A map of specific timeouts for specific message identifiers. For example, `"island_info": 30` keeps the connection open for 30 seconds after receiving an "island_info" message.

When a message is received, the connection's expiry time is extended based on these rules. If the timer expires, the Middleman closes the connection to the server (but keeps the client connection open, ready to reconnect when the client sends a new message).

## API Documentation

The Middleman exposes a REST API for controlling connections and sending messages.

See [API.md](API.md) for full documentation.
