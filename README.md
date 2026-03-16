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

## Basic setup
1. Edit RavenBot's `config.json`, found in the same folder as the executable. Change the `Port` number to `4050` (this is what `clientPort` is set to in the middleman's config). Save the file and restart RavenBot if it was running.
2. Start `middleman.exe`. If things are well you should see something like `Client 127.0.0.1:xxxxx connected to port 4050`.
3. Run your ravenfall middleman scripts

## Prerequisites (for development)

- **Go**: Required to build the Middleman.

## Configuration

The Middleman is configured via `config.json`:

```json
{
    "enableMessageLogging": true,
    "disableTimeout": false,
    "defaultTimeoutSeconds": 15,
    "noIdentifierTimeoutSeconds": 5,
    "apiPort": 8080,
    "identifierTimeouts": {
        "island_info": 30
    },
    "proxyMappings": [
        {
            "clientPort": 4050,
            "serverHost": "localhost",
            "serverPort": 4040
        }
    ],
    "messageProcessor": {
        "enabled": false,
        "urls": [
            "ws://127.0.0.1:7100/process"
        ]
    }
}
```

- **proxyMappings**: Defines the ports the middleman listens on (`clientPort`) and where it forwards traffic (`serverHost`, `serverPort`).
- **messageProcessor**: Configures the connection to the external Message Processor service.
- **apiPort**: The port for the REST API.

See [config.json](/middleman/config.json) for the config file similar to one used in my environment.

## Connection Management & Timeouts

The Middleman includes an automatic disconnection feature to optimize resource usage. Ravenfall can consume significant CPU resources when a client is connected, even if idle. To mitigate this, the Middleman disconnects from the server after a period of inactivity.

This behavior is controlled by the following configuration options:

- **disableTimeout**: Set to `true` to completely disable the timeout feature (keep connections open indefinitely).
- **defaultTimeoutSeconds**: The default time (in seconds) to keep the connection open after receiving a message with a known identifier.
- **noIdentifierTimeoutSeconds**: The time (in seconds) to keep the connection open after receiving a message *without* an identifier.
- **identifierTimeouts**: A map of specific timeouts for specific message identifiers. For example, `"island_info": 30` keeps the connection open for 30 seconds after receiving an "island_info" message.

When a message is received, the connection's expiry time is extended based on these rules. If the timer expires, the Middleman closes the connection to the server (but keeps the client connection open, ready to reconnect when the client sends a new message).

## API Documentation

The Middleman exposes a REST API for controlling connections and sending messages.

See [API.md](API.md) for full documentation (fully ai generated).
