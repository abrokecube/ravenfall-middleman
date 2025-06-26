#!/usr/bin/env python3
"""
WebSocket client for Ravenfall Middleman

This script connects to the WebSocket server and prints all incoming messages.
Configuration is loaded from a .env file in the same directory.
"""
import asyncio
import json
import logging
import os
import signal
import sys
from typing import Dict, Any, Optional

import websockets
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration with defaults
CONFIG = {
    'HOST': os.getenv('WEBSOCKET_HOST', 'localhost'),
    'PORT': int(os.getenv('WEBSOCKET_PORT', '8080')),
    'PATH': os.getenv('WEBSOCKET_PATH', 'ws'),
    'LOG_LEVEL': os.getenv('LOG_LEVEL', 'INFO'),
    'RECONNECT_DELAY': float(os.getenv('RECONNECT_DELAY', '5.0')),  # seconds
}

# Initialize logger
logger = logging.getLogger(__name__)

# Configure logging with the level from config (will be set in main)

class MiddlemanWebSocketClient:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the WebSocket client.
        
        Args:
            config: Configuration dictionary (uses CONFIG by default)
        """
        self.config = config or CONFIG
        self.uri = f"ws://{self.config['HOST']}:{self.config['PORT']}/{self.config['PATH']}"
        self.websocket = None
        self.running = False

    async def connect(self):
        """Establish WebSocket connection to the server."""
        logger.info(f"Connecting to {self.uri}...")
        try:
            # Add connection timeout and better error handling
            self.websocket = await asyncio.wait_for(
                websockets.connect(
                    self.uri,
                    ping_interval=30,
                    ping_timeout=10,
                    open_timeout=10  # Add open timeout
                ),
                timeout=15  # Overall connection timeout
            )
            self.running = True
            logger.info("Successfully connected to WebSocket server")
        except asyncio.TimeoutError:
            logger.error(f"Connection to {self.uri} timed out after 15 seconds")
            raise
        except websockets.InvalidURI as e:
            logger.error(f"Invalid WebSocket URI: {self.uri}. Error: {e}")
            raise
        except websockets.InvalidHandshake as e:
            logger.error(f"WebSocket handshake failed. The server may not be a WebSocket server or the path is incorrect. Error: {e}")
            raise
        except ConnectionRefusedError as e:
            logger.error(f"Connection refused. Is the server running at {self.uri}? Error: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket server at {self.uri}. Error: {str(e)}")
            logger.debug("Full error details:", exc_info=True)
            raise

    async def receive_messages(self):
        """
        Continuously receive and process messages from the WebSocket.
        Automatically reconnects if the connection is lost.
        """
        while self.running:
            try:
                if not self.websocket:
                    logger.info("Not connected to WebSocket server. Attempting to connect...")
                    await self.connect()
                    
                message = await self.websocket.recv()
                self.handle_message(message)
                
            except websockets.exceptions.ConnectionClosed as e:
                logger.error(f"WebSocket connection closed: {e}")
                if self.running:  # Only attempt to reconnect if we're still supposed to be running
                    logger.info(f"Attempting to reconnect in {self.config['RECONNECT_DELAY']} seconds...")
                    await asyncio.sleep(self.config['RECONNECT_DELAY'])
                continue
                
            except (websockets.exceptions.WebSocketException, ConnectionError) as e:
                logger.error(f"WebSocket error: {e}")
                if self.running:
                    logger.info(f"Attempting to reconnect in {self.config['RECONNECT_DELAY']} seconds...")
                    await asyncio.sleep(self.config['RECONNECT_DELAY'])
                continue
                
            except asyncio.CancelledError:
                logger.info("Message receiving was cancelled")
                raise
                
            except Exception as e:
                logger.error(f"Unexpected error in receive_messages: {e}", exc_info=True)
                if self.running:
                    logger.info(f"Will attempt to reconnect in {self.config['RECONNECT_DELAY']} seconds...")
                    await asyncio.sleep(self.config['RECONNECT_DELAY'])
                continue

    def handle_message(self, message: str):
        """
        Process an incoming WebSocket message.
        
        Args:
            message: Raw message string from WebSocket
        """
        try:
            # Parse the message as JSON
            data = json.loads(message)
            
            # Format the output
            print("\n" + "=" * 80)
            print(f"Source: {data.get('source', 'unknown').upper()}")
            print(f"From: {data.get('client_addr', 'unknown')}")
            print(f"To: {data.get('server_addr', 'unknown')}")
            print(f"Connection ID: {data.get('connection_id', 'unknown')}")
            print(f"Timestamp: {data.get('timestamp', 'unknown')}")
            
            # Pretty print the message content
            print("\nMessage:")
            try:
                # Try to parse the message as JSON for pretty printing
                message_content = json.loads(data.get('message', '{}'))
                print(json.dumps(message_content, indent=2))
            except (json.JSONDecodeError, TypeError):
                # If not JSON, print as-is
                print(data.get('message', ''))
                
        except json.JSONDecodeError:
            logger.warning(f"Received non-JSON message: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def close(self):
        """Close the WebSocket connection."""
        self.running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket connection closed")


async def main():
    """Main function to run the WebSocket client."""
    # Set up logging level from config
    log_level = getattr(logging, CONFIG['LOG_LEVEL'].upper(), None)
    if not isinstance(log_level, int):
        log_level = logging.INFO  # Default to INFO if invalid level is provided
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Update the module logger level
    logger.setLevel(log_level)
    
    logger.info("Starting WebSocket client with configuration:")
    logger.info(f"  Host: {CONFIG['HOST']}")
    logger.info(f"  Port: {CONFIG['PORT']}")
    logger.info(f"  Path: {CONFIG['PATH']}")
    logger.info(f"  Log Level: {CONFIG['LOG_LEVEL']}")
    logger.info(f"  Full URI: ws://{CONFIG['HOST']}:{CONFIG['PORT']}/{CONFIG['PATH']}")
    
    # Verify port is an integer
    if not isinstance(CONFIG['PORT'], int) or not (0 < CONFIG['PORT'] <= 65535):
        logger.error(f"Invalid port number: {CONFIG['PORT']}")
        return
    
    client = MiddlemanWebSocketClient()
    
    # Set up signal handler for graceful shutdown
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    
    try:
        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
    except (NotImplementedError, RuntimeError) as e:
        logger.warning(f"Signal handling not available: {e}")
    
    receive_task = None
    
    try:
        # Connect to the WebSocket server
        logger.info("Attempting to connect to WebSocket server...")
        await client.connect()
        
        # Start receiving messages in the background
        receive_task = asyncio.create_task(client.receive_messages())
        logger.info("WebSocket client is running. Press Ctrl+C to stop.")
        
        # Wait for a stop signal or error
        await stop
        
    except asyncio.CancelledError:
        logger.info("Received shutdown signal, stopping client...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
    finally:
        if receive_task and not receive_task.done():
            receive_task.cancel()
            try:
                await receive_task
            except asyncio.CancelledError:
                pass
        
        await client.close()
        logger.info("Client stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Client stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
