import asyncio
import json
import logging
import os
import signal
from datetime import datetime
from dotenv import load_dotenv
import websockets

# Load environment variables
load_dotenv()

# Configuration
HOST = os.getenv('HOST', 'localhost')
PORT = int(os.getenv('PORT', '8000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
MAX_MESSAGE_SIZE_MB = int(os.getenv('MAX_MESSAGE_SIZE_MB', '10'))  # 10MB default
MAX_MESSAGE_SIZE = MAX_MESSAGE_SIZE_MB * 1024 * 1024  # Convert MB to bytes
COMPRESSION_ENABLED = os.getenv('COMPRESSION_ENABLED', 'true').lower() == 'true'
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'

# Configure logging
log_level = logging.DEBUG if DEBUG else getattr(logging, LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('message_processor')

# Global variable to control the server loop
stop_event = asyncio.Event()

def handle_sigint():
    """Handle Ctrl+C signal to gracefully shut down the server."""
    logger.info("Shutting down server...")
    stop_event.set()

async def process_message(message: str) -> str:
    """
    Process an incoming message and return a response.
    
    Args:
        message: The incoming message as a string (expected to be JSON)
        
    Returns:
        str: The processed message as a JSON string
    """
    try:
        # Parse the incoming message as JSON
        data = json.loads(message)
        
        # Return the processed message as JSON
        return json.dumps(data)
        
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON: {str(e)}"
        logger.error(error_msg)
        return json.dumps({"error": error_msg, "original_message": message})
    except Exception as e:
        error_msg = f"Error processing message: {str(e)}"
        logger.error(error_msg, exc_info=DEBUG)
        return json.dumps({"error": error_msg, "original_message": message})

async def handler(websocket):
    """Handle incoming WebSocket connections and messages."""
    client_ip = websocket.remote_address[0] if websocket.remote_address else 'unknown'
    logger.info(f"New connection from {client_ip}")
    
    try:
        async for message in websocket:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            
            logger.debug(f"Received message from {client_ip}: {message[:200]}" + 
                       ("..." if len(message) > 200 else ""))
            
            # Process the message and send response
            response = await process_message(message)
            await websocket.send(response)
            logger.debug(f"Sent response to {client_ip}")
            
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Connection closed by client {client_ip}")
    except Exception as e:
        logger.error(f"Error in connection handler: {str(e)}", exc_info=DEBUG)
    finally:
        logger.info(f"Connection closed for {client_ip}")

async def start_server():
    """Start the WebSocket server with the specified configuration."""
    # Set up signal handler for graceful shutdown
    loop = asyncio.get_running_loop()
    # For Windows, signal handling is different
    try:
        loop.add_signal_handler(signal.SIGINT, handle_sigint)
        loop.add_signal_handler(signal.SIGTERM, handle_sigint)
    except NotImplementedError:
        logger.warning("Signal handlers not fully supported on Windows. Use Ctrl+C to stop.")

    # Start the server
    server = await websockets.serve(
        handler,
        host=HOST,
        port=PORT,
        compression='deflate' if COMPRESSION_ENABLED else None,
        max_size=MAX_MESSAGE_SIZE,
        ping_interval=20,  # 20 seconds
        ping_timeout=20,   # 20 seconds
        close_timeout=5,   # 5 seconds
        max_queue=32,      # 32 messages
    )
    
    logger.info(f"Message processor server started on ws://{HOST}:{PORT}")
    logger.info("Press Ctrl+C to stop the server")
    
    try:
        await stop_event.wait()  # Run until we receive a stop signal
    except asyncio.CancelledError:
        pass
    finally:
        # Close the server
        server.close()
        await server.wait_closed()
        logger.info("Server stopped")

if __name__ == "__main__":
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=DEBUG)
        raise
