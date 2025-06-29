import asyncio
import json
import logging
import os
import signal
import aiohttp
import websockets
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
HOST = os.getenv('HOST', '127.0.0.1')
PORT = int(os.getenv('PORT', '8000'))
MIDDLEMAN_API_HOST = os.getenv('MIDDLEMAN_API_HOST', 'http://127.0.0.1:8080')
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

async def call_middleman_api(endpoint: str, method: str = 'GET', data: Optional[Dict] = None) -> Tuple[Dict, int]:
    """Make an API call to the middleman server."""
    url = f"{MIDDLEMAN_API_HOST.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {'Content-Type': 'application/json'}
    
    try:
        async with aiohttp.ClientSession() as session:
            if method.upper() == 'GET':
                async with session.get(url, headers=headers) as response:
                    return await response.json(), response.status
            else:
                async with session.post(url, json=data, headers=headers) as response:
                    return await response.json(), response.status
    except Exception as e:
        logger.error(f"Error calling middleman API: {str(e)}", exc_info=DEBUG)
        return {"error": f"Failed to connect to middleman API: {str(e)}"}, 500

async def list_connections() -> Dict:
    """List all active connections."""
    response, status = await call_middleman_api('/api/connections')
    return response

async def force_reconnect(connection_id: str, timeout: int = 0) -> Dict:
    """Force a reconnection for the specified connection."""
    data = {
        "connectionId": connection_id,
        "timeout": timeout
    }
    response, status = await call_middleman_api('/api/reconnect', 'POST', data)
    return response

async def send_to_client(connection_id: str, message: str) -> Dict:
    """Send a message to a specific client."""
    data = {
        "connectionId": connection_id,
        "data": message
    }
    response, status = await call_middleman_api('/api/send-to-client', 'POST', data)
    return response

async def send_to_server(connection_id: str, message: str) -> Dict:
    """Send a message to the server through a specific connection."""
    data = {
        "connectionId": connection_id,
        "data": message
    }
    response, status = await call_middleman_api('/api/send-to-server', 'POST', data)
    return response


async def ensure_connected(connection_id: str, timeout: int = 0) -> Dict:
    """
    Ensure the connection to the server is active.
    
    Args:
        connection_id: The connection ID to check/ensure
        timeout: Optional timeout in seconds for the connection (0 for default)
        
    Returns:
        Dict containing the result of the operation
    """
    data = {
        "connectionId": connection_id,
        "timeout": timeout
    }
    response, status = await call_middleman_api('/api/ensure-connected', 'POST', data)
    return response


async def send_and_wait_response(connection_id: str, message: str, correlation_id: str = "", timeout: int = 30) -> Dict:
    """
    Send a message to the server and wait for a response with the given correlation ID.
    
    Args:
        connection_id: The connection ID to send the message through
        message: The message to send to the server
        correlation_id: Optional correlation ID to match the response. If not provided, one will be generated.
        timeout: Maximum time in seconds to wait for a response (default: 30)
        
    Returns:
        Dict containing the response data or error information
    """
    data = {
        "connectionId": connection_id,
        "data": message,
        "timeout": timeout
    }
    
    if correlation_id:
        data["correlationId"] = correlation_id
        
    response, status = await call_middleman_api('/api/send-and-wait-response', 'POST', data)
    return response

async def process_message(message: str) -> str:
    """
    Process an incoming message and return a response.
    
    To block a message, return a dictionary with {"block": True}
    To modify a message, return a dictionary with {"message": "modified message"}
    To allow the message through unmodified, return an empty dictionary or None
    
    Args:
        message: The incoming message as a string (expected to be JSON)
        
    Returns:
        str: The processed message as a JSON string with a trailing newline
    """
    try:
        # Parse the incoming message as JSON
        data = json.loads(message)
        
        # Check if this is a wrapped message with source information
        if isinstance(data, dict) and 'source' in data and 'message' in data:
            source = data['source']  # 'client' or 'server'
            message_content = data['message']
            
            # Log the message with all its metadata
            logger.info(
                f"Processing {source.upper()} message from {data.get('client_addr', 'unknown')} "
                f"(conn: {data.get('connection_id', 'unknown')}) at {data.get('timestamp')}"
            )
            
            # Here you can add source-specific processing if needed
            if source == 'client':
                # Process client message
                processed_content = message_content
                logger.debug(f"Client message content: {message_content}")
            else:  # server
                # Process server message
                processed_content = message_content
                logger.debug(f"Server message content: {message_content}")
                
            # Return the processed content (you might want to wrap it back with source info)
            return json.dumps(processed_content) + '\n'
            
        # If it's not a wrapped message, process it as before
        return json.dumps(data) + '\n'
        
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON: {str(e)}"
        logger.error(f"{error_msg}. Message: {message[:200]}")
        # Return the original message with newline if it had one
        if not message.endswith('\n'):
            message += '\n'
        return message
    except Exception as e:
        error_msg = f"Error processing message: {str(e)}"
        logger.error(error_msg, exc_info=DEBUG)
        return json.dumps({"error": error_msg, "original_message": message}) + '\n'

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
            logger.debug(f"Sent response: {response[:100]}..." if len(response) > 100 else f"Sent response: {response}")
            
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
