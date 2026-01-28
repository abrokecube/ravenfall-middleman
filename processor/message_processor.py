import asyncio
import datetime
import json
import logging
import os
import signal
import uuid
from typing import Dict, Any, Optional, Tuple, Callable, Awaitable, List, Union, TypedDict, TypeVar, Generic, Type, Literal
from uuid import UUID
from dataclasses import dataclass, field
from websockets import WebSocketServerProtocol as WebSocketServer
import websockets

# Type variable for message content types
T = TypeVar('T', bound=Dict[str, Any])

class Sender(TypedDict):
    Id: UUID
    CharacterId: UUID
    Username: str
    DisplayName: str
    Color: str
    Platform: str
    PlatformId: str
    IsBroadcaster: bool
    IsModerator: bool
    IsSubscriber: bool
    IsVip: bool
    IsGameAdministrator: bool
    IsGameModerator: bool
    SubTier: int
    Identifier: str

class RavenBotMessage(TypedDict):
    """Represents a message from RavenBot with its metadata."""
    Identifier: str
    Sender: Sender
    Content: str
    CorrelationId: UUID

class Recipient(TypedDict):
    """Represents the recipient information in a Ravenfall message."""
    UserId: UUID
    CharacterId: UUID
    Platform: str
    PlatformId: str
    PlatformUserName: str

class RavenfallMessage(TypedDict):
    """Represents a message received from Ravenfall."""
    Identifier: str  # e.g., "message"
    Recipient: Recipient
    Format: str  # Format string for the message
    Args: List[str]  # Arguments to be inserted into the format string
    Tags: List[str]  # Any tags associated with the message
    Category: str  # Message category (if any)
    CorrelationId: UUID  # For tracking the message

# Union type for all possible message types
RavenMessage = Union[RavenBotMessage, RavenfallMessage, Dict[str, Any]]

# Configure logging
logger = logging.getLogger('new_message_processor')

@dataclass
class ProcessorResponse(TypedDict, total=False):
    """Response format for message processor callbacks."""
    block: bool  # If True, the message will be blocked
    message: Dict[str, Any]  # Modified message content (optional)
    error: str  # Error message (optional)

@dataclass
class MessageMetadata:
    """Metadata about a message being processed."""
    source: str = "unknown"
    connection_id: str = "unknown"
    correlation_id: str = ""
    is_api: bool = False
    timestamp: str = field(default_factory=lambda: datetime.datetime.utcnow().isoformat())
    custom_metadata: Dict[str, Any] = field(default_factory=dict)
    client_addr: str = ""
    server_addr: str = ""

# Define types for callbacks
MessageCallback = Callable[
    [RavenMessage, MessageMetadata, 'ClientInfo'],  # message_data, metadata, client_info
    Awaitable[Optional[RavenMessage]]  # Return None to keep current message, or return new message data
]
ConnectionCallback = Callable[['ClientInfo'], Awaitable[None]]

@dataclass
class ClientInfo:
    """Information about a connected WebSocket client."""
    websocket: WebSocketServer
    client_id: str
    remote_address: str
    connection_time: float = field(default_factory=lambda: asyncio.get_event_loop().time())
    metadata: Dict[str, Any] = field(default_factory=dict)

class MessageProcessor:
    def __init__(self, host: str = '0.0.0.0', port: int = 8000, max_message_size: int = 10 * 1024 * 1024):
        """
        Initializes the MessageProcessor with WebSocket server configuration.
        
        Args:
            host: Host to bind the WebSocket server to
            port: Port to listen on
            max_message_size: Maximum message size in bytes (default: 10MB)
        """
        self.host = host
        self.port = port
        self.max_message_size = max_message_size
        self.server = None
        self.clients: Dict[str, ClientInfo] = {}
        self.running = False
        
        # Callback lists
        self.message_callbacks: List[MessageCallback] = []
        self.connection_callbacks: List[ConnectionCallback] = []
        self.disconnection_callbacks: List[ConnectionCallback] = []
        
        logger.info(f"MessageProcessor initialized on {host}:{port}")

    # Callback registration methods
    def add_message_callback(self, callback: MessageCallback) -> None:
        """Register a callback to process incoming messages."""
        self.message_callbacks.append(callback)
        
    def add_connection_callback(self, callback: ConnectionCallback) -> None:
        """Register a callback for new client connections."""
        self.connection_callbacks.append(callback)
        
    def add_disconnection_callback(self, callback: ConnectionCallback) -> None:
        """Register a callback for client disconnections."""
        self.disconnection_callbacks.append(callback)
    
    async def process_message(self, message: str, client_info: ClientInfo) -> str:
        """
        Processes an incoming message and returns a response.

        This method can be overridden by subclasses or extended using callbacks.
        
        Args:
            message: The incoming message as a string (expected to be JSON).
            client_info: Information about the client that sent the message.
            
        Returns:
            str: The processed message as a JSON string with a trailing newline.
        """
        try:
            # Parse the message as JSON
            try:
                message_data: Dict[str, Any] = json.loads(message)
                
                # Create message metadata
                metadata = MessageMetadata(
                    source=message_data.pop('source', 'unknown'),
                    connection_id=message_data.pop('connection_id', 'unknown'),
                    correlation_id=message_data.pop('correlation_id', ''),
                    is_api=message_data.pop('is_api', False),
                    custom_metadata=message_data.pop('custom_metadata', {})
                )
                
                # The remaining data is the actual message content
                message_content: RavenMessage = message_data.pop('message', {})
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message as JSON: {e}")
                raise ValueError(f"Invalid JSON message: {message}")

            # Log processing information
            if metadata.is_api:
                logger.info(f"Processing API-originated message (Correlation ID: {metadata.correlation_id})")

            logger.debug(
                f"Processing message from {metadata.source} "
                f"(client: {client_info.client_id}, "
                f"connection: {metadata.connection_id}, "
                f"correlation: {metadata.correlation_id})"
            )
            logger.debug(f"Message data: {message_content}")

            # Process message through callbacks
            processor_response: Optional[ProcessorResponse] = None
            
            for callback in self.message_callbacks:
                try:
                    result = await callback(message_content, metadata, client_info)
                    if not result:
                        continue
                        
                    if not isinstance(result, dict):
                        logger.warning(f"Callback returned non-dict result: {result}")
                        continue
                        
                    # Check for block flag
                    if result.get('block') is True:
                        logger.debug(f"Message blocked by callback for connection {metadata.connection_id}")
                        return json.dumps({
                            "block": True,
                            "correlation_id": metadata.correlation_id
                        }) + '\n'
                        
                    # Update message content if provided
                    if 'message' in result:
                        message_content = result['message']
                        
                    # Store any error
                    if 'error' in result and result['error']:
                        processor_response = {
                            'error': str(result['error']),
                            'correlation_id': metadata.correlation_id
                        }
                        
                except Exception as e:
                    error_msg = f"Error in message callback: {e}"
                    logger.error(error_msg, exc_info=True)
                    processor_response = {
                        'error': error_msg,
                        'correlation_id': metadata.correlation_id
                    }
                    break
                    
            # If there was an error in processing, return it
            if processor_response and 'error' in processor_response:
                return json.dumps(processor_response) + '\n'

            # Prepare response with metadata
            response = {
                "message": message_content,  # Include all message data
                "correlation_id": metadata.correlation_id,
                "status": "processed"
            }
            
            # Only include source and connection_id if they're not empty
            if metadata.source and metadata.source != "unknown":
                response["source"] = metadata.source
            if metadata.connection_id and metadata.connection_id != "unknown":
                response["connection_id"] = metadata.connection_id
            
            return json.dumps(response) + '\n'
            
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON: {str(e)}"
            logger.error(f"{error_msg}. Message: {message[:200]}")
            # Return the original message with a newline if it didn't have one
            if not message.endswith('\n'):
                message += '\n'
            return message
        except Exception as e:
            error_msg = f"Error processing message: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return json.dumps({"error": error_msg, "original_message": message}) + '\n'
    
    async def _handle_client(self, websocket: WebSocketServer) -> None:
        """Handle a new WebSocket client connection.
        
        Args:
            websocket: The WebSocket connection instance
        """
        client_id = str(uuid.uuid4())
        remote_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        
        client_info = ClientInfo(
            websocket=websocket,
            client_id=client_id,
            remote_address=remote_address
        )
        
        self.clients[client_id] = client_info
        logger.info(f"Client connected: {client_id} from {remote_address}")
        
        # Notify connection callbacks
        for callback in self.connection_callbacks:
            try:
                await callback(client_info)
            except Exception as e:
                logger.error(f"Error in connection callback: {e}", exc_info=True)
        
        try:
            async for message in websocket:
                if isinstance(message, bytes):
                    message = message.decode('utf-8')
                
                # Process the message
                response = await self.process_message(message, client_info)
                
                # Send the response back to the client
                if response:
                    await websocket.send(response)
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {client_id} disconnected")
        except Exception as e:
            logger.error(f"Error with client {client_id}: {e}", exc_info=True)
        finally:
            # Clean up
            self.clients.pop(client_id, None)
            
            # Notify disconnection callbacks
            for callback in self.disconnection_callbacks:
                try:
                    await callback(client_info)
                except Exception as e:
                    logger.error(f"Error in disconnection callback: {e}", exc_info=True)
    
    def start(self) -> None:
        """Start the WebSocket server.
        
        Note: This method is synchronous. The server will run in the current event loop.
        """
        if self.running:
            logger.warning("Server is already running")
            return
            
        loop = asyncio.get_event_loop()
        
        # Create server coroutine
        server_coro = websockets.serve(
            self._handle_client,
            self.host,
            self.port,
            max_size=self.max_message_size,
            ping_interval=20,  # 20 seconds
            ping_timeout=20,   # 20 seconds
            close_timeout=5,   # 5 seconds
        )
        
        # Start the server, handling both running and new event loops
        if loop.is_running():
            # If loop is already running, create a task to start the server
            async def start_server():
                self.server = await server_coro
                self.running = True
                logger.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            
            # Schedule the server to start
            loop.create_task(start_server())
        else:
            # If no loop is running, use run_until_complete
            self.server = loop.run_until_complete(server_coro)
            self.running = True
            logger.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            
            # Only set up signal handlers if we're not in a running loop
            try:
                loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.astop()))
                loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(self.astop()))
            except NotImplementedError:
                # Windows compatibility
                pass
    
    async def astop(self) -> None:
        """Asynchronously stop the WebSocket server."""
        if not self.running:
            return
            
        logger.info("Stopping WebSocket server...")
        self.running = False
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
            
        # Close all client connections
        if self.clients:
            logger.info(f"Closing {len(self.clients)} client connections...")
            tasks = [
                client_info.websocket.close() 
                for client_info in self.clients.values()
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            self.clients.clear()
            
        logger.info("WebSocket server stopped")
        
    def stop(self) -> None:
        """Synchronously stop the WebSocket server."""
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(self.astop())
        else:
            loop.run_until_complete(self.astop())