import asyncio
import websockets
import json
import logging
from typing import Any
from validators import validate_market_data
from pydantic import ValidationError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def handle_client(websocket):
    """Handle incoming WebSocket connections and messages"""
    client_id = id(websocket)
    logging.info(f"New client connected. ID: {client_id}")
    
    try:
        async for message in websocket:
            try:
                # Parse JSON
                data = json.loads(message)
                
                # Validate the data
                validated_data = validate_market_data(data)
                
                # Print the validated data
                print("\n=== Received Valid Data ===")
                print(f"Data Type: {validated_data.data_type}")
                
                # Convert to dict and print other fields
                data_dict = validated_data.dict()
                for key, value in data_dict.items():
                    if key != 'data_type':
                        print(f"{key}: {value}")
                print("========================\n")
                
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON: {e}")
            except ValidationError as e:
                logging.error(f"Validation error: {e}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                
    except websockets.exceptions.ConnectionClosed:
        logging.info(f"Client {client_id} disconnected")
    except Exception as e:
        logging.error(f"Error handling client {client_id}: {e}")

async def main():
    """Start the WebSocket server"""
    host = "localhost"
    port = 1111
    
    async with websockets.serve(handle_client, host, port):
        logging.info(f"WebSocket server started on ws://{host}:{port}")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    except Exception as e:
        logging.error(f"Server error: {e}")