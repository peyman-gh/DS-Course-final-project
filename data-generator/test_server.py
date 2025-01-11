import asyncio
import websockets
import json
import logging
from typing import Any

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
                # Parse and print the received data
                data = json.loads(message)
                print("\n=== Received Data ===")
                print(f"Data Type: {data.get('data_type', 'unknown')}")

                # Pretty print the data
                for key, value in data.items():
                    if key != 'data_type':
                        print(f"{key}: {value}")
                print("==================\n")

            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON: {e}")
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