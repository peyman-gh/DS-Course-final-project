import os
import socket
import json
import logging
import threading
import time
from typing import Any
from validators import validate_market_data
from pydantic import ValidationError
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


SOCKET_PORT = 5000
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
print("- ENVS:",KAFKA_SERVER,KAFKA_TOPIC_NAME)


class IngestionServer:
    def __init__(self, host: str = '0.0.0.0', port: int = SOCKET_PORT):
        self.host = host
        self.port = port
        self.server_socket = None
        self.clients = []


        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                security_protocol='PLAINTEXT'
            )
            logging.info("Kafka producer initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize Kafka producer: {e}")
            raise


    def start(self):
        """Start the server and listen for connections"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        logging.info(f"Server started on {self.host}:{self.port}")
        
        while True:
            try:
                client_socket, address = self.server_socket.accept()
                logging.info(f"New connection from {address}")
                self.clients.append(client_socket)
                
                # Start a new thread to handle this client
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")

    def receive_message(self, client_socket: socket.socket) -> bytes:
        """Receive a length-prefixed message"""
        # First receive the 4-byte length prefix
        length_prefix = client_socket.recv(4)
        if not length_prefix:
            return None
            
        # Convert length prefix to integer
        message_length = int.from_bytes(length_prefix, byteorder='big')
        
        # Receive the actual message
        message = b""
        remaining = message_length
        
        while remaining > 0:
            chunk = client_socket.recv(min(remaining, 4096))
            if not chunk:
                return None
            message += chunk
            remaining -= len(chunk)
            
        return message


    def forward_data_to_processing_service(self,json_str):
        """
        Forward validated data to Kafka topic
        Args:
            json_str: JSON string containing the validated market data
        """

        try:
            # Send the JSON string directly after encoding to bytes
            future = self.producer.send(
                topic=KAFKA_TOPIC_NAME,
                value=json_str.encode('utf-8'),
                #key=json.loads(json_str).get('stock_symbol', '').encode('utf-8')
            )

            # Wait for message to be sent
            record_market_data_metadata = future.get(timeout=10)
            
            logging.info(
                f"Data sent to Kafka ✅ - Topic: {record_market_data_metadata.topic}"
            )

            
        except KafkaError as e:
            logging.error(f"Failed to send data to Kafka: {e}")
            raise
        except Exception as e:
            logging.error(f"Error in forwarding data: {e}")
            raise

    
    def handle_client(self, client_socket: socket.socket, address: tuple):
        """Handle individual client connections"""
        while True:
            try:
                # Receive message
                message = self.receive_message(client_socket)
                if not message:
                    break
                
                # Parse and validate the data
                data_json_str = message.decode('utf-8')
                data = json.loads(data_json_str)
                validated_data = validate_market_data(data)
                
                # Print the validated data
                print("\n=== Received Valid Data ===")
                print(f"Data Type: {validated_data.data_type}")
                
                # Convert to dict and print other fields
                # data_dict = validated_data.dict()
                # for key, value in data_dict.items():
                #     if key != 'data_type':
                #         print(f"{key}: {value}")
                # print("========================\n")

                self.forward_data_to_processing_service(data_json_str)

                
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON from {address}: {e}")
            except ValidationError as e:
                logging.error(f"Validation error from {address}: {e}")
            except Exception as e:
                logging.error(f"Error handling client {address}: {e}")
                break
        
        # Clean up
        logging.info(f"Client {address} disconnected")
        if client_socket in self.clients:
            self.clients.remove(client_socket)
        client_socket.close()

    def shutdown(self):
        """Shutdown the server and clean up connections"""
        logging.info("Shutting down server...")
        for client in self.clients:
            try:
                client.close()
            except:
                pass
        if self.server_socket:
            self.server_socket.close()

if __name__ == "__main__":
    server = IngestionServer()
    try:
        server.start()
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
        server.shutdown()
    except Exception as e:
        logging.error(f"Server error: {e}")
        server.shutdown()
    
    