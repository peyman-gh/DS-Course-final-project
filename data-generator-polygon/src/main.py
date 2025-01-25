import time
import random
import json
import psutil
import os
import threading
import socket
import logging
from typing import Dict, Any
from datetime import datetime, timedelta
from polygon import RESTClient

# Initialize the Polygon client
api_key = 'KmtQqnDSBEmtknsUv_qeuWx6HdkMbKXE'  # Replace with your actual API key
client = RESTClient(api_key=api_key)
stock_symbols = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]
end_date = datetime.now()
start_date = end_date -  timedelta(days=1)


# Function to convert date to 'YYYY-MM-DD' format
def format_date(date):
    return date.strftime('%Y-%m-%d')


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

# Configuration
HOST = os.getenv("INGESTION_SVC_HOST")
PORT = int(os.getenv("INGESTION_SVC_PORT"))
RECONNECT_DELAY = 5
BUFFER_SIZE = 4096


# Set process affinity to a single core
p = psutil.Process(os.getpid())
p.cpu_affinity([0])

class DataGenerator:
    def __init__(self):
        self.socket = None
        self.is_connected = False
        self.connect()

    def connect(self) -> None:
        """Establish socket connection with reconnection logic"""
        while not self.is_connected:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((HOST, PORT))
                self.is_connected = True
                logging.info("Connected to server")
            except socket.error as e:
                logging.error(f"Connection failed: {e}")
                logging.info(f"Reconnecting in {RECONNECT_DELAY} seconds...")
                time.sleep(RECONNECT_DELAY)
                continue

    def send_data(self, data: Dict[str, Any]) -> None:
        """Send data through socket connection with length prefix"""
        if not self.is_connected:
            logging.warning("Not connected to server")
            return

        try:
            # Convert data to JSON and encode
            json_data = json.dumps(data)
            message = json_data.encode('utf-8')

            # Add message length prefix (4 bytes)
            message_length = len(message)
            length_prefix = message_length.to_bytes(4, byteorder='big')

            # Send length prefix and message
            self.socket.sendall(length_prefix + message)

            current_time = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
            print(f"{current_time} Sent (Polygon Data Generator) : {data['data_type']}")

        except Exception as e:
            logging.error(f"Error sending data: {e}")
            self.is_connected = False
            self.socket.close()
            self.connect()


    def run(self) -> None:

        # Main stock data generation loop
        while True:

            stock_data = []
            for symbol in stock_symbols:
                try:
                    # Retrieve daily aggregates (bars) for the stock
                    aggs = client.list_aggs(
                        ticker=symbol,
                        multiplier=1,
                        timespan="day",
                        from_=format_date(start_date),
                        to=format_date(end_date),
                        limit=5000
                    )
                    for agg in aggs:
                        data_point = {
                            "data_type": "stock_price",
                            "stock_symbol": symbol,
                            "opening_price": agg.open,
                            "closing_price": agg.close,
                            "high": agg.high,
                            "low": agg.low,
                            "volume": agg.volume,
                            "timestamp": time.time() #agg.timestamp
                        }
                        stock_data.append(data_point)
                except Exception as e:
                    print(f"Error retrieving data for {symbol}: {e}")

            for data in stock_data:
                self.send_data(data)

            time.sleep(86400)

if __name__ == "__main__":
    generator = DataGenerator()
    generator.run()