import time
import random
import json
import psutil
import os
import numpy as np
import threading
import socket
import logging
from typing import Dict, Any
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

# Configuration
HOST = os.getenv("INGESTION_SVC_HOST", "localhost")
PORT = int(os.getenv("INGESTION_SVC_PORT", "1111"))
RECONNECT_DELAY = 5
BUFFER_SIZE = 4096
stocks = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]

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
            print(f"{current_time} Sent: {data['data_type']}")
            
        except Exception as e:
            logging.error(f"Error sending data: {e}")
            self.is_connected = False
            self.socket.close()
            self.connect()

    # The rest of the data generation methods remain the same
    def generate_stock_data(self) -> Dict[str, Any]:
        """Generate random stock price data"""
        stock_symbol = random.choice(stocks)
        prev_price = 1000
        dt = 1
        mu = 0.0002
        sigma = 0.01

        price_change = np.exp((mu - 0.5 * sigma**2) * dt +
                            sigma * np.sqrt(dt) * np.random.normal())
        opening_price = max(0, prev_price * price_change)
        closing_price = max(0, opening_price +
                          round(random.normalvariate(0, 10), 2))
        high = max(opening_price, closing_price) + \
            round(abs(random.normalvariate(0, 5)), 2)
        low = min(opening_price, closing_price) - \
            round(abs(random.normalvariate(0, 5)), 2)
        volume = max(0, int(np.random.poisson(5000) *
                          (1 + 0.1 * np.random.normal())))

        return {
            "data_type": "stock_price",
            "stock_symbol": stock_symbol,
            "opening_price": opening_price,
            "closing_price": closing_price,
            "high": high,
            "low": low,
            "volume": volume,
            "timestamp": time.time()
        }

    def generate_additional_data(self) -> Dict[str, Any]:
        """Generate additional market-related data"""
        stock_symbol = random.choice(stocks)
        timestamp = time.time()
        data_types = ['order_book', 'news_sentiment',
                     'market_data', 'economic_indicator']
        data_type = random.choice(data_types)

        if data_type == 'order_book':
            return {
                "data_type": "order_book",
                "timestamp": timestamp,
                "stock_symbol": stock_symbol,
                "order_type": random.choice(['buy', 'sell']),
                "price": random.uniform(100, 1000),
                "quantity": random.randint(1, 100)
            }
        elif data_type == 'news_sentiment':
            return {
                "data_type": "news_sentiment",
                "timestamp": timestamp,
                "stock_symbol": stock_symbol,
                "sentiment_score": random.uniform(-1, 1),
                "sentiment_magnitude": random.uniform(0, 1)
            }
        elif data_type == 'market_data':
            return {
                "data_type": "market_data",
                "timestamp": timestamp,
                "stock_symbol": stock_symbol,
                "market_cap": random.uniform(1e9, 1e12),
                "pe_ratio": random.uniform(5, 30)
            }
        else:  # economic_indicator
            return {
                "data_type": "economic_indicator",
                "timestamp": timestamp,
                "indicator_name": "GDP Growth Rate",
                "value": random.uniform(-5, 5)
            }

    def run_additional_data_generator(self) -> None:
        """Run the additional data generation loop"""
        while True:
            data = self.generate_additional_data()
            self.send_data(data)
            time.sleep(random.uniform(1, 5))

    def run(self) -> None:
        """Main execution method"""
        # Start additional data generator thread
        threading.Thread(
            target=self.run_additional_data_generator,
            daemon=True
        ).start()

        # Main stock data generation loop
        while True:
            data = self.generate_stock_data()
            self.send_data(data)
            time.sleep(random.uniform(1, 5))

if __name__ == "__main__":
    generator = DataGenerator()
    generator.run()