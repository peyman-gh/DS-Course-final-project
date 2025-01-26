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
import requests
from datetime import datetime
import time
import random

HOST = os.getenv("INGESTION_SVC_HOST")
PORT = int(os.getenv("INGESTION_SVC_PORT"))
RECONNECT_DELAY = 5
BUFFER_SIZE = 4096

stock_symbols = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
def convert_to_timestamp(time_string: str) -> float:
    dt_object = datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S")
    timestamp = dt_object.timestamp()
    return timestamp


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

    def send_data(self, data) -> None:

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
            print(f"{current_time} Sent (Real Data Generator) : {data['data_type']}")

        except Exception as e:
            logging.error(f"Error sending data: {e}")
            self.is_connected = False
            self.socket.close()
            self.connect()


    def run(self) -> None:

        # Main stock data generation loop
        while True:
            
            for s in stock_symbols:
                url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={s}&interval=30min&apikey=3WMSUO6A7YEEXZGN"
                response = requests.get(url)
                if response.status_code == 200:
                    data_dict = response.json()

                    time_series = data_dict["Time Series (15min)"]

                    # Loop through the time series and extract the values
                    for timestamp, values in time_series.items():


                        open_price = values["1. open"]
                        high_price = values["2. high"]
                        low_price = values["3. low"]
                        close_price = values["4. close"]
                        volume = values["5. volume"]

                        data_point = {
                            "data_type": "stock_price",
                            "stock_symbol": s,
                            "opening_price": open_price,
                            "closing_price": close_price,
                            "high": high_price,
                            "low": low_price,
                            "volume": volume,
                            "timestamp": convert_to_timestamp(timestamp)
                        }

                        self.send_data(data_point)


                else:
                    print(f"Failed to retrieve data. Status code: {response.status_code}")

            time.sleep(86400)

if __name__ == "__main__":
    generator = DataGenerator()
    generator.run()