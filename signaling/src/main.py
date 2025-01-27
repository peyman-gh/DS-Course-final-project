import json
import logging
import os

import psycopg2
from datetime import datetime, timezone
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")

# Initialize Kafka producer
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        security_protocol='PLAINTEXT'
    )
    logging.info("Kafka producer initialized successfully")
except Exception as e:
    logging.error(f"Failed to initialize Kafka producer: {e}")
    raise

def send_data_to_signals_topic(json_str):

    try:
        # Send the JSON string directly after encoding to bytes
        future = kafka_producer.send(
            topic=KAFKA_TOPIC_NAME,
            value=json_str.encode('utf-8')
        )

        # Wait for message to be sent
        record_market_data_metadata = future.get(timeout=10)

        logging.info(
            f"Signal sent to Kafka ✅ - Topic: {record_market_data_metadata.topic}"
        )


    except KafkaError as e:
        logging.error(f"Failed to send data to Kafka: {e}")
        raise
    except Exception as e:
        logging.error(f"Error in forwarding data: {e}")
        raise

def fetch_new_rsi_rows(last_timestamp):
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()

    query = """
    SELECT * 
    FROM technical_indicators
    WHERE created_at > %s
    ORDER BY created_at;
    """

    cursor.execute(query, (last_timestamp,))
    rows = cursor.fetchall()

    conn.commit()
    cursor.close()
    conn.close()
    return rows

def fetch_new_market_data_rows(last_timestamp):
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()

    query = """
    SELECT *
    FROM market_data WHERE created_at > %s ORDER BY created_at;
    """

    cursor.execute(query, (last_timestamp,))
    rows = cursor.fetchall()

    conn.commit()
    cursor.close()
    conn.close()
    return rows



"""
Fetch RSI indicator data and generate buy/sell signals based on that.
"""
last_timestamp_rsi = datetime.now(timezone.utc)
def rsi_based_signaling():
    global last_timestamp_rsi

    # Fetch new rows
    rows = fetch_new_rsi_rows(last_timestamp_rsi)
    for row in rows:
        symbol= row[1]
        indicator= row[2]
        period= row[3]
        value= row[4]

        sig={}

        if indicator=='RSI' and value<30:
            sig = {"stock_symbol":symbol,"signal":"buy"}
        elif indicator=='RSI' and value>70:
            sig = {"stock_symbol": symbol, "signal": "sell"}

        # Send signal to signals topic
        if sig:
            send_data_to_signals_topic(json.dumps(sig))

    # Update the last timestamp if rows are fetched
    if rows:
        last_timestamp_rsi = max(row[6] for row in rows)  # Assuming 'created_at' is at index 6

"""
Fetch market data and generate buy/sell signals based on P/E ratio.
"""
pe_buy_threshold=15
pe_sell_threshold=25
last_timestamp_pe = datetime.now(timezone.utc)
def pe_ratio_based_signaling():

    global last_timestamp_pe

    # Fetch new rows
    rows = fetch_new_rsi_rows(last_timestamp_pe)
    for row in rows:
        symbol= row[1]
        pe_ratio= row[3]

        sig={}

        if pe_ratio < pe_buy_threshold:
            sig = {"stock_symbol":symbol,"signal":"buy"}
        elif pe_ratio > pe_sell_threshold:
            sig = {"stock_symbol":symbol,"signal":"sell"}

        # Send signal to signals topic
        if sig:
            send_data_to_signals_topic(json.dumps(sig))

    # Update the last timestamp if rows are fetched
    if rows:
        last_timestamp_pe = max(row[5] for row in rows)



def main():

    while True:
        print("Getting recent indicators to make signals (RSI-based) ...")
        rsi_based_signaling()
        time.sleep(15)
        print("Getting recent indicators to make signals (PE-Ratio-based) ...")
        #pe_ratio_based_signaling()
        time.sleep(15)


if __name__ == "__main__":
    main()
