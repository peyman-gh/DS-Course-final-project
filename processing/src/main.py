import os
import traceback

from kafka import KafkaConsumer
import json
import inserter

KAFKA_SERVER = os.getenv("KAFKA_SERVER","localhost:9094")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME","market_data")

db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "postgres",
    "port": "5432"
}
db_inserter = inserter.MarketDataInserter(db_params)
db_inserter.connect()

def main():

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,  # Replace with your Kafka topic name
        bootstrap_servers=KAFKA_SERVER,  # Replace with your Kafka broker address
        group_id='proc-consumer-group',  # Consumer group ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"Listening for messages on Kafka topic '{KAFKA_TOPIC_NAME}'...")
    try:
        for message in consumer:
            data = message.value
            db_inserter.insert_data(data)
    except KeyboardInterrupt:
        print("Consumer stopped manually.")
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exception(e)
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()
