from kafka import KafkaConsumer
import json

def main():
    KAFKA_SERVER = 'kafka:9092'
    KAFKA_TOPIC_NAME = "market_data"

    print(KAFKA_SERVER,KAFKA_TOPIC_NAME)
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='latest',  # Read messages from the beginning
        enable_auto_commit=True,
        group_id='my-group',  # Consumer group name
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize JSON messages
    )

    print(f"Listening for messages on Kafka topic '{KAFKA_TOPIC_NAME}'...")
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        print("Consumer stopped manually.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()
