from kafka import KafkaConsumer
import json

def main():
    topic_name = 'xxx'
    bootstrap_servers = 'localhost:9092'

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',  # Read messages from the beginning
        enable_auto_commit=True,
        group_id='my-group',  # Consumer group name
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize JSON messages
    )

    print(f"Listening for messages on Kafka topic '{topic_name}'...")
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
