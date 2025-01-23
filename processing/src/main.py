import os
from kafka import KafkaConsumer
import json

def clear_and_write_to_file(file_path='temp.txt', content=''):
    try:
        # Open the file in write mode ('w'), which clears the file if it exists,
        # or creates it if it doesn't.
        with open(file_path, 'w') as file:
            file.write(str(content))
        print(f"Content successfully written to {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    KAFKA_SERVER = os.getenv("KAFKA_SERVER")
    KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")

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
            clear_and_write_to_file(content=message.value)
            #print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        print("Consumer stopped manually.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()
