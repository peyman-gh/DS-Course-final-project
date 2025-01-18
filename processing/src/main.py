from kafka import KafkaProducer
import json
import time

def json_serializer(data):
    """Serializes Python objects to JSON."""
    return json.dumps(data).encode('utf-8')

def main():
    topic_name = 'xxx'
    bootstrap_servers = 'localhost:9092'

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=json_serializer
    )

    try:
        print(f"Sending data to Kafka topic '{topic_name}'...")
        for i in range(10):  # Send 10 messages
            message = {"id": i, "message": f"This is message {i}"}
            producer.send(topic_name, value=message)
            print(f"Sent: {message}")
            time.sleep(1)  # Wait 1 second between messages
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()
