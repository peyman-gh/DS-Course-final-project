import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, String, Float, DateTime, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import redis
from datetime import datetime

# Database setup
Base = declarative_base()

class AggregatedMetric(Base):
    __tablename__ = 'aggregated_metrics'

    symbol = Column(String, primary_key=True)
    metric = Column(String, primary_key=True)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)

# Configuration
DATABASE_URI = 'postgresql://postgres:postgres@localhost:5432/postgres'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
KAFKA_TOPIC = 'stock'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_CONSUMER_GROUP = 'analyze'

# Redis setup
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Kafka setup
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_CONSUMER_GROUP,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# SQLAlchemy setup
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)
session = Session()

# Helper functions for metrics
def calculate_moving_average(data, window_size):
    return sum(data[-window_size:]) / window_size

def calculate_exponential_moving_average(data, window_size):
    k = 2 / (window_size + 1)
    ema = calculate_moving_average(data[:window_size], window_size)
    for price in data[window_size:]:
        ema = price * k + ema * (1 - k)
    return ema

def calculate_rsi(prices, period):
    gains, losses = 0, 0
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change > 0:
            gains += change
        else:
            losses -= change

    avg_gain = gains / period
    avg_loss = losses / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# Service logic
def process_message(message):
    symbol = message['symbol']
    timestamp = datetime.fromtimestamp(message['timestamp'])

    # Intermediate values
    total_volume = redis_client.get(f"{symbol}_total_volume")
    total_volume = float(total_volume) if total_volume else 0

    count = redis_client.get(f"{symbol}_count")
    count = int(count) if count else 0

    total_volume += message['volume']
    count += 1

    # Cache updated values
    redis_client.set(f"{symbol}_total_volume", total_volume)
    redis_client.set(f"{symbol}_count", count)

    # Calculate averages
    average_volume = total_volume / count

    # Persist metrics every 10 messages
    if count % 10 == 0:
        metrics = [
            AggregatedMetric(symbol=symbol, metric='total_volume', value=total_volume, timestamp=timestamp),
            AggregatedMetric(symbol=symbol, metric='average_volume', value=average_volume, timestamp=timestamp),
        ]
        session.bulk_save_objects(metrics)
        session.commit()

# Consume messages
for msg in consumer:
    try:
        process_message(msg.value)
    except Exception as e:
        print(f"Error processing message: {e}")
