import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Define SQLAlchemy base and model
Base = declarative_base()

class StockAnalysis(Base):
    __tablename__ = 'stock_analysis'
    symbol = Column(String, primary_key=True)
    indicator = Column(String, primary_key=True)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# Indicator calculations
def calculate_moving_average(data, window_size):
    return sum(d['closing_price'] for d in data[-window_size:]) / window_size

def calculate_ema(data, window_size):
    k = 2 / (window_size + 1)
    ema = calculate_moving_average(data[:window_size], window_size)
    for d in data[window_size:]:
        ema = d['closing_price'] * k + ema * (1 - k)
    return ema

def calculate_rsi(data, period):
    gains, losses = 0.0, 0.0
    for i in range(1, len(data)):
        change = data[i]['closing_price'] - data[i - 1]['closing_price']
        if change > 0:
            gains += change
        else:
            losses -= change
    
    avg_gain = gains / period
    avg_loss = losses / period

    if avg_loss == 0:
        return 100  # All gains, no losses

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# Analyze Service class
class AnalyzeService:
    def __init__(self, kafka_topic, kafka_group, kafka_servers, db_url, window_size=3):
        self.consumer = KafkaConsumer(
            kafka_topic,
            group_id=kafka_group,
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        
        self.window_size = window_size
        self.data_window = []

        # Set up database
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def process_message(self, stock_data):
        self.data_window.append(stock_data)

        if len(self.data_window) < self.window_size:
            return

        self.calculate_and_store_indicators()
        self.data_window.pop(0)  # Maintain sliding window

    def calculate_and_store_indicators(self):
        session = self.Session()
        try:
            ma = calculate_moving_average(self.data_window, self.window_size)
            ema = calculate_ema(self.data_window, self.window_size)
            rsi = calculate_rsi(self.data_window, self.window_size)

            timestamp = datetime.fromtimestamp(self.data_window[-1]['timestamp'])
            symbol = self.data_window[-1]['symbol']

            for indicator, value in [
                ('moving_average', ma),
                ('exponential_moving_average', ema),
                ('relative_strength_index', rsi),
            ]:
                analysis = StockAnalysis(
                    symbol=symbol,
                    indicator=indicator,
                    value=value,
                    timestamp=timestamp,
                )
                session.add(analysis)

            session.commit()
        except Exception as e:
            print(f"Error saving indicators: {e}")
            session.rollback()
        finally:
            session.close()

    def start(self):
        print("Starting Analyze Service...")
        for message in self.consumer:
            stock_data = message.value
            print(f"Received data: {stock_data}")
            self.process_message(stock_data)

# Example usage
if __name__ == "__main__":
    kafka_topic = "stock"
    kafka_group = "analyze"
    kafka_servers = ["localhost:9092"]
    db_url = "postgresql://postgres:postgres@localhost:5432/postgres"

    service = AnalyzeService(kafka_topic, kafka_group, kafka_servers, db_url)
    service.start()
