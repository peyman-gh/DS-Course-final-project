import json
import psycopg2
from datetime import datetime
from typing import Dict, Any
import logging
from psycopg2.extras import execute_batch


class MarketDataInserter:
    def __init__(self, db_params: Dict[str, str]):
        """
        Initialize database connection

        :param db_params: Dictionary containing database connection parameters
        """
        self.db_params = db_params
        self.conn = None
        self.cursor = None

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cursor = self.conn.cursor()
            self.logger.info("Successfully connected to the database")
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")

    def convert_timestamp(self, timestamp: float) -> str:
        """Convert Unix timestamp to PostgreSQL timestamp string"""
        return datetime.fromtimestamp(timestamp).isoformat()

    def insert_data(self, data: Dict[str, Any]):
        """
        Insert data based on data_type

        :param data: Dictionary containing the data to insert
        """
        data_type = data.get('data_type')

        try:
            if data_type == 'stock_price':
                self._insert_stock_price(data)
            elif data_type == 'market_data':
                self._insert_market_data(data)
            elif data_type == 'economic_indicator':
                self._insert_economic_indicator(data)
            elif data_type == 'news_sentiment':
                self._insert_news_sentiment(data)
            elif data_type == 'order_book':
                self._insert_order_book(data)
            else:
                self.logger.warning(f"Unknown data type: {data_type}")
                return

            self.conn.commit()
            self.logger.info(f"Successfully inserted {data_type} data")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting {data_type} data: {e}")
            raise

    def _insert_stock_price(self, data: Dict[str, Any]):
        """Insert stock price data"""
        query = """
        INSERT INTO stock_prices 
        (stock_symbol, opening_price, closing_price, high, low, volume, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_symbol, timestamp) 
        DO UPDATE SET
            opening_price = EXCLUDED.opening_price,
            closing_price = EXCLUDED.closing_price,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            volume = EXCLUDED.volume;
        """
        values = (
            data['stock_symbol'],
            data['opening_price'],
            data['closing_price'],
            data['high'],
            data['low'],
            data['volume'],
            self.convert_timestamp(data['timestamp'])
        )
        self.cursor.execute(query, values)

    def _insert_market_data(self, data: Dict[str, Any]):
        """Insert market data"""
        query = """
        INSERT INTO market_data 
        (stock_symbol, market_cap, pe_ratio, timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (stock_symbol, timestamp) 
        DO UPDATE SET
            market_cap = EXCLUDED.market_cap,
            pe_ratio = EXCLUDED.pe_ratio;
        """
        values = (
            data['stock_symbol'],
            data['market_cap'],
            data['pe_ratio'],
            self.convert_timestamp(data['timestamp'])
        )
        self.cursor.execute(query, values)

    def _insert_economic_indicator(self, data: Dict[str, Any]):
        """Insert economic indicator data"""
        query = """
        INSERT INTO economic_indicators 
        (indicator_name, value, timestamp)
        VALUES (%s, %s, %s)
        ON CONFLICT (indicator_name, timestamp) 
        DO UPDATE SET
            value = EXCLUDED.value;
        """
        values = (
            data['indicator_name'],
            data['value'],
            self.convert_timestamp(data['timestamp'])
        )
        self.cursor.execute(query, values)

    def _insert_news_sentiment(self, data: Dict[str, Any]):
        """Insert news sentiment data"""
        query = """
        INSERT INTO news_sentiment 
        (stock_symbol, sentiment_score, sentiment_magnitude, timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (stock_symbol, timestamp) 
        DO UPDATE SET
            sentiment_score = EXCLUDED.sentiment_score,
            sentiment_magnitude = EXCLUDED.sentiment_magnitude;
        """
        values = (
            data['stock_symbol'],
            data['sentiment_score'],
            data['sentiment_magnitude'],
            self.convert_timestamp(data['timestamp'])
        )
        self.cursor.execute(query, values)

    def _insert_order_book(self, data: Dict[str, Any]):
        """Insert order book data"""
        query = """
        INSERT INTO order_book 
        (stock_symbol, order_type, price, quantity, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            data['stock_symbol'],
            data['order_type'],
            data['price'],
            data['quantity'],
            self.convert_timestamp(data['timestamp'])
        )
        self.cursor.execute(query, values)

global inserter

# Usage example
if __name__ == "__main__":
    # Database connection parameters
    db_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5433"
    }

    # Sample data
    # sample_data = [
    #     {
    #         'data_type': 'stock_price',
    #         'stock_symbol': 'AAPL',
    #         'opening_price': 1003.6601837217902,
    #         'closing_price': 994.0301837217902,
    #         'high': 1011.3101837217902,
    #         'low': 988.1401837217902,
    #         'volume': 4847,
    #         'timestamp': 1737992487.2368355
    #     },
    #     {
    #         'data_type': 'market_data',
    #         'timestamp': 1737962491.5765667,
    #         'stock_symbol': 'AAPL',
    #         'market_cap': 138124619440.0391,
    #         'pe_ratio': 29.051080989097763
    #     },
    #     {
    #         'data_type': 'economic_indicator',
    #         'timestamp': 1737992487.6426015,
    #         'indicator_name': 'GDP Growth Rate',
    #         'value': 5.6892213848529725
    #     },
    #     {
    #         'data_type': 'news_sentiment',
    #         'timestamp': 1737992487.8027065,
    #         'stock_symbol': 'AMZN',
    #         'sentiment_score': 0.14165295211795724,
    #         'sentiment_magnitude': 0.6895532226326736
    #     }
    # ]
    pass

    # try:
    #     # Initialize the inserter
    #     inserter = MarketDataInserter(db_params)
    #     inserter.connect()
    #
    #     # Insert each data point
    #     for data in sample_data:
    #         inserter.insert_data(data)
    #
    # except Exception as e:
    #     print(f"An error occurred: {e}")
    #
    # finally:
    #     inserter.disconnect()