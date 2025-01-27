from fastapi import FastAPI, HTTPException
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json
from decimal import Decimal
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field
import os
from functools import wraps

# Database connection pool
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

# Initialize FastAPI app
app = FastAPI(title="Stock Market API")

# Initialize Redis connection
redis_client = redis.from_url(REDIS_URL)
CACHE_TTL = 300  # Cache time to live in seconds (5 minutes)

# Pydantic models for response serialization
class MarketCap(BaseModel):
    stock_symbol: str
    market_cap: float
    timestamp: datetime

class StockPrice(BaseModel):
    stock_symbol: str
    opening_price: float
    closing_price: float
    high: float
    low: float
    volume: int
    timestamp: datetime

class NewsSentiment(BaseModel):
    stock_symbol: str
    sentiment_score: float
    sentiment_magnitude: float
    timestamp: datetime

class EconomicIndicator(BaseModel):
    indicator_name: str
    value: float
    timestamp: datetime

class OrderBookEntry(BaseModel):
    price: float
    quantity: int
    order_type: str

class OrderBook(BaseModel):
    stock_symbol: str
    timestamp: datetime
    bids: List[OrderBookEntry]
    asks: List[OrderBookEntry]

# Cache decorator
def cache_response(prefix: str, expire_time: int = CACHE_TTL):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Create cache key from prefix and function arguments
            cache_key = f"{prefix}:" + ":".join(str(arg) for arg in args[1:])

            # Try to get cached response
            cached_response = redis_client.get(cache_key)
            if cached_response:
                return json.loads(cached_response)

            # Get fresh response
            response = await func(*args, **kwargs)

            # Cache the response
            redis_client.setex(
                cache_key,
                expire_time,
                json.dumps(response, default=str)
            )

            return response
        return wrapper
    return decorator

# Database connection helper
def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

@app.get("/market-cap/{stock_symbol}", response_model=MarketCap)
@cache_response(prefix="market_cap")
async def get_market_cap(stock_symbol: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT stock_symbol, market_cap, timestamp
                FROM market_data
                WHERE stock_symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (stock_symbol,))

            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Stock not found")

            return result

@app.get("/stock-price/{stock_symbol}", response_model=StockPrice)
@cache_response(prefix="stock_price")
async def get_stock_price(stock_symbol: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT stock_symbol, opening_price, closing_price, high, low, volume, timestamp
                FROM stock_prices
                WHERE stock_symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (stock_symbol,))

            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Stock price not found")

            return result

@app.get("/news-sentiment/{stock_symbol}", response_model=NewsSentiment)
@cache_response(prefix="news_sentiment")
async def get_news_sentiment(stock_symbol: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT stock_symbol, sentiment_score, sentiment_magnitude, timestamp
                FROM news_sentiment
                WHERE stock_symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (stock_symbol,))

            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="News sentiment not found")

            return result

@app.get("/economic-indicator/{indicator_name}", response_model=EconomicIndicator)
@cache_response(prefix="economic_indicator")
async def get_economic_indicator(indicator_name: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator_name, value, timestamp
                FROM economic_indicators
                WHERE indicator_name = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (indicator_name,))

            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Economic indicator not found")

            return result

@app.get("/order-book/{stock_symbol}", response_model=OrderBook)
@cache_response(prefix="order_book")
async def get_order_book(stock_symbol: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get the latest timestamp for the stock's order book
            cur.execute("""
                SELECT timestamp
                FROM order_book
                WHERE stock_symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (stock_symbol,))

            latest_timestamp = cur.fetchone()
            if not latest_timestamp:
                raise HTTPException(status_code=404, detail="Order book not found")

            # Get bids and asks
            cur.execute("""
                SELECT price, quantity, order_type
                FROM order_book
                WHERE stock_symbol = %s
                AND timestamp = %s
                ORDER BY 
                    CASE 
                        WHEN order_type = 'buy' THEN price END DESC,
                    CASE 
                        WHEN order_type = 'sell' THEN price END ASC
            """, (stock_symbol, latest_timestamp['timestamp']))

            orders = cur.fetchall()

            # Separate bids and asks
            bids = [order for order in orders if order['order_type'] == 'buy']
            asks = [order for order in orders if order['order_type'] == 'sell']

            return {
                "stock_symbol": stock_symbol,
                "timestamp": latest_timestamp['timestamp'],
                "bids": bids,
                "asks": asks
            }