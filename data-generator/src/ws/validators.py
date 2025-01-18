from pydantic import BaseModel, Field, validator
from typing import Literal, Union
from datetime import datetime
import time

class StockPrice(BaseModel):
    data_type: Literal["stock_price"]
    stock_symbol: str
    opening_price: float = Field(ge=0)  # must be greater than or equal to 0
    closing_price: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    volume: int = Field(ge=0)
    timestamp: float

    @validator('high')
    def high_must_be_highest(cls, v, values):
        if 'opening_price' in values and 'closing_price' in values:
            if v < max(values['opening_price'], values['closing_price']):
                raise ValueError('high must be higher than opening and closing prices')
        return v

    @validator('low')
    def low_must_be_lowest(cls, v, values):
        if 'opening_price' in values and 'closing_price' in values:
            if v > min(values['opening_price'], values['closing_price']):
                raise ValueError('low must be lower than opening and closing prices')
        return v

    @validator('timestamp')
    def validate_timestamp(cls, v):
        # Check if timestamp is within reasonable range (e.g., last 24 hours)
        current_time = time.time()
        if v > current_time + 86400:  # 24 hours in future
            raise ValueError('timestamp too far in future')
        if v < current_time - 86400:  # 24 hours in past
            raise ValueError('timestamp too old')
        return v

class OrderBook(BaseModel):
    data_type: Literal["order_book"]
    timestamp: float
    stock_symbol: str
    order_type: Literal["buy", "sell"]
    price: float = Field(ge=0)
    quantity: int = Field(ge=1)  # must be at least 1

class NewsSentiment(BaseModel):
    data_type: Literal["news_sentiment"]
    timestamp: float
    stock_symbol: str
    sentiment_score: float = Field(ge=-1, le=1)  # between -1 and 1
    sentiment_magnitude: float = Field(ge=0, le=1)  # between 0 and 1

class MarketData(BaseModel):
    data_type: Literal["market_data"]
    timestamp: float
    stock_symbol: str
    market_cap: float = Field(ge=0)
    pe_ratio: float = Field(ge=0)

class EconomicIndicator(BaseModel):
    data_type: Literal["economic_indicator"]
    timestamp: float
    indicator_name: Literal["GDP Growth Rate"]
    value: float = Field(ge=-20, le=20)  # reasonable range for GDP growth

# Union of all possible data types
MarketMessage = Union[StockPrice, OrderBook, NewsSentiment, MarketData, EconomicIndicator]

def validate_market_data(data: dict) -> MarketMessage:
    """
    Validate incoming market data based on its data_type
    Returns the validated model or raises ValidationError
    """
    data_type = data.get('data_type')
    if data_type == "stock_price":
        return StockPrice.parse_obj(data)
    elif data_type == "order_book":
        return OrderBook.parse_obj(data)
    elif data_type == "news_sentiment":
        return NewsSentiment.parse_obj(data)
    elif data_type == "market_data":
        return MarketData.parse_obj(data)
    elif data_type == "economic_indicator":
        return EconomicIndicator.parse_obj(data)
    else:
        raise ValueError(f"Unknown data_type: {data_type}")

# Example usage
if __name__ == "__main__":
    # Example valid data
    stock_data = {
        "data_type": "stock_price",
        "stock_symbol": "AAPL",
        "opening_price": 150.0,
        "closing_price": 151.0,
        "high": 152.0,
        "low": 149.0,
        "volume": 1000000,
        "timestamp": time.time()
    }

    try:
        validated_data = validate_market_data(stock_data)
        print(f"Validated data: {validated_data}")
    except Exception as e:
        print(f"Validation error: {e}")