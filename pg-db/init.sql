-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Create enums and types
CREATE TYPE data_type AS ENUM ('stock_price', 'market_data', 'economic_indicator', 'news_sentiment', 'order_book');
CREATE TYPE order_type AS ENUM ('buy', 'sell');

-- Create base tables
CREATE TABLE stocks (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stock_prices (
    id BIGSERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) REFERENCES stocks(symbol),
    opening_price DECIMAL(12,4),
    closing_price DECIMAL(12,4),
    high DECIMAL(12,4),
    low DECIMAL(12,4),
    volume INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_price_entry UNIQUE (stock_symbol, timestamp)
);

CREATE TABLE market_data (
    id BIGSERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) REFERENCES stocks(symbol),
    market_cap DECIMAL(20,4),
    pe_ratio DECIMAL(10,4),
    timestamp TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_market_data_entry UNIQUE (stock_symbol, timestamp)
);

CREATE TABLE economic_indicators (
    id BIGSERIAL PRIMARY KEY,
    indicator_name VARCHAR(50),
    value DECIMAL(12,4),
    timestamp TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_indicator_entry UNIQUE (indicator_name, timestamp)
);

CREATE TABLE news_sentiment (
    id BIGSERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) REFERENCES stocks(symbol),
    sentiment_score DECIMAL(5,4),
    sentiment_magnitude DECIMAL(5,4),
    timestamp TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_sentiment_entry UNIQUE (stock_symbol, timestamp)
);

CREATE TABLE order_book (
    id BIGSERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) REFERENCES stocks(symbol),
    order_type order_type NOT NULL,
    price DECIMAL(12,4) NOT NULL,
    quantity INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_quantity CHECK (quantity > 0),
    CONSTRAINT valid_price CHECK (price > 0)
);

CREATE TABLE technical_indicators (
    id BIGSERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) REFERENCES stocks(symbol),
    indicator_type VARCHAR(50),  -- 'MA', 'EMA', 'RSI'
    period INTEGER,  -- number of periods used in calculation
    value DECIMAL(12,4),
    timestamp TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_technical_indicator UNIQUE (stock_symbol, indicator_type, period, timestamp)
);

CREATE TABLE calculation_logs (
    id BIGSERIAL PRIMARY KEY,
    calculation_type VARCHAR(50),
    status VARCHAR(20),
    message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_stock_prices_symbol_timestamp ON stock_prices(stock_symbol, timestamp);
CREATE INDEX idx_market_data_symbol_timestamp ON market_data(stock_symbol, timestamp);
CREATE INDEX idx_economic_indicators_timestamp ON economic_indicators(timestamp);
CREATE INDEX idx_news_sentiment_symbol_timestamp ON news_sentiment(stock_symbol, timestamp);
CREATE INDEX idx_technical_indicators_symbol_type ON technical_indicators(stock_symbol, indicator_type);
CREATE INDEX idx_calculation_logs_type_status ON calculation_logs(calculation_type, status);
-- New indexes for order book
CREATE INDEX idx_order_book_symbol_timestamp ON order_book(stock_symbol, timestamp);
CREATE INDEX idx_order_book_type_price ON order_book(order_type, price);

-- Create update timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers
CREATE TRIGGER update_stocks_updated_at
    BEFORE UPDATE ON stocks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create technical indicator calculation functions
CREATE OR REPLACE FUNCTION calculate_moving_average(
    p_symbol VARCHAR,
    p_period INTEGER,
    p_end_date TIMESTAMP WITH TIME ZONE
)
RETURNS DECIMAL AS $$
BEGIN
    RETURN (
        SELECT AVG(closing_price)
        FROM (
            SELECT closing_price
            FROM stock_prices
            WHERE stock_symbol = p_symbol
            AND timestamp <= p_end_date
            ORDER BY timestamp DESC
            LIMIT p_period
        ) t
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION calculate_ema(
    p_symbol VARCHAR,
    p_period INTEGER,
    p_end_date TIMESTAMP WITH TIME ZONE
)
RETURNS DECIMAL AS $$
DECLARE
    v_smoothing DECIMAL := 2.0 / (p_period + 1);
    v_previous_ema DECIMAL;
    v_current_price DECIMAL;
BEGIN
    -- Get the current price
    SELECT closing_price INTO v_current_price
    FROM stock_prices
    WHERE stock_symbol = p_symbol
    AND timestamp <= p_end_date
    ORDER BY timestamp DESC
    LIMIT 1;

    -- Get the previous EMA
    SELECT value INTO v_previous_ema
    FROM technical_indicators
    WHERE stock_symbol = p_symbol
    AND indicator_type = 'EMA'
    AND period = p_period
    AND timestamp < p_end_date
    ORDER BY timestamp DESC
    LIMIT 1;

    -- If no previous EMA exists, use SMA as initial value
    IF v_previous_ema IS NULL THEN
        v_previous_ema := calculate_moving_average(p_symbol, p_period, p_end_date);
    END IF;

    RETURN (v_current_price * v_smoothing) + (v_previous_ema * (1 - v_smoothing));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION calculate_rsi(
    p_symbol VARCHAR,
    p_period INTEGER,
    p_end_date TIMESTAMP WITH TIME ZONE
)
RETURNS DECIMAL AS $$
DECLARE
    v_avg_gain DECIMAL;
    v_avg_loss DECIMAL;
    v_rsi DECIMAL;
BEGIN
    WITH price_changes AS (
        SELECT
            closing_price - LAG(closing_price) OVER (ORDER BY timestamp) as change
        FROM stock_prices
        WHERE stock_symbol = p_symbol
        AND timestamp <= p_end_date
        ORDER BY timestamp DESC
        LIMIT p_period + 1
    ),
    gains_losses AS (
        SELECT
            COALESCE(AVG(CASE WHEN change > 0 THEN change ELSE 0 END), 0) as avg_gain,
            COALESCE(AVG(CASE WHEN change < 0 THEN ABS(change) ELSE 0 END), 0) as avg_loss
        FROM price_changes
    )
    SELECT
        avg_gain,
        avg_loss
    INTO
        v_avg_gain,
        v_avg_loss
    FROM gains_losses;

    IF v_avg_loss = 0 THEN
        RETURN 100;
    ELSE
        v_rsi := 100 - (100 / (1 + (v_avg_gain / v_avg_loss)));
        RETURN v_rsi;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create main calculation function
CREATE OR REPLACE FUNCTION calculate_and_store_indicators()
RETURNS void AS $$
DECLARE
    v_stock RECORD;
    v_periods INTEGER[] := ARRAY[7, 14, 30];
    v_period INTEGER;
BEGIN
    FOR v_stock IN (SELECT symbol FROM stocks) LOOP
        FOREACH v_period IN ARRAY v_periods LOOP
            -- Insert Moving Average
            INSERT INTO technical_indicators
            (stock_symbol, indicator_type, period, value, timestamp)
            VALUES (
                v_stock.symbol,
                'MA',
                v_period,
                calculate_moving_average(v_stock.symbol, v_period, CURRENT_TIMESTAMP),
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (stock_symbol, indicator_type, period, timestamp)
            DO UPDATE SET value = EXCLUDED.value;

            -- Insert EMA
            INSERT INTO technical_indicators
            (stock_symbol, indicator_type, period, value, timestamp)
            VALUES (
                v_stock.symbol,
                'EMA',
                v_period,
                calculate_ema(v_stock.symbol, v_period, CURRENT_TIMESTAMP),
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (stock_symbol, indicator_type, period, timestamp)
            DO UPDATE SET value = EXCLUDED.value;

            -- Insert RSI
            INSERT INTO technical_indicators
            (stock_symbol, indicator_type, period, value, timestamp)
            VALUES (
                v_stock.symbol,
                'RSI',
                v_period,
                calculate_rsi(v_stock.symbol, v_period, CURRENT_TIMESTAMP),
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (stock_symbol, indicator_type, period, timestamp)
            DO UPDATE SET value = EXCLUDED.value;
        END LOOP;
    END LOOP;

    INSERT INTO calculation_logs (calculation_type, status, message)
    VALUES ('Technical Indicators', 'SUCCESS', 'Calculated indicators for all stocks at ' || CURRENT_TIMESTAMP);

EXCEPTION WHEN OTHERS THEN
    INSERT INTO calculation_logs (calculation_type, status, message)
    VALUES ('Technical Indicators', 'ERROR', 'Error: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Create scheduler management functions
CREATE OR REPLACE FUNCTION update_indicator_calculation_interval(new_interval TEXT)
RETURNS void AS $$
BEGIN
    PERFORM cron.unschedule('calculate_indicators');
    PERFORM cron.schedule('calculate_indicators', new_interval, 'SELECT calculate_and_store_indicators()');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pause_indicator_calculations()
RETURNS void AS $$
BEGIN
    PERFORM cron.unschedule('calculate_indicators');

    INSERT INTO calculation_logs (calculation_type, status, message)
    VALUES ('Technical Indicators', 'INFO', 'Calculations paused at ' || CURRENT_TIMESTAMP);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION resume_indicator_calculations(interval_pattern TEXT DEFAULT '* * * * *')
RETURNS void AS $$
BEGIN
    PERFORM cron.schedule('calculate_indicators', interval_pattern, 'SELECT calculate_and_store_indicators()');

    INSERT INTO calculation_logs (calculation_type, status, message)
    VALUES ('Technical Indicators', 'INFO', 'Calculations resumed at ' || CURRENT_TIMESTAMP);
END;
$$ LANGUAGE plpgsql;

-- Set up the initial schedule
SELECT cron.schedule('calculate_indicators', '* * * * *', 'SELECT calculate_and_store_indicators()');

-- Insert some sample stocks
INSERT INTO stocks (symbol, name) VALUES
    ('AAPL', 'Apple Inc.'),
    ('MSFT', 'Microsoft Corporation'),
    ('GOOGL', 'Alphabet Inc.'),
    ('AMZN', 'Amazon.com Inc.'),
    ('TSLA', 'Tesla')
ON CONFLICT DO NOTHING;