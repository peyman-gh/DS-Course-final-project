-- Create the stock market table
CREATE TABLE IF NOT EXISTS stock_market_data (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,        -- Stock ticker symbol (e.g., AAPL, TSLA)
    date DATE NOT NULL,                 -- Date of the record
    opening_price DECIMAL(10, 2),       -- Opening price of the stock
    closing_price DECIMAL(10, 2),       -- Closing price of the stock
    high_price DECIMAL(10, 2),          -- Highest price of the stock on the given day
    low_price DECIMAL(10, 2),           -- Lowest price of the stock on the given day
    volume BIGINT,                      -- Trading volume for the day
    market_cap DECIMAL(15, 2),          -- Market capitalization
    CONSTRAINT unique_stock_day UNIQUE(ticker, date) -- Ensure no duplicates for ticker-date combination
);

-- Insert some example stock market data into the table
-- Insert more stock market data with different dates
INSERT INTO stock_market_data (ticker, date, opening_price, closing_price, high_price, low_price, volume, market_cap)
VALUES
    ('AAPL', '2025-01-02', 150.20, 153.50, 154.00, 149.80, 95000000, 2470000000000.00),
    ('AAPL', '2025-01-03', 153.80, 157.30, 158.10, 152.90, 92000000, 2485000000000.00),
    ('GOOGL', '2025-01-02', 2780.00, 2795.00, 2805.00, 2765.00, 1300000, 1855000000000.00),
    ('GOOGL', '2025-01-03', 2796.00, 2810.00, 2825.00, 2780.00, 1150000, 1860000000000.00),
    ('AMZN', '2025-01-02', 3520.00, 3550.50, 3570.00, 3500.00, 2400000, 1780000000000.00),
    ('AMZN', '2025-01-03', 3555.00, 3590.00, 3605.00, 3540.00, 2200000, 1790000000000.00),
    ('MSFT', '2025-01-02', 321.00, 325.50, 328.00, 320.00, 19000000, 2415000000000.00),
    ('MSFT', '2025-01-03', 326.00, 330.25, 332.00, 324.00, 20000000, 2425000000000.00),
    ('TSLA', '2025-01-02', 985.00, 990.00, 995.00, 980.00, 3600000, 785000000000.00),
    ('TSLA', '2025-01-03', 992.00, 995.50, 1005.00, 990.00, 3400000, 790000000000.00);


