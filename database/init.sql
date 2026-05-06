CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    cash_amount NUMERIC(12, 4),
    currency VARCHAR(10),
    declaration_date DATE,
    dividend_type VARCHAR(50),
    ex_dividend_date DATE,
    frequency INTEGER,
    pay_date DATE,
    record_date DATE,
    prediction NUMERIC(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TO DO: Update table schema