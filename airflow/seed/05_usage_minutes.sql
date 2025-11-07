CREATE TABLE usage_minutes (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    period_date DATE NOT NULL,
    monthly_minutes INTEGER,
    overage_minutes INTEGER,
    roaming_calls INTEGER,
    perc_change_minutes NUMERIC(5,2), 
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE
);