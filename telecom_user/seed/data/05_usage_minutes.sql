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


INSERT INTO usage_minutes (customer_id, period_date, monthly_minutes, overage_minutes, roaming_calls, perc_change_minutes)
VALUES
(1, '2023-10-01', 500, 50, 10, 10.00),
(2, '2023-10-01', 800, 100, 20, -5.00);