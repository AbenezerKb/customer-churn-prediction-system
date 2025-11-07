CREATE TABLE billing (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    period_date DATE NOT NULL,
    monthly_revenue NUMERIC(10,2),
    total_recurring_charge NUMERIC(10,2),
    perc_change_revenues NUMERIC(5,2), 
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE
);