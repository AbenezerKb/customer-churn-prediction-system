CREATE TABLE device (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
    handsets INTEGER,
    handset_models INTEGER,
    current_equipment_days INTEGER,
    handset_refurbished BOOLEAN,
    handset_web_capable BOOLEAN,
    handset_price NUMERIC(10,2),
    activation_date DATE,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);