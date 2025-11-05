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

INSERT INTO device (customer_id, handsets, handset_models, current_equipment_days, handset_refurbished, handset_web_capable, handset_price, activation_date)
VALUES
(1, 1, 1, 180, FALSE, TRUE, 199.99, '2023-01-01'),
(2, 2, 2, 365, TRUE, TRUE, 99.99, '2022-01-01');