CREATE TABLE customer (
    id SERIAL PRIMARY KEY,
    months_in_service INTEGER,
    unique_subs INTEGER,
    active_subs INTEGER,
    service_area VARCHAR(50),
    retention_calls INTEGER,
    retention_offers_accepted INTEGER,
    new_cellphone_user BOOLEAN,
    referrals_made_by_subscriber INTEGER,
    adjustments_to_credit_rating INTEGER,
    made_call_to_retention_team BOOLEAN,
    credit_rating VARCHAR(10),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);