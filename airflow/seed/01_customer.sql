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

INSERT INTO customer (months_in_service, unique_subs, active_subs, service_area, retention_calls, retention_offers_accepted, new_cellphone_user, referrals_made_by_subscriber, adjustments_to_credit_rating, made_call_to_retention_team, credit_rating)
VALUES
(12, 1, 1, 'NYC001', 0, 0, TRUE, 0, 0, FALSE, 'A'),
(24, 2, 2, 'LAX002', 1, 1, FALSE, 2, 1, TRUE, 'B');