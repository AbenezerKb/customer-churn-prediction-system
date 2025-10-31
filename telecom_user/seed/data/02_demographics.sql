CREATE TABLE demographics (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
    age_hh1 INTEGER,
    age_hh2 INTEGER,
    children_in_hh INTEGER,
    truck_owner BOOLEAN,
    rv_owner BOOLEAN,
    homeownership VARCHAR(20),
    buys_via_mail_order BOOLEAN,
    responds_to_mail_offers BOOLEAN,
    opt_out_mailings BOOLEAN,
    non_us_travel BOOLEAN,
    owns_computer BOOLEAN,
    has_credit_card BOOLEAN,
    income_group INTEGER,
    owns_motorcycle BOOLEAN,
    prizm_code VARCHAR(50),
    occupation VARCHAR(50),
    marital_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);


INSERT INTO demographics (customer_id, age_hh1, age_hh2, children_in_hh, truck_owner, rv_owner, homeownership, buys_via_mail_order, responds_to_mail_offers, opt_out_mailings, non_us_travel, owns_computer, has_credit_card, income_group, owns_motorcycle, prizm_code, occupation, marital_status)
VALUES
(1, 35, 32, 2, FALSE, FALSE, 'Owner', TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, 5, FALSE, 'Urban', 'Professional', 'Married'),
(2, 45, NULL, 0, TRUE, FALSE, 'Renter', FALSE, FALSE, TRUE, FALSE, TRUE, TRUE, 7, TRUE, 'Suburban', 'Manager', 'Single');