CREATE TABLE call_details (
    id SERIAL,
    customer_id INTEGER NOT NULL,
    period_date DATE NOT NULL,
    dropped_calls INTEGER,
    blocked_calls INTEGER,
    unanswered_calls INTEGER,
    customer_care_calls INTEGER,
    three_way_calls INTEGER,
    received_calls INTEGER,
    outbound_calls INTEGER,
    inbound_calls INTEGER,
    peak_calls_in_out INTEGER,
    off_peak_calls_in_out INTEGER,
    dropped_blocked_calls INTEGER,
    call_forwarding_calls INTEGER,
    call_waiting_calls INTEGER,
    director_assisted_calls INTEGER,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id),
    FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE
);



INSERT INTO call_details (customer_id, period_date, dropped_calls, blocked_calls, unanswered_calls, customer_care_calls, three_way_calls, received_calls, outbound_calls, inbound_calls, peak_calls_in_out, off_peak_calls_in_out, dropped_blocked_calls, call_forwarding_calls, call_waiting_calls, director_assisted_calls)
VALUES
(1, '2023-10-01', 5, 3, 10, 2, 1, 100, 150, 120, 200, 150, 8, 0, 5, 2),
(2, '2023-10-01', 10, 5, 15, 3, 2, 200, 250, 220, 300, 250, 15, 1, 10, 3);


-- CREATE TABLE usage_minutes (
--     id SERIAL PRIMARY KEY,
--     customer_id INTEGER NOT NULL,
--     period_date DATE NOT NULL,
--     monthly_minutes INTEGER,
--     overage_minutes INTEGER,
--     roaming_calls INTEGER,
--     perc_change_minutes NUMERIC(5,2), 
--     created_at TIMESTAMP DEFAULT now(),
--     updated_at TIMESTAMP DEFAULT now(),
--     FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE
-- );



-- CREATE TABLE billing (
--     id SERIAL PRIMARY KEY,
--     customer_id INTEGER NOT NULL,
--     period_date DATE NOT NULL,
--     monthly_revenue NUMERIC(10,2),
--     total_recurring_charge NUMERIC(10,2),
--     perc_change_revenues NUMERIC(5,2), 
--     created_at TIMESTAMP DEFAULT now(),
--     updated_at TIMESTAMP DEFAULT now(),
--     FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE
-- );


-- CREATE TABLE device (
--     id SERIAL PRIMARY KEY,
--     customer_id INTEGER NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
--     handsets INTEGER,
--     handset_models INTEGER,
--     current_equipment_days INTEGER,
--     handset_refurbished BOOLEAN,
--     handset_web_capable BOOLEAN,
--     handset_price NUMERIC(10,2),
--     activation_date DATE,
--     created_at TIMESTAMP DEFAULT now(),
--     updated_at TIMESTAMP DEFAULT now()
-- );



-- CREATE TABLE demographics (
--     id SERIAL PRIMARY KEY,
--     customer_id INTEGER NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
--     age_hh1 INTEGER,
--     age_hh2 INTEGER,
--     children_in_hh INTEGER,
--     truck_owner BOOLEAN,
--     rv_owner BOOLEAN,
--     homeownership VARCHAR(20),
--     buys_via_mail_order BOOLEAN,
--     responds_to_mail_offers BOOLEAN,
--     opt_out_mailings BOOLEAN,
--     non_us_travel BOOLEAN,
--     owns_computer BOOLEAN,
--     has_credit_card BOOLEAN,
--     income_group INTEGER,
--     owns_motorcycle BOOLEAN,
--     prizm_code VARCHAR(50),
--     occupation VARCHAR(50),
--     marital_status VARCHAR(20),
--     created_at TIMESTAMP DEFAULT now(),
--     updated_at TIMESTAMP DEFAULT now()
-- );




-- CREATE TABLE customer (
--     id SERIAL PRIMARY KEY,
--     months_in_service INTEGER,
--     unique_subs INTEGER,
--     active_subs INTEGER,
--     service_area VARCHAR(50),
--     retention_calls INTEGER,
--     retention_offers_accepted INTEGER,
--     new_cellphone_user BOOLEAN,
--     referrals_made_by_subscriber INTEGER,
--     adjustments_to_credit_rating INTEGER,
--     made_call_to_retention_team BOOLEAN,
--     credit_rating VARCHAR(10),
--     created_at TIMESTAMP DEFAULT now(),
--     updated_at TIMESTAMP DEFAULT now()
-- );