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