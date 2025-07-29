CREATE TABLE IF NOT EXISTS payments (
    order_id VARCHAR,
    payment_sequential INTEGER,
    payment_type VARCHAR,
    payment_installments INTEGER,
    payment_value NUMERIC,
    PRIMARY KEY (order_id, payment_sequential)
);
