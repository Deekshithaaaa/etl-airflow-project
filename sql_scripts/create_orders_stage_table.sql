CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR PRIMARY KEY,
    product_id VARCHAR,
    customer_id VARCHAR,
    quantity INTEGER,
    order_date TIMESTAMP
);
