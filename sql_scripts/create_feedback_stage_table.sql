CREATE TABLE IF NOT EXISTS feedback (
    feedback_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    customer_id VARCHAR,
    rating INTEGER,
    comments TEXT,
    feedback_date TIMESTAMP
);
