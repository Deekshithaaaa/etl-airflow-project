CREATE TABLE IF NOT EXISTS users (
    user_name VARCHAR PRIMARY KEY,
    email VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    signup_date TIMESTAMP
);
