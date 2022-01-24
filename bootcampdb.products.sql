-- Table: bootcampdb.user_purchases

CREATE SCHEMA IF NOT EXISTS bootcampdb;

CREATE TABLE IF NOT EXISTS bootcampdb.user_purchases (
    invoice_number VARCHAR(10),
    stock_code VARCHAR(20),
    detail VARCHAR(1000),
    quantity BIGINT,
    invoice_date timestamp,
    unit_price NUMERIC(8,3),
    customer_id VARCHAR(20),
    country VARCHAR(20)
);
