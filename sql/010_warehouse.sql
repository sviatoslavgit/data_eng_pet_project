CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT,
    purchased_at TIMESTAMP NOT NULL,
    approved_at TIMESTAMP,
    delivered_carrier_at TIMESTAMP,
    delivered_customer_at TIMESTAMP,
    estimated_delivery_at TIMESTAMP,
    source_batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT,
    source_batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_category_name_english TEXT,
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    source_batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT,
    source_batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    order_id TEXT NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_at TIMESTAMP,
    price NUMERIC(12, 2),
    freight_value NUMERIC(12, 2),
    source_batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_payments (
    order_id TEXT NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value NUMERIC(12, 2),
    source_batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS staging.stg_reviews (
    review_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_at TIMESTAMP,
    review_answer_at TIMESTAMP,
    source_batch_id BIGINT NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT,
    source_batch_id BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_category_name_english TEXT,
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    source_batch_id BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_seller (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT,
    source_batch_id BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_id TEXT NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_at TIMESTAMP,
    price NUMERIC(12, 2),
    freight_value NUMERIC(12, 2),
    source_batch_id BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT,
    purchased_at TIMESTAMP NOT NULL,
    approved_at TIMESTAMP,
    delivered_carrier_at TIMESTAMP,
    delivered_customer_at TIMESTAMP,
    estimated_delivery_at TIMESTAMP,
    delivery_days NUMERIC(10, 2),
    order_item_count INTEGER NOT NULL,
    distinct_product_count INTEGER NOT NULL,
    total_item_value NUMERIC(12, 2),
    total_freight_value NUMERIC(12, 2),
    total_payment_value NUMERIC(12, 2),
    avg_review_score NUMERIC(3, 2),
    review_count INTEGER NOT NULL,
    source_batch_id BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.etl_watermarks (
    pipeline_name TEXT PRIMARY KEY,
    last_order_purchase_timestamp TIMESTAMP NOT NULL,
    last_order_id TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.etl_batch_log (
    batch_id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    batch_size INTEGER NOT NULL,
    source_min_purchase_ts TIMESTAMP,
    source_max_purchase_ts TIMESTAMP,
    source_min_order_id TEXT,
    source_max_order_id TEXT,
    staging_orders_loaded INTEGER DEFAULT 0,
    staging_items_loaded INTEGER DEFAULT 0,
    fact_orders_upserted INTEGER DEFAULT 0,
    fact_items_upserted INTEGER DEFAULT 0,
    message TEXT
);

CREATE INDEX IF NOT EXISTS idx_stg_orders_purchased_at ON staging.stg_orders (purchased_at, order_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_purchased_at ON warehouse.fact_orders (purchased_at, order_id);
CREATE INDEX IF NOT EXISTS idx_batch_log_pipeline_started ON warehouse.etl_batch_log (pipeline_name, started_at);
