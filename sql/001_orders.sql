CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_state TEXT,
    order_status TEXT,
    purchased_at TIMESTAMP,
    delivered_at TIMESTAMP,
    items_count INTEGER,
    total_items_value NUMERIC(12, 2),
    total_freight_value NUMERIC(12, 2),
    total_payment_value NUMERIC(12, 2),
    avg_review_score NUMERIC(3, 2),
    product_categories TEXT
);
