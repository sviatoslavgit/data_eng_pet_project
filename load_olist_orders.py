import os
from io import StringIO

import pandas as pd
import psycopg


DATA_FILE = "data/olist_dataset.pkl"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://olist:olist@localhost:5432/olist")


def build_orders() -> pd.DataFrame:
    t = pd.read_pickle(DATA_FILE)
    orders = t["olist_orders_dataset"]
    customers = t["olist_customers_dataset"][["customer_id", "customer_state"]]

    items = t["olist_order_items_dataset"].merge(
        t["olist_products_dataset"][["product_id", "product_category_name"]],
        on="product_id",
        how="left",
    )
    items = items.groupby("order_id").agg(
        items_count=("order_item_id", "count"),
        total_items_value=("price", "sum"),
        total_freight_value=("freight_value", "sum"),
        product_categories=("product_category_name", lambda x: ", ".join(sorted(x.dropna().unique()))),
    )
    payments = t["olist_order_payments_dataset"].groupby("order_id", as_index=True)["payment_value"].sum()
    reviews = t["olist_order_reviews_dataset"].groupby("order_id", as_index=True)["review_score"].mean()

    df = orders.merge(customers, on="customer_id", how="left").join(items, on="order_id")
    df = df.join(payments.rename("total_payment_value"), on="order_id")
    df = df.join(reviews.rename("avg_review_score"), on="order_id")
    df = df.rename(columns={
        "order_purchase_timestamp": "purchased_at",
        "order_delivered_customer_date": "delivered_at",
    })
    return df[[
        "order_id", "customer_id", "customer_state", "order_status", "purchased_at",
        "delivered_at", "items_count", "total_items_value", "total_freight_value",
        "total_payment_value", "avg_review_score", "product_categories",
    ]]


def main() -> None:
    df = build_orders()
    df = df.where(pd.notna(df), None)
    csv = StringIO()
    df.to_csv(csv, index=False, header=False)
    csv.seek(0)

    with psycopg.connect(DATABASE_URL) as conn:
        with open("sql/001_orders.sql", encoding="utf-8") as sql:
            conn.execute(sql.read())
        conn.execute("TRUNCATE orders")
        with conn.cursor().copy(
            "COPY orders FROM STDIN WITH (FORMAT CSV)"
        ) as copy:
            copy.write(csv.getvalue())

    print(f"Loaded {len(df)} orders")


if __name__ == "__main__":
    main()
