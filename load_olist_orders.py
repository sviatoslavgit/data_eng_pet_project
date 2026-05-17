import os
from io import StringIO

import pandas as pd
import psycopg


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://olist:olist@localhost:5432/olist")


def read_table(conn: psycopg.Connection, table_name: str, columns: list[str] | None = None) -> pd.DataFrame:
    selected_columns = "*" if columns is None else ", ".join(columns)
    query = f"SELECT {selected_columns} FROM raw.{table_name}"
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [description.name for description in cursor.description]

    return pd.DataFrame(rows, columns=column_names)


def build_orders(conn: psycopg.Connection) -> pd.DataFrame:
    orders = read_table(conn, "olist_orders_dataset")
    customers = read_table(conn, "olist_customers_dataset", ["customer_id", "customer_state"])

    items = read_table(conn, "olist_order_items_dataset").merge(
        read_table(conn, "olist_products_dataset", ["product_id", "product_category_name"]),
        on="product_id",
        how="left",
    )
    items["price"] = pd.to_numeric(items["price"], errors="coerce")
    items["freight_value"] = pd.to_numeric(items["freight_value"], errors="coerce")
    items = items.groupby("order_id").agg(
        items_count=("order_item_id", "count"),
        total_items_value=("price", "sum"),
        total_freight_value=("freight_value", "sum"),
        product_categories=("product_category_name", lambda x: ", ".join(sorted(x.dropna().unique()))),
    )
    payments_data = read_table(conn, "olist_order_payments_dataset", ["order_id", "payment_value"])
    payments_data["payment_value"] = pd.to_numeric(payments_data["payment_value"], errors="coerce")
    payments = payments_data.groupby("order_id", as_index=True)["payment_value"].sum()

    reviews_data = read_table(conn, "olist_order_reviews_dataset", ["order_id", "review_score"])
    reviews_data["review_score"] = pd.to_numeric(reviews_data["review_score"], errors="coerce")
    reviews = reviews_data.groupby("order_id", as_index=True)["review_score"].mean()

    df = orders.merge(customers, on="customer_id", how="left").join(items, on="order_id")
    df = df.join(payments.rename("total_payment_value"), on="order_id")
    df = df.join(reviews.rename("avg_review_score"), on="order_id")
    df = df.rename(columns={
        "order_purchase_timestamp": "purchased_at",
        "order_delivered_customer_date": "delivered_at",
    })
    df["items_count"] = df["items_count"].astype("Int64")
    return df[[
        "order_id", "customer_id", "customer_state", "order_status", "purchased_at",
        "delivered_at", "items_count", "total_items_value", "total_freight_value",
        "total_payment_value", "avg_review_score", "product_categories",
    ]]


def main() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        df = build_orders(conn)
        df = df.where(pd.notna(df), None)
        csv = StringIO()
        df.to_csv(csv, index=False, header=False)
        csv.seek(0)

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
