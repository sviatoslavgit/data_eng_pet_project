import argparse
import logging
import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import psycopg
from psycopg import sql


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://olist:olist@localhost:5432/olist")
PIPELINE_NAME = "olist_warehouse_incremental"
MAX_BATCH_SIZE = 5_000
DEFAULT_BATCH_SIZE = int(os.getenv("WAREHOUSE_BATCH_SIZE", MAX_BATCH_SIZE))
WAREHOUSE_DDL = Path(__file__).resolve().parent / "sql/010_warehouse.sql"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def scalar_to_python(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    return value


def rows_from_dataframe(dataframe: pd.DataFrame, columns: list[str]) -> Iterable[tuple]:
    for row in dataframe[columns].itertuples(index=False, name=None):
        yield tuple(scalar_to_python(value) for value in row)


def fetch_dataframe(conn: psycopg.Connection, query: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [description.name for description in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def execute_sql_file(conn: psycopg.Connection, path: Path) -> None:
    with path.open(encoding="utf-8") as sql_file:
        conn.execute(sql_file.read())


def clean_text(series: pd.Series, *, upper: bool = False, lower: bool = False) -> pd.Series:
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.mask(cleaned == "")
    if upper:
        cleaned = cleaned.str.upper()
    if lower:
        cleaned = cleaned.str.lower()
    return cleaned


def to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def to_number(series: pd.Series, *, integer: bool = False) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.astype("Int64") if integer else values


def upsert_dataframe(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    dataframe: pd.DataFrame,
    conflict_columns: list[str],
) -> int:
    if dataframe.empty:
        return 0

    columns = list(dataframe.columns)
    update_columns = [column for column in columns if column not in conflict_columns]
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)

    base_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        placeholders,
    )

    if update_columns:
        upsert_query = base_query + sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in conflict_columns),
            sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
                for column in update_columns
            ),
        )
    else:
        upsert_query = base_query + sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in conflict_columns)
        )

    with conn.cursor() as cursor:
        cursor.executemany(upsert_query, rows_from_dataframe(dataframe, columns))

    return len(dataframe)


def get_watermark(conn: psycopg.Connection) -> tuple[pd.Timestamp, str]:
    dataframe = fetch_dataframe(
        conn,
        """
        SELECT last_order_purchase_timestamp, last_order_id
        FROM warehouse.etl_watermarks
        WHERE pipeline_name = %s
        """,
        (PIPELINE_NAME,),
    )

    if dataframe.empty:
        return pd.Timestamp("1900-01-01"), ""

    return pd.Timestamp(dataframe.loc[0, "last_order_purchase_timestamp"]), dataframe.loc[0, "last_order_id"]


def update_watermark(conn: psycopg.Connection, last_purchase_ts: pd.Timestamp, last_order_id: str) -> None:
    conn.execute(
        """
        INSERT INTO warehouse.etl_watermarks (
            pipeline_name,
            last_order_purchase_timestamp,
            last_order_id,
            updated_at
        )
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (pipeline_name) DO UPDATE SET
            last_order_purchase_timestamp = EXCLUDED.last_order_purchase_timestamp,
            last_order_id = EXCLUDED.last_order_id,
            updated_at = CURRENT_TIMESTAMP
        """,
        (PIPELINE_NAME, last_purchase_ts.to_pydatetime(), last_order_id),
    )


def fetch_order_batch(
    conn: psycopg.Connection,
    last_purchase_ts: pd.Timestamp,
    last_order_id: str,
    batch_size: int,
) -> pd.DataFrame:
    return fetch_dataframe(
        conn,
        """
        SELECT *
        FROM raw.olist_orders_dataset
        WHERE order_purchase_timestamp IS NOT NULL
          AND (
              order_purchase_timestamp::timestamp > %s
              OR (
                  order_purchase_timestamp::timestamp = %s
                  AND order_id > %s
              )
          )
        ORDER BY order_purchase_timestamp::timestamp, order_id
        LIMIT %s
        """,
        (
            last_purchase_ts.to_pydatetime(),
            last_purchase_ts.to_pydatetime(),
            last_order_id,
            batch_size,
        ),
    )


def fetch_related_data(conn: psycopg.Connection, orders: pd.DataFrame) -> dict[str, pd.DataFrame]:
    order_ids = orders["order_id"].dropna().astype(str).tolist()
    customer_ids = orders["customer_id"].dropna().astype(str).unique().tolist()

    customers = fetch_dataframe(
        conn,
        "SELECT * FROM raw.olist_customers_dataset WHERE customer_id = ANY(%s)",
        (customer_ids,),
    )
    items = fetch_dataframe(
        conn,
        "SELECT * FROM raw.olist_order_items_dataset WHERE order_id = ANY(%s)",
        (order_ids,),
    )
    payments = fetch_dataframe(
        conn,
        "SELECT * FROM raw.olist_order_payments_dataset WHERE order_id = ANY(%s)",
        (order_ids,),
    )
    reviews = fetch_dataframe(
        conn,
        "SELECT * FROM raw.olist_order_reviews_dataset WHERE order_id = ANY(%s)",
        (order_ids,),
    )

    product_ids = items["product_id"].dropna().astype(str).unique().tolist() if not items.empty else []
    seller_ids = items["seller_id"].dropna().astype(str).unique().tolist() if not items.empty else []
    categories = []

    products = fetch_dataframe(
        conn,
        "SELECT * FROM raw.olist_products_dataset WHERE product_id = ANY(%s)",
        (product_ids,),
    )
    if not products.empty:
        categories = products["product_category_name"].dropna().astype(str).unique().tolist()

    translations = fetch_dataframe(
        conn,
        "SELECT * FROM raw.product_category_name_translation WHERE product_category_name = ANY(%s)",
        (categories,),
    )
    sellers = fetch_dataframe(
        conn,
        "SELECT * FROM raw.olist_sellers_dataset WHERE seller_id = ANY(%s)",
        (seller_ids,),
    )

    return {
        "customers": customers,
        "items": items,
        "payments": payments,
        "reviews": reviews,
        "products": products,
        "translations": translations,
        "sellers": sellers,
    }


def clean_orders(dataframe: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    orders = pd.DataFrame({
        "order_id": clean_text(dataframe["order_id"]),
        "customer_id": clean_text(dataframe["customer_id"]),
        "order_status": clean_text(dataframe["order_status"], lower=True),
        "purchased_at": to_datetime(dataframe["order_purchase_timestamp"]),
        "approved_at": to_datetime(dataframe["order_approved_at"]),
        "delivered_carrier_at": to_datetime(dataframe["order_delivered_carrier_date"]),
        "delivered_customer_at": to_datetime(dataframe["order_delivered_customer_date"]),
        "estimated_delivery_at": to_datetime(dataframe["order_estimated_delivery_date"]),
        "source_batch_id": batch_id,
    })
    return orders.dropna(subset=["order_id", "customer_id", "purchased_at"])


def clean_customers(dataframe: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    return pd.DataFrame({
        "customer_id": clean_text(dataframe["customer_id"]),
        "customer_unique_id": clean_text(dataframe["customer_unique_id"]),
        "customer_zip_code_prefix": to_number(dataframe["customer_zip_code_prefix"], integer=True),
        "customer_city": clean_text(dataframe["customer_city"], lower=True),
        "customer_state": clean_text(dataframe["customer_state"], upper=True),
        "source_batch_id": batch_id,
    }).dropna(subset=["customer_id"]).drop_duplicates(subset=["customer_id"], keep="last")


def clean_products(products: pd.DataFrame, translations: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    if products.empty:
        return pd.DataFrame()

    products = products.merge(translations, on="product_category_name", how="left")
    return pd.DataFrame({
        "product_id": clean_text(products["product_id"]),
        "product_category_name": clean_text(products["product_category_name"], lower=True),
        "product_category_name_english": clean_text(products["product_category_name_english"], lower=True),
        "product_name_length": to_number(products["product_name_lenght"], integer=True),
        "product_description_length": to_number(products["product_description_lenght"], integer=True),
        "product_photos_qty": to_number(products["product_photos_qty"], integer=True),
        "product_weight_g": to_number(products["product_weight_g"], integer=True),
        "product_length_cm": to_number(products["product_length_cm"], integer=True),
        "product_height_cm": to_number(products["product_height_cm"], integer=True),
        "product_width_cm": to_number(products["product_width_cm"], integer=True),
        "source_batch_id": batch_id,
    }).dropna(subset=["product_id"]).drop_duplicates(subset=["product_id"], keep="last")


def clean_sellers(dataframe: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    return pd.DataFrame({
        "seller_id": clean_text(dataframe["seller_id"]),
        "seller_zip_code_prefix": to_number(dataframe["seller_zip_code_prefix"], integer=True),
        "seller_city": clean_text(dataframe["seller_city"], lower=True),
        "seller_state": clean_text(dataframe["seller_state"], upper=True),
        "source_batch_id": batch_id,
    }).dropna(subset=["seller_id"]).drop_duplicates(subset=["seller_id"], keep="last")


def clean_items(dataframe: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    items = pd.DataFrame({
        "order_id": clean_text(dataframe["order_id"]),
        "order_item_id": to_number(dataframe["order_item_id"], integer=True),
        "product_id": clean_text(dataframe["product_id"]),
        "seller_id": clean_text(dataframe["seller_id"]),
        "shipping_limit_at": to_datetime(dataframe["shipping_limit_date"]),
        "price": to_number(dataframe["price"]),
        "freight_value": to_number(dataframe["freight_value"]),
        "source_batch_id": batch_id,
    })
    return items.dropna(subset=["order_id", "order_item_id"]).drop_duplicates(
        subset=["order_id", "order_item_id"],
        keep="last",
    )


def clean_payments(dataframe: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    payments = pd.DataFrame({
        "order_id": clean_text(dataframe["order_id"]),
        "payment_sequential": to_number(dataframe["payment_sequential"], integer=True),
        "payment_type": clean_text(dataframe["payment_type"], lower=True),
        "payment_installments": to_number(dataframe["payment_installments"], integer=True),
        "payment_value": to_number(dataframe["payment_value"]),
        "source_batch_id": batch_id,
    })
    return payments.dropna(subset=["order_id", "payment_sequential"]).drop_duplicates(
        subset=["order_id", "payment_sequential"],
        keep="last",
    )


def clean_reviews(dataframe: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    reviews = pd.DataFrame({
        "review_id": clean_text(dataframe["review_id"]),
        "order_id": clean_text(dataframe["order_id"]),
        "review_score": to_number(dataframe["review_score"], integer=True),
        "review_comment_title": clean_text(dataframe["review_comment_title"]),
        "review_comment_message": clean_text(dataframe["review_comment_message"]),
        "review_creation_at": to_datetime(dataframe["review_creation_date"]),
        "review_answer_at": to_datetime(dataframe["review_answer_timestamp"]),
        "source_batch_id": batch_id,
    })
    return reviews.dropna(subset=["review_id", "order_id"]).drop_duplicates(
        subset=["review_id", "order_id"],
        keep="last",
    )


def build_fact_orders(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    payments: pd.DataFrame,
    reviews: pd.DataFrame,
    batch_id: int,
) -> pd.DataFrame:
    facts = orders.copy()

    if items.empty:
        item_agg = pd.DataFrame(columns=[
            "order_id",
            "order_item_count",
            "distinct_product_count",
            "total_item_value",
            "total_freight_value",
        ])
    else:
        item_agg = items.groupby("order_id", as_index=False).agg(
            order_item_count=("order_item_id", "count"),
            distinct_product_count=("product_id", "nunique"),
            total_item_value=("price", "sum"),
            total_freight_value=("freight_value", "sum"),
        )

    if payments.empty:
        payment_agg = pd.DataFrame(columns=["order_id", "total_payment_value"])
    else:
        payment_agg = payments.groupby("order_id", as_index=False).agg(
            total_payment_value=("payment_value", "sum")
        )

    if reviews.empty:
        review_agg = pd.DataFrame(columns=["order_id", "avg_review_score", "review_count"])
    else:
        review_agg = reviews.groupby("order_id", as_index=False).agg(
            avg_review_score=("review_score", "mean"),
            review_count=("review_score", "count"),
        )

    facts = facts.merge(item_agg, on="order_id", how="left")
    facts = facts.merge(payment_agg, on="order_id", how="left")
    facts = facts.merge(review_agg, on="order_id", how="left")

    facts["delivery_days"] = (
        facts["delivered_customer_at"] - facts["purchased_at"]
    ).dt.total_seconds() / 86400
    facts["order_item_count"] = facts["order_item_count"].fillna(0).astype("Int64")
    facts["distinct_product_count"] = facts["distinct_product_count"].fillna(0).astype("Int64")
    facts["review_count"] = facts["review_count"].fillna(0).astype("Int64")
    facts["source_batch_id"] = batch_id

    return facts[[
        "order_id",
        "customer_id",
        "order_status",
        "purchased_at",
        "approved_at",
        "delivered_carrier_at",
        "delivered_customer_at",
        "estimated_delivery_at",
        "delivery_days",
        "order_item_count",
        "distinct_product_count",
        "total_item_value",
        "total_freight_value",
        "total_payment_value",
        "avg_review_score",
        "review_count",
        "source_batch_id",
    ]]


def start_batch_log(conn: psycopg.Connection, orders: pd.DataFrame) -> int:
    sorted_orders = orders.sort_values(["order_purchase_timestamp", "order_id"])
    first = sorted_orders.iloc[0]
    last = sorted_orders.iloc[-1]

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO warehouse.etl_batch_log (
                pipeline_name,
                status,
                batch_size,
                source_min_purchase_ts,
                source_max_purchase_ts,
                source_min_order_id,
                source_max_order_id,
                message
            )
            VALUES (%s, 'running', %s, %s, %s, %s, %s, %s)
            RETURNING batch_id
            """,
            (
                PIPELINE_NAME,
                len(orders),
                pd.Timestamp(first["order_purchase_timestamp"]).to_pydatetime(),
                pd.Timestamp(last["order_purchase_timestamp"]).to_pydatetime(),
                first["order_id"],
                last["order_id"],
                "Batch started",
            ),
        )
        return cursor.fetchone()[0]


def complete_batch_log(
    conn: psycopg.Connection,
    batch_id: int,
    staging_orders_loaded: int,
    staging_items_loaded: int,
    fact_orders_upserted: int,
    fact_items_upserted: int,
) -> None:
    conn.execute(
        """
        UPDATE warehouse.etl_batch_log
        SET status = 'completed',
            finished_at = CURRENT_TIMESTAMP,
            staging_orders_loaded = %s,
            staging_items_loaded = %s,
            fact_orders_upserted = %s,
            fact_items_upserted = %s,
            message = 'Batch completed'
        WHERE batch_id = %s
        """,
        (
            staging_orders_loaded,
            staging_items_loaded,
            fact_orders_upserted,
            fact_items_upserted,
            batch_id,
        ),
    )


def fail_batch_log(conn: psycopg.Connection, batch_id: int, message: str) -> None:
    conn.execute(
        """
        UPDATE warehouse.etl_batch_log
        SET status = 'failed',
            finished_at = CURRENT_TIMESTAMP,
            message = %s
        WHERE batch_id = %s
        """,
        (message[:1000], batch_id),
    )


def reset_warehouse(conn: psycopg.Connection) -> None:
    logger.warning("Resetting staging and warehouse tables for a full refresh")
    conn.execute(
        """
        TRUNCATE TABLE
            staging.stg_orders,
            staging.stg_customers,
            staging.stg_products,
            staging.stg_sellers,
            staging.stg_order_items,
            staging.stg_payments,
            staging.stg_reviews,
            warehouse.dim_customer,
            warehouse.dim_product,
            warehouse.dim_seller,
            warehouse.fact_order_items,
            warehouse.fact_orders
        RESTART IDENTITY
        """
    )
    conn.execute(
        "DELETE FROM warehouse.etl_watermarks WHERE pipeline_name = %s",
        (PIPELINE_NAME,),
    )


def process_batch(conn: psycopg.Connection, batch_id: int, raw_orders: pd.DataFrame) -> tuple[int, int, int, int]:
    related = fetch_related_data(conn, raw_orders)

    orders = clean_orders(raw_orders, batch_id)
    customers = clean_customers(related["customers"], batch_id)
    products = clean_products(related["products"], related["translations"], batch_id)
    sellers = clean_sellers(related["sellers"], batch_id)
    items = clean_items(related["items"], batch_id)
    payments = clean_payments(related["payments"], batch_id)
    reviews = clean_reviews(related["reviews"], batch_id)
    fact_orders = build_fact_orders(orders, items, payments, reviews, batch_id)

    staging_orders_loaded = upsert_dataframe(conn, "staging", "stg_orders", orders, ["order_id"])
    upsert_dataframe(conn, "staging", "stg_customers", customers, ["customer_id"])
    upsert_dataframe(conn, "staging", "stg_products", products, ["product_id"])
    upsert_dataframe(conn, "staging", "stg_sellers", sellers, ["seller_id"])
    staging_items_loaded = upsert_dataframe(
        conn,
        "staging",
        "stg_order_items",
        items,
        ["order_id", "order_item_id"],
    )
    upsert_dataframe(conn, "staging", "stg_payments", payments, ["order_id", "payment_sequential"])
    upsert_dataframe(conn, "staging", "stg_reviews", reviews, ["review_id", "order_id"])

    upsert_dataframe(conn, "warehouse", "dim_customer", customers, ["customer_id"])
    upsert_dataframe(conn, "warehouse", "dim_product", products, ["product_id"])
    upsert_dataframe(conn, "warehouse", "dim_seller", sellers, ["seller_id"])
    fact_items_upserted = upsert_dataframe(
        conn,
        "warehouse",
        "fact_order_items",
        items,
        ["order_id", "order_item_id"],
    )
    fact_orders_upserted = upsert_dataframe(conn, "warehouse", "fact_orders", fact_orders, ["order_id"])

    return staging_orders_loaded, staging_items_loaded, fact_orders_upserted, fact_items_upserted


def run_pipeline(batch_size: int, max_batches: int, full_refresh: bool) -> None:
    if batch_size > MAX_BATCH_SIZE:
        raise ValueError(f"Batch size must be <= {MAX_BATCH_SIZE}. Received: {batch_size}")
    if batch_size <= 0:
        raise ValueError("Batch size must be positive")

    with psycopg.connect(DATABASE_URL) as conn:
        execute_sql_file(conn, WAREHOUSE_DDL)
        if full_refresh:
            reset_warehouse(conn)
        conn.commit()

        last_purchase_ts, last_order_id = get_watermark(conn)
        logger.info(
            "Starting incremental warehouse load from watermark purchase_ts=%s order_id=%s batch_size=%s",
            last_purchase_ts,
            last_order_id,
            batch_size,
        )

        batches_processed = 0
        while True:
            raw_orders = fetch_order_batch(conn, last_purchase_ts, last_order_id, batch_size)
            if raw_orders.empty:
                logger.info("No more source records to process")
                break

            batch_id = start_batch_log(conn, raw_orders)
            conn.commit()

            sorted_orders = raw_orders.sort_values(["order_purchase_timestamp", "order_id"])
            next_purchase_ts = pd.Timestamp(sorted_orders.iloc[-1]["order_purchase_timestamp"])
            next_order_id = sorted_orders.iloc[-1]["order_id"]
            logger.info(
                "Batch %s started: records=%s watermark_from=(%s, %s) watermark_to=(%s, %s)",
                batch_id,
                len(raw_orders),
                last_purchase_ts,
                last_order_id,
                next_purchase_ts,
                next_order_id,
            )

            try:
                with conn.transaction():
                    metrics = process_batch(conn, batch_id, raw_orders)
                    update_watermark(conn, next_purchase_ts, next_order_id)
                    complete_batch_log(conn, batch_id, *metrics)

                logger.info(
                    "Batch %s completed: staging_orders=%s staging_items=%s fact_orders=%s fact_items=%s",
                    batch_id,
                    *metrics,
                )
            except Exception as exc:
                conn.rollback()
                fail_batch_log(conn, batch_id, str(exc))
                conn.commit()
                logger.exception("Batch %s failed", batch_id)
                raise

            last_purchase_ts, last_order_id = next_purchase_ts, next_order_id
            batches_processed += 1
            if max_batches and batches_processed >= max_batches:
                logger.info("Stopped after max_batches=%s", max_batches)
                break

        logger.info("Warehouse load finished: batches_processed=%s", batches_processed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally build the Olist warehouse from raw PostgreSQL tables.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Records per batch. Hard max: 5000.")
    parser.add_argument("--max-batches", type=int, default=0, help="Optional limit for testing or scheduled runs.")
    parser.add_argument(
        "--full-refresh",
        nargs="?",
        const=True,
        default=False,
        type=parse_bool,
        help="Reset staging/warehouse tables and rebuild from raw. Accepts true/false for Databricks job parameters.",
    )
    return parser.parse_args()


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False

    raise argparse.ArgumentTypeError(f"Expected a boolean value, got: {value}")


def main() -> None:
    args = parse_args()
    run_pipeline(args.batch_size, args.max_batches, args.full_refresh)


if __name__ == "__main__":
    main()
