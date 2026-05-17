import os
from io import StringIO

import pandas as pd
import psycopg
from psycopg import sql


DATA_FILE = "data/olist_dataset.pkl"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://olist:olist@localhost:5432/olist")
RAW_SCHEMA = "raw"


def create_raw_table(conn: psycopg.Connection, table_name: str, columns: list[str]) -> None:
    column_defs = [
        sql.SQL("{} TEXT").format(sql.Identifier(column))
        for column in columns
    ]

    conn.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(RAW_SCHEMA),
            sql.Identifier(table_name),
        )
    )
    conn.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(RAW_SCHEMA),
            sql.Identifier(table_name),
            sql.SQL(", ").join(column_defs),
        )
    )


def copy_dataframe(conn: psycopg.Connection, table_name: str, dataframe: pd.DataFrame) -> None:
    csv_buffer = StringIO()
    dataframe.where(pd.notna(dataframe), None).to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    column_names = [
        sql.Identifier(column)
        for column in dataframe.columns
    ]
    copy_statement = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV)").format(
        sql.Identifier(RAW_SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(column_names),
    )

    with conn.cursor().copy(copy_statement) as copy:
        copy.write(csv_buffer.getvalue())


def main() -> None:
    tables = pd.read_pickle(DATA_FILE)

    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(RAW_SCHEMA)))

        for table_name, dataframe in tables.items():
            create_raw_table(conn, table_name, list(dataframe.columns))
            copy_dataframe(conn, table_name, dataframe)
            print(f"Loaded raw.{table_name}: {len(dataframe)} rows")


if __name__ == "__main__":
    main()
