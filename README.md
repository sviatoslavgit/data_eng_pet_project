# Olist Data Engineering Pipeline

This project downloads the Olist Brazilian ecommerce dataset, stores the raw dataset in PostgreSQL, and builds an analysis-ready `orders` table from PostgreSQL as the source of data.

## Local Stack

The local environment runs with Docker Compose:

- `postgres`: PostgreSQL 16 database.
- `olist-parser`: downloads the Kaggle dataset and saves it locally as `data/olist_dataset.pkl`.
- `olist-raw-loader`: reads `data/olist_dataset.pkl` and loads every original Olist table into the `raw` PostgreSQL schema.
- `olist-orders-loader`: reads from PostgreSQL `raw.*` tables and builds the `public.orders` table.

PostgreSQL credentials for local development:

```text
Host: localhost
Port: 5432
Database: olist
User: olist
Password: olist
```

Inside Docker services, the database URL is:

```text
postgresql://olist:olist@postgres:5432/olist
```

From the host machine, use:

```text
postgresql://olist:olist@localhost:5432/olist
```

## How PostgreSQL Is Set Up Locally

The `postgres` service in `docker-compose.yml` uses the official `postgres:16` image. It creates a local database named `olist` with user/password `olist`.

Database files are stored in a named Docker volume:

```text
postgres-data
```

That means PostgreSQL data survives container restarts. Stopping the containers does not delete the database.

The local `./sql` folder is mounted to:

```text
/docker-entrypoint-initdb.d
```

Postgres runs files in that folder automatically only when the database volume is created for the first time. The loader scripts also execute the table DDL they need, so schema updates can be applied during normal loads.

## Pipeline Flow

Run the full pipeline:

```powershell
docker compose up --build postgres olist-parser olist-raw-loader olist-orders-loader
```

What happens:

1. `postgres` starts and waits until it is healthy.
2. `olist-parser` downloads the Kaggle dataset and writes `data/olist_dataset.pkl`.
3. `olist-raw-loader` creates schema `raw`, recreates one raw table per Olist CSV, and copies the data into PostgreSQL.
4. `olist-orders-loader` reads from the `raw` PostgreSQL tables, joins orders with customers, items, products, payments, and reviews, then loads `public.orders`.

After the first run, PostgreSQL is the source for downstream transforms. The pickle file is only a local ingestion artifact used to load raw data into the database.

## Useful Commands

Start only PostgreSQL:

```powershell
docker compose up -d postgres
```

Download and save the Kaggle dataset:

```powershell
docker compose run --rm olist-parser
```

Load all raw Olist tables into PostgreSQL:

```powershell
docker compose run --rm olist-raw-loader
```

Build the analysis-ready orders table from PostgreSQL:

```powershell
docker compose run --rm olist-orders-loader
```

Open a PostgreSQL shell:

```powershell
docker compose exec postgres psql -U olist -d olist
```

Example checks inside `psql`:

```sql
\dt raw.*
\dt public.*
SELECT COUNT(*) FROM raw.olist_orders_dataset;
SELECT COUNT(*) FROM public.orders;
SELECT * FROM public.orders LIMIT 5;
```

Stop containers:

```powershell
docker compose down
```

Delete containers and the local PostgreSQL data volume:

```powershell
docker compose down -v
```

Use `down -v` only when you intentionally want a clean database rebuild.
