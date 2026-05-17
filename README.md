# Olist Data Engineering Pipeline

This project downloads the Olist Brazilian ecommerce dataset, stores the raw dataset in PostgreSQL, and builds warehouse tables from PostgreSQL as the source of data.

## Local Stack

The local environment runs with Docker Compose:

- `postgres`: PostgreSQL 16 database.
- `olist-parser`: downloads the Kaggle dataset and saves it locally as `data/olist_dataset.pkl`.
- `olist-raw-loader`: reads `data/olist_dataset.pkl` and loads every original Olist table into the `raw` PostgreSQL schema.
- `olist-orders-loader`: reads from PostgreSQL `raw.*` tables and builds the older demo `public.orders` table.
- `olist-warehouse-loader`: incrementally reads from PostgreSQL `raw.*`, cleans batches of up to 5,000 orders, and loads a dimensional warehouse model.

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
docker compose up --build postgres olist-parser olist-raw-loader olist-warehouse-loader
```

What happens:

1. `postgres` starts and waits until it is healthy.
2. `olist-parser` downloads the Kaggle dataset and writes `data/olist_dataset.pkl`.
3. `olist-raw-loader` creates schema `raw`, recreates one raw table per Olist CSV, and copies the data into PostgreSQL.
4. `olist-warehouse-loader` reads from the `raw` PostgreSQL tables in incremental batches of up to 5,000 orders, cleans/types the batch, then loads staging and warehouse tables.

After the first run, PostgreSQL is the source for downstream transforms. The pickle file is only a local ingestion artifact used to load raw data into the database.

## Warehouse Model

The warehouse load creates three layers:

- `raw`: source tables copied from the original Olist CSV files.
- `staging`: typed and cleaned tables used as the transformation boundary.
- `warehouse`: dimensional tables for analytics.

Warehouse tables:

```text
warehouse.dim_customer
warehouse.dim_product
warehouse.dim_seller
warehouse.fact_orders
warehouse.fact_order_items
warehouse.etl_watermarks
warehouse.etl_batch_log
```

`warehouse.fact_orders` is one row per order. `warehouse.fact_order_items` is one row per order item. The dimension tables hold customer, product, and seller attributes.

## Incremental Batch Processing

The warehouse loader is intentionally batch-oriented for orchestration. It processes at most 5,000 source orders per batch.

```powershell
docker compose run --rm olist-warehouse-loader python build_olist_warehouse.py --batch-size 5000 --max-batches 1
```

Important options:

```text
--batch-size 5000     Number of source orders per batch. The hard maximum is 5,000.
--max-batches 1      Useful for scheduled orchestration; one trigger can process one bounded batch.
--full-refresh       Clears staging/warehouse tables and restarts from the beginning.
```

The incremental state is stored in:

```text
warehouse.etl_watermarks
```

Each batch run is logged in:

```text
warehouse.etl_batch_log
```

Example validation query:

```sql
SELECT
    batch_id,
    status,
    batch_size,
    staging_orders_loaded,
    fact_orders_upserted,
    source_min_purchase_ts,
    source_max_purchase_ts
FROM warehouse.etl_batch_log
ORDER BY batch_id;
```

The ordering key is:

```text
order_purchase_timestamp, order_id
```

That pair acts as the batch watermark, which lets the job continue from the last successfully completed batch.

## Databricks-Oriented Orchestration

This repository runs locally with Docker Compose, but the target orchestration path is Databricks Workflows deployed through GitHub Actions.

The Databricks bundle is defined in:

```text
databricks.yml
```

The manual GitHub Actions workflow is defined in:

```text
.github/workflows/databricks-warehouse.yml
```

The workflow:

1. Compiles the Python scripts.
2. Installs the Databricks CLI.
3. Validates the Databricks bundle.
4. Deploys the Databricks Workflow.
5. Optionally runs one manual incremental batch.

The Databricks job task runs:

```text
build_olist_warehouse.py --batch-size 5000 --max-batches 1 --full-refresh false
```

Each run processes no more than 5,000 source orders. For scheduled or manual validation, keep `max_batches=1` so one Databricks run equals one easy-to-check batch.

### GitHub Configuration

Create these GitHub repository secrets:

```text
DATABRICKS_HOST       Databricks workspace URL
DATABRICKS_TOKEN      Databricks personal access token or service principal token
```

Create these GitHub repository variables:

```text
DATABRICKS_NODE_TYPE_ID                Required. Example: Standard_DS3_v2 on Azure or i3.xlarge on AWS.
DATABRICKS_SPARK_VERSION               Optional. Default: 15.4.x-scala2.12
DATABRICKS_SECRET_SCOPE                Optional. Default: data_eng_pet_project
DATABRICKS_DATABASE_URL_SECRET_KEY     Optional. Default: postgres_database_url
```

Create this Databricks secret:

```text
scope: data_eng_pet_project
key: postgres_database_url
value: postgresql://user:password@host:5432/olist
```

The PostgreSQL host must be reachable from the Databricks job cluster. A local Docker database on `localhost:5432` is only reachable from your laptop, not from Databricks.

### Manual Databricks Run

In GitHub, open:

```text
Actions -> Databricks warehouse workflow -> Run workflow
```

Recommended validation inputs:

```text
target: dev
run_job: true
batch_size: 5000
max_batches: 1
full_refresh: false
```

Use `full_refresh=true` only when you intentionally want to reset the warehouse tables and restart the incremental watermark.

### Databricks Validation

After the workflow run completes, validate from Databricks SQL or any PostgreSQL client connected to the same database:

```sql
SELECT
    batch_id,
    status,
    batch_size,
    staging_orders_loaded,
    fact_orders_upserted,
    source_min_purchase_ts,
    source_max_purchase_ts
FROM warehouse.etl_batch_log
ORDER BY batch_id DESC
LIMIT 10;
```

Check total warehouse rows:

```sql
SELECT COUNT(*) FROM warehouse.fact_orders;
SELECT COUNT(*) FROM warehouse.fact_order_items;
```

Check the current incremental watermark:

```sql
SELECT *
FROM warehouse.etl_watermarks
WHERE pipeline_name = 'olist_warehouse_incremental';
```

For a more production-like Databricks version, the same logical steps would usually move from pandas/PostgreSQL writes to Spark DataFrames and Delta tables, while keeping this same incremental orchestration pattern.

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

Build the dimensional warehouse from PostgreSQL in one 5,000-record batch:

```powershell
docker compose run --rm olist-warehouse-loader python build_olist_warehouse.py --batch-size 5000 --max-batches 1
```

Rebuild the warehouse from the beginning:

```powershell
docker compose run --rm olist-warehouse-loader python build_olist_warehouse.py --batch-size 5000 --full-refresh
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
SELECT COUNT(*) FROM warehouse.fact_orders;
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
