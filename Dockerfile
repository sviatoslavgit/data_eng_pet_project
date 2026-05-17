FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY parse_olist_dataset.py load_olist_raw.py load_olist_orders.py ./
COPY sql ./sql

CMD ["python", "parse_olist_dataset.py"]
