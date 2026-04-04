import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

API_URL = os.getenv("API_URL", "http://localhost:8000")
WATERMARK_FILE = os.getenv("WATERMARK_FILE", "watermark_orders.json")

PG_DSN = os.getenv(
    "PG_DSN",
    "host=localhost port=5432 dbname=demo user=demo password=demo"
)

def load_watermark() -> str | None:
    if not os.path.exists(WATERMARK_FILE):
        return None
    with open(WATERMARK_FILE, "r") as f:
        return json.load(f).get("since")

def save_watermark(since: str) -> None:
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"since": since}, f)

def iso_now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def fetch_orders(since: str | None) -> list[dict]:
    params = {}
    if since:
        params["since"] = since
    r = requests.get(f"{API_URL}/orders", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def upsert_orders(conn, orders: list[dict]) -> None:
    if not orders:
        return

    rows = []
    max_updated_at = None

    for o in orders:
        # FastAPI returns ISO8601 strings; Postgres can parse them as timestamptz
        rows.append((
            o["order_id"],
            o["customer_id"],
            o["amount"],
            o["currency"],
            o["status"],
            o["created_at"],
            o["updated_at"],
            iso_now_utc(),
        ))
        upd = datetime.fromisoformat(o["updated_at"].replace("Z", "+00:00"))
        if max_updated_at is None or upd > max_updated_at:
            max_updated_at = upd

    sql = """
    INSERT INTO orders_raw (
      order_id, customer_id, amount, currency, status, created_at, updated_at, ingested_at
    )
    VALUES %s
    ON CONFLICT (order_id) DO UPDATE SET
      customer_id = EXCLUDED.customer_id,
      amount      = EXCLUDED.amount,
      currency    = EXCLUDED.currency,
      status      = EXCLUDED.status,
      created_at  = EXCLUDED.created_at,
      updated_at  = EXCLUDED.updated_at,
      ingested_at = EXCLUDED.ingested_at
    ;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)

    conn.commit()

    # update watermark to latest updated_at
    if max_updated_at:
        save_watermark(max_updated_at.astimezone(timezone.utc).isoformat())

def main():
    since = load_watermark()
    print(f"Using since watermark: {since}")

    orders = fetch_orders(since)
    print(f"Fetched {len(orders)} orders")

    if not orders:
        return

    conn = psycopg2.connect(PG_DSN)
    try:
        upsert_orders(conn, orders)
        print("Upsert complete.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()