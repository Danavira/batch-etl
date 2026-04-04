import os
import random
import uuid
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2.extras import execute_values

# -------- Config --------
PG_DSN = os.getenv(
    "PG_DSN",
    "host=localhost port=5432 dbname=demo user=demo password=demo"
)

# How much data per run (tune later)
NEW_CUSTOMERS_PER_RUN = int(os.getenv("NEW_CUSTOMERS_PER_RUN", "5"))
NEW_ORDERS_PER_RUN = int(os.getenv("NEW_ORDERS_PER_RUN", "20"))
STATUS_UPDATES_PER_RUN = int(os.getenv("STATUS_UPDATES_PER_RUN", "15"))


# -------- Helpers --------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def rand_name() -> str:
    first = random.choice(["Alya", "Bima", "Citra", "Dimas", "Eka", "Fajar", "Gita", "Hana"])
    last = random.choice(["Putra", "Sari", "Wijaya", "Pratama", "Utami", "Saputra", "Nugroho"])
    return f"{first} {last}"

def rand_email(name: str) -> str:
    base = name.lower().replace(" ", ".")
    domain = random.choice(["example.com", "mail.test", "demo.local"])
    suffix = random.randint(1, 9999)
    return f"{base}{suffix}@{domain}"

ORDER_STATUS_FLOW = {
    "created": ["paid", "canceled"],
    "paid": ["shipped", "refunded"],
    "shipped": ["delivered", "lost"],
    "delivered": ["returned", "completed"],
    # terminal-ish
    "canceled": [],
    "refunded": [],
    "lost": [],
    "returned": [],
    "completed": [],
}

def next_status(current: str) -> str | None:
    options = ORDER_STATUS_FLOW.get(current, [])
    return random.choice(options) if options else None


# -------- DB actions --------
def upsert_customers(conn, customers: list[tuple]) -> None:
    """
    customers tuples:
      (customer_id, email, full_name, created_at, updated_at)
    """
    if not customers:
        return

    sql = """
    INSERT INTO customers_raw (customer_id, email, full_name, created_at, updated_at)
    VALUES %s
    ON CONFLICT (customer_id) DO UPDATE SET
      email = EXCLUDED.email,
      full_name = EXCLUDED.full_name,
      updated_at = EXCLUDED.updated_at
    ;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, customers, page_size=500)

def insert_orders(conn, orders: list[tuple]) -> None:
    """
    orders tuples:
      (order_id, customer_id, amount, currency, status, created_at, updated_at)
    """
    if not orders:
        return

    sql = """
    INSERT INTO orders_raw (order_id, customer_id, amount, currency, status, created_at, updated_at)
    VALUES %s
    ON CONFLICT (order_id) DO NOTHING
    ;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, orders, page_size=500)

def insert_events(conn, events: list[tuple]) -> None:
    """
    events tuples:
      (event_id, order_id, event_type, event_time)
    """
    if not events:
        return

    sql = """
    INSERT INTO order_events_raw (event_id, order_id, event_type, event_time)
    VALUES %s
    ON CONFLICT (event_id) DO NOTHING
    ;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, events, page_size=500)

def fetch_random_orders(conn, k: int) -> list[tuple[str, str]]:
    """
    Returns list of (order_id, status) for random subset.
    """
    if k <= 0:
        return []
    sql = """
    SELECT order_id, status
    FROM orders_raw
    ORDER BY random()
    LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (k,))
        return cur.fetchall()

def update_order_statuses(conn, updates: list[tuple[str, str, datetime]]) -> None:
    """
    updates tuples:
      (new_status, order_id, updated_at)
    """
    if not updates:
        return

    sql = """
    UPDATE orders_raw
    SET status = %s, updated_at = %s
    WHERE order_id = %s;
    """
    with conn.cursor() as cur:
        for new_status, order_id, updated_at in updates:
            cur.execute(sql, (new_status, updated_at, order_id))


# -------- Main generator tick --------
def generate_tick(
    new_customers: int = NEW_CUSTOMERS_PER_RUN,
    new_orders: int = NEW_ORDERS_PER_RUN,
    status_updates: int = STATUS_UPDATES_PER_RUN,
) -> dict:
    """
    One 'tick' simulating new entities + status transitions.
    Suitable to call from Airflow PythonOperator.
    """
    t = now_utc()

    conn = psycopg2.connect(PG_DSN)
    try:
        conn.autocommit = False

        # 1) Create/upsert customers
        customers = []
        customer_ids = []
        for _ in range(new_customers):
            cid = f"cust_{uuid.uuid4().hex[:10]}"
            name = rand_name()
            customers.append((cid, rand_email(name), name, t, t))
            customer_ids.append(cid)

        upsert_customers(conn, customers)

        # 2) Create orders for either new or existing customers
        # We'll mix: 60% existing, 40% new
        existing_customer_ids = []
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM customers_raw ORDER BY random() LIMIT 200;")
            existing_customer_ids = [r[0] for r in cur.fetchall()]

        all_customers = (existing_customer_ids + customer_ids) or customer_ids

        orders = []
        events = []

        for _ in range(new_orders):
            oid = f"ord_{uuid.uuid4().hex[:12]}"
            cid = random.choice(all_customers)
            amt = round(random.uniform(5, 500), 2)
            status = "created"
            created_at = t - timedelta(seconds=random.randint(0, 300))
            updated_at = created_at

            orders.append((oid, cid, amt, "USD", status, created_at, updated_at))
            events.append((f"evt_{uuid.uuid4().hex[:14]}", oid, "created", created_at))

        insert_orders(conn, orders)
        insert_events(conn, events)

        # 3) Advance status on some existing orders
        sampled = fetch_random_orders(conn, status_updates)
        status_changes = []
        status_events = []
        for order_id, cur_status in sampled:
            nxt = next_status(cur_status)
            if not nxt:
                continue
            ut = now_utc()
            status_changes.append((nxt, order_id, ut))
            status_events.append((f"evt_{uuid.uuid4().hex[:14]}", order_id, nxt, ut))

        update_order_statuses(conn, status_changes)
        insert_events(conn, status_events)

        conn.commit()

        return {
            "new_customers": len(customers),
            "new_orders": len(orders),
            "status_transitions": len(status_changes),
            "events_written": len(events) + len(status_events),
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    summary = generate_tick()
    print(summary)