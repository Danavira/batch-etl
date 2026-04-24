"""
Japanese-themed fake data generator.

Generates User and Transaction records and writes them directly to PostgreSQL.
Runs in a continuous loop; each tick inserts a batch of new users and transactions,
then syncs updated balances back.

Environment variables:
    PG_DSN                  Postgres connection string (default: local demo)
    USERS_PER_BATCH         New users per tick           (default: 10)
    TRANSACTIONS_PER_BATCH  Transactions per tick        (default: 50)
    INTERVAL_SECONDS        Sleep between ticks          (default: 30)
    MAX_EXISTING_USERS      Users sampled from DB for the transaction pool (default: 200)
"""
import hashlib
import logging
import os
import random
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg2
from psycopg2.extras import execute_values
from mimesis import Address, Finance, Person, Text
from mimesis.locales import Locale

# Allow importing schemas from src/ when run as a standalone image.
# The Dockerfile sets PYTHONPATH=/app/src; this covers local runs.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from batch_etl.config.schemas.user import User, AccountType, KYCStatus, AccountStatus
from batch_etl.config.schemas.transaction import (
    Transaction,
    TransactionType,
    TransactionChannel,
    TransactionStatus,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PG_DSN = os.getenv("PG_DSN", "host=localhost port=5432 dbname=demo user=demo password=demo")
USERS_PER_BATCH = int(os.getenv("USERS_PER_BATCH", "10"))
TRANSACTIONS_PER_BATCH = int(os.getenv("TRANSACTIONS_PER_BATCH", "50"))
INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "30"))
MAX_EXISTING_USERS = int(os.getenv("MAX_EXISTING_USERS", "200"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Mimesis providers — Japanese locale
_person = Person(Locale.JA)
_address = Address(Locale.JA)
_text = Text(Locale.JA)
_finance = Finance(Locale.JA)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id                      UUID          PRIMARY KEY,
    customer_number         VARCHAR(20)   UNIQUE NOT NULL,
    full_name               VARCHAR(150)  NOT NULL,
    email                   VARCHAR(255)  UNIQUE NOT NULL,
    phone_number            VARCHAR(20)   NOT NULL,
    password_hash           TEXT          NOT NULL,
    date_of_birth           DATE          NOT NULL,
    government_id_type      VARCHAR(30),
    government_id_number    VARCHAR(100),
    address_line_1          VARCHAR(255),
    address_line_2          VARCHAR(255),
    city                    VARCHAR(100),
    state                   VARCHAR(100),
    postal_code             VARCHAR(20),
    country                 VARCHAR(100)  NOT NULL DEFAULT 'Japan',
    account_number          VARCHAR(30)   UNIQUE NOT NULL,
    account_type            VARCHAR(20)   NOT NULL,
    currency                CHAR(3)       NOT NULL DEFAULT 'JPY',
    balance                 NUMERIC(18,2) NOT NULL DEFAULT 0.00,
    kyc_status              VARCHAR(20)   NOT NULL DEFAULT 'pending',
    account_status          VARCHAR(20)   NOT NULL DEFAULT 'active',
    last_login_at           TIMESTAMPTZ,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
"""

CREATE_TRANSACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS transactions (
    id                          UUID          PRIMARY KEY,
    transaction_reference       VARCHAR(40)   UNIQUE NOT NULL,
    user_id                     UUID          NOT NULL REFERENCES users(id),
    transaction_type            VARCHAR(20)   NOT NULL,
    channel                     VARCHAR(20)   NOT NULL,
    amount                      NUMERIC(18,2) NOT NULL,
    currency                    CHAR(3)       NOT NULL DEFAULT 'JPY',
    direction                   VARCHAR(10)   NOT NULL,
    status                      VARCHAR(20)   NOT NULL DEFAULT 'pending',
    description                 TEXT,
    merchant_name               VARCHAR(150),
    counterparty_name           VARCHAR(150),
    counterparty_account_number VARCHAR(50),
    counterparty_bank_name      VARCHAR(150),
    balance_before              NUMERIC(18,2) NOT NULL,
    balance_after               NUMERIC(18,2) NOT NULL,
    external_reference          VARCHAR(100),
    processed_at                TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
"""

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------
CREDIT_TYPES: list[TransactionType] = ["deposit", "transfer_in", "refund"]
DEBIT_TYPES: list[TransactionType] = ["withdrawal", "transfer_out", "payment", "fee"]

ACCOUNT_TYPES: list[AccountType] = ["savings", "current", "business"]
KYC_STATUSES: list[KYCStatus] = ["pending", "verified", "rejected"]
ACCOUNT_STATUSES: list[AccountStatus] = ["active", "active", "active", "frozen", "suspended"]

CHANNELS: list[TransactionChannel] = ["mobile_app", "atm", "branch", "api", "card", "bank_transfer"]

JP_BANKS = [
    "三菱UFJ銀行",
    "三井住友銀行",
    "みずほ銀行",
    "りそな銀行",
    "ゆうちょ銀行",
    "楽天銀行",
    "SBI新生銀行",
    "PayPay銀行",
    "住信SBIネット銀行",
    "auじぶん銀行",
]

JP_GOV_ID_TYPES = ["マイナンバーカード", "パスポート", "運転免許証"]

EMAIL_DOMAINS = ["gmail.com", "yahoo.co.jp", "docomo.ne.jp", "softbank.ne.jp", "icloud.com", "ezweb.ne.jp"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fake_dob(min_age: int = 18, max_age: int = 70) -> date:
    today = date.today()
    days = random.randint(min_age * 365, max_age * 365)
    return today.replace(year=today.year - days // 365) - __import__("datetime").timedelta(days=days % 365)


def _password_hash() -> str:
    return hashlib.sha256(uuid4().bytes).hexdigest()


def _gov_id_number(id_type: str) -> str:
    if id_type == "パスポート":
        letters = "".join(random.choices("ABCDEFGHJKLMNPRSTUWXY", k=2))
        digits = str(random.randint(1_000_000, 9_999_999))
        return f"{letters}{digits}"
    # マイナンバーカード and 運転免許証: 12 digits
    return str(random.randint(100_000_000_000, 999_999_999_999))


# ---------------------------------------------------------------------------
# Record factories
# ---------------------------------------------------------------------------
def make_user() -> User:
    has_gov_id = random.random() > 0.2
    gov_id_type = random.choice(JP_GOV_ID_TYPES) if has_gov_id else None

    return User(
        customer_number=f"CUST{random.randint(10_000_000, 99_999_999)}",
        full_name=_person.full_name(),
        email=f"{_person.username()}{random.randint(1, 9999)}@{random.choice(EMAIL_DOMAINS)}",
        phone_number=_person.telephone()[:20],
        password_hash=_password_hash(),
        date_of_birth=_fake_dob(),
        government_id_type=gov_id_type,
        government_id_number=_gov_id_number(gov_id_type) if gov_id_type else None,
        address_line_1=_address.address(),
        address_line_2=f"{random.randint(1, 20)}F" if random.random() > 0.7 else None,
        city=_address.city(),
        state=_address.state(),
        postal_code=_address.postal_code(),
        country="Japan",
        account_number=f"ACC{random.randint(100_000_000_000, 999_999_999_999)}",
        account_type=random.choice(ACCOUNT_TYPES),
        currency="JPY",
        balance=Decimal(str(round(random.uniform(10_000, 5_000_000), 2))),
        kyc_status=random.choice(KYC_STATUSES),
        account_status=random.choice(ACCOUNT_STATUSES),
    )


def make_transaction(user_id: UUID, current_balance: Decimal) -> tuple[Transaction, Decimal]:
    """
    Returns the Transaction and the user's updated balance after this transaction.
    Debits are capped at the current balance so balance_after never goes negative.
    Failed/reversed transactions leave the balance unchanged.
    """
    if current_balance < Decimal("10_000"):
        tx_type: TransactionType = random.choice(CREDIT_TYPES)
    else:
        tx_type = random.choice(CREDIT_TYPES + DEBIT_TYPES)

    is_credit = tx_type in CREDIT_TYPES

    # JPY amounts: ¥100 – ¥1,000,000
    amount = Decimal(str(round(random.uniform(100, 1_000_000), 2)))
    if not is_credit:
        amount = min(amount, current_balance)

    direction = "credit" if is_credit else "debit"
    balance_before = current_balance
    balance_after = balance_before + amount if is_credit else balance_before - amount

    # Counterparty / merchant
    merchant_name = _finance.company()[:150] if tx_type == "payment" else None
    counterparty_name = None
    counterparty_account_number = None
    counterparty_bank_name = None
    if tx_type in ("transfer_in", "transfer_out"):
        counterparty_name = _person.full_name()
        counterparty_account_number = f"ACC{random.randint(100_000_000_000, 999_999_999_999)}"
        counterparty_bank_name = random.choice(JP_BANKS)

    status: TransactionStatus = random.choices(
        ["completed", "pending", "failed", "reversed"],
        weights=[70, 15, 10, 5],
    )[0]

    # Failed/reversed transactions do not change the user's balance
    effective_balance_after = balance_before if status in ("failed", "reversed") else balance_after

    txn = Transaction(
        transaction_reference=f"TXN{random.randint(100_000_000_000_000_000, 999_999_999_999_999_999)}",
        user_id=user_id,
        transaction_type=tx_type,
        channel=random.choice(CHANNELS),
        amount=amount,
        currency="JPY",
        direction=direction,
        status=status,
        description=_text.sentence() if random.random() > 0.5 else None,
        merchant_name=merchant_name,
        counterparty_name=counterparty_name,
        counterparty_account_number=counterparty_account_number,
        counterparty_bank_name=counterparty_bank_name,
        balance_before=balance_before,
        balance_after=effective_balance_after,
        processed_at=_now() if status == "completed" else None,
    )

    return txn, effective_balance_after


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------
def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_USERS_TABLE)
        cur.execute(CREATE_TRANSACTIONS_TABLE)
    conn.commit()
    log.info("Tables verified.")


def insert_users(conn, users: list[User]) -> None:
    if not users:
        return
    rows = [
        (
            str(u.id), u.customer_number, u.full_name, u.email, u.phone_number,
            u.password_hash, u.date_of_birth, u.government_id_type, u.government_id_number,
            u.address_line_1, u.address_line_2, u.city, u.state, u.postal_code, u.country,
            u.account_number, u.account_type, u.currency, float(u.balance),
            u.kyc_status, u.account_status, u.last_login_at, u.created_at, u.updated_at,
        )
        for u in users
    ]
    sql = """
    INSERT INTO users (
        id, customer_number, full_name, email, phone_number, password_hash,
        date_of_birth, government_id_type, government_id_number,
        address_line_1, address_line_2, city, state, postal_code, country,
        account_number, account_type, currency, balance,
        kyc_status, account_status, last_login_at, created_at, updated_at
    )
    VALUES %s
    ON CONFLICT (id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)


def insert_transactions(conn, txns: list[Transaction]) -> None:
    if not txns:
        return
    rows = [
        (
            str(t.id), t.transaction_reference, str(t.user_id), t.transaction_type, t.channel,
            float(t.amount), t.currency, t.direction, t.status, t.description,
            t.merchant_name, t.counterparty_name, t.counterparty_account_number, t.counterparty_bank_name,
            float(t.balance_before), float(t.balance_after), t.external_reference,
            t.processed_at, t.created_at, t.updated_at,
        )
        for t in txns
    ]
    sql = """
    INSERT INTO transactions (
        id, transaction_reference, user_id, transaction_type, channel,
        amount, currency, direction, status, description,
        merchant_name, counterparty_name, counterparty_account_number, counterparty_bank_name,
        balance_before, balance_after, external_reference,
        processed_at, created_at, updated_at
    )
    VALUES %s
    ON CONFLICT (id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)


def update_user_balances(conn, balance_updates: dict[UUID, Decimal]) -> None:
    if not balance_updates:
        return
    with conn.cursor() as cur:
        for user_id, new_balance in balance_updates.items():
            cur.execute(
                "UPDATE users SET balance = %s, updated_at = NOW() WHERE id = %s",
                (float(new_balance), str(user_id)),
            )


def fetch_existing_users(conn, limit: int) -> list[tuple[UUID, Decimal]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, balance FROM users ORDER BY random() LIMIT %s",
            (limit,),
        )
        return [(UUID(str(row[0])), Decimal(str(row[1]))) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tick
# ---------------------------------------------------------------------------
def run_tick(conn) -> dict:
    # 1. Generate and insert new users
    new_users = [make_user() for _ in range(USERS_PER_BATCH)]
    insert_users(conn, new_users)

    # 2. Build user pool: newly inserted + a sample of existing users
    existing = fetch_existing_users(conn, MAX_EXISTING_USERS)
    balance_map: dict[UUID, Decimal] = {uid: bal for uid, bal in existing}
    for u in new_users:
        balance_map[u.id] = u.balance

    user_pool = list(balance_map.keys())

    # 3. Generate and insert transactions
    txns: list[Transaction] = []
    for _ in range(TRANSACTIONS_PER_BATCH):
        uid = random.choice(user_pool)
        txn, new_bal = make_transaction(uid, balance_map[uid])
        txns.append(txn)
        balance_map[uid] = new_bal

    insert_transactions(conn, txns)

    # 4. Sync balances back for users that had transactions this tick
    touched = {t.user_id for t in txns}
    balance_updates = {uid: balance_map[uid] for uid in touched if uid in balance_map}
    update_user_balances(conn, balance_updates)

    conn.commit()

    return {
        "new_users": len(new_users),
        "transactions": len(txns),
        "balance_updates": len(balance_updates),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(PG_DSN)
    ensure_tables(conn)
    log.info(
        "Starting generation loop — users_per_batch=%d  transactions_per_batch=%d  interval=%ss",
        USERS_PER_BATCH, TRANSACTIONS_PER_BATCH, INTERVAL_SECONDS,
    )

    while True:
        try:
            summary = run_tick(conn)
            log.info("Tick: %s", summary)
        except Exception as exc:
            log.exception("Tick failed: %s", exc)
            conn.rollback()
            try:
                conn.close()
            except Exception:
                pass
            log.info("Reconnecting...")
            conn = psycopg2.connect(PG_DSN)

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
