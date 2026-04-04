import datetime
import re
from typing import Any, Iterable

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


# =========================
# Config
# =========================
DEFAULT_START_DATE = datetime.date(2023, 1, 1)
LOOKBACK_DAYS = 365

AVAILABLE_PRINCIPALS = ["chief_4"]  # keep this as your allow-list

CLICKHOUSE_CONN_ID = "clickhouse_default"
CLICKHOUSE_POOL = "clickhouse_pool"

DAG_ID = "backfill"
SCHEDULE = "*/30 * * * *"


# =========================
# Helpers
# =========================
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name: str) -> str:
    """
    Very important: ClickHouse doesn't let you parameterize identifiers (db/table),
    so we *must* strictly validate anything used in `db.table` contexts.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name


def month_start(d: datetime.date) -> datetime.date:
    return d.replace(day=1)


def month_end(d: datetime.date) -> datetime.date:
    # last day of month: move to next month 1st day then subtract 1 day
    next_month = (d.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    return next_month - datetime.timedelta(days=1)


def first_cell(rows: list[list[Any]] | list[tuple[Any, ...]] | None, default: Any = None) -> Any:
    if rows and rows[0]:
        return rows[0][0]
    return default


def ch_hook() -> ClickHouseHook:
    return ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)


# =========================
# ClickHouse SQL (metadata checks)
# =========================
def sql_last_success_mode(principal: str) -> str:
    principal = validate_identifier(principal)
    return f"""
        SELECT mode
        FROM metadata.chief_to_training
        WHERE principal = '{principal}'
          AND model_name = 'sre'
          AND status = 'success'
        ORDER BY finished_at DESC
        LIMIT 1
    """


def sql_tables_concurrent_ok(principal: str) -> str:
    principal = validate_identifier(principal)
    return f"""
        SELECT (count() > 0) AND (countIf(backfill = 1) = 0) AS ok
        FROM metadata.etl_bookmark
        WHERE schema = '{principal}'
    """


def sql_last_success_end_date(principal: str) -> str:
    """
    Returns the last successful backfill end_date.
    If you prefer to advance by processed boundary, use MAX(processed_end_date) instead.
    """
    principal = validate_identifier(principal)
    return f"""
        SELECT nullIf(MAX(end_date), toDate('1970-01-01'))
        FROM metadata.chief_to_training
        WHERE principal = '{principal}'
          AND model_name = 'sre'
          AND mode = 'backfill'
          AND status = 'success'
    """


# =========================
# Watermark SQL builders
# =========================
def sql_watermark_running(window: dict, run_id: str) -> str:
    p = validate_identifier(window["principal"])
    s = window["start_date"]
    e = window["end_date"]
    return f"""
        INSERT INTO metadata.chief_to_training
            (principal, model_name, mode, start_date, end_date, status, run_id, finished_at)
        VALUES
            ('{p}', 'sre', 'backfill', toDate('{s}'), toDate('{e}'), 'running', '{run_id}', NULL)
    """


def sql_watermark_failed(window: dict, run_id: str) -> str:
    p = validate_identifier(window["principal"])
    s = window["start_date"]
    e = window["end_date"]
    return f"""
        INSERT INTO metadata.chief_to_training
            (principal, model_name, mode, start_date, end_date, status, run_id, finished_at)
        VALUES
            ('{p}', 'sre', 'backfill', toDate('{s}'), toDate('{e}'), 'failed', '{run_id}', now64(3))
    """


def sql_watermark_success(window: dict, run_id: str) -> str:
    p = validate_identifier(window["principal"])
    s = window["start_date"]
    e = window["end_date"]

    # NOTE: mode is set to incremental only if current date is within the window.
    # If you want a stricter definition, adjust this CASE.
    return f"""
        INSERT INTO metadata.chief_to_training
            (principal, model_name, mode, start_date, end_date,
             processed_end_date, processed_max_updated_at,
             status, run_id, finished_at)
        SELECT
            '{p}' AS principal,
            'sre' AS model_name,
            CAST(
                CASE
                    WHEN (today() BETWEEN toDate('{s}') AND toDate('{e}')) THEN 'incremental'
                    ELSE 'backfill'
                END
                AS Enum('backfill' = 1, 'incremental' = 2)
            ) AS mode,
            toDate('{s}') AS start_date,
            toDate('{e}') AS end_date,
            (
                SELECT ifNull(max(invoice_date), toDate('1970-01-01'))
                FROM clean.{p}_staging_sre_base_fact
                WHERE invoice_date BETWEEN toDate('{s}') AND toDate('{e}')
            ) AS processed_end_date,
            (
                SELECT ifNull(max(updated_at), toDateTime64('1970-01-01 00:00:00', 3))
                FROM clean.{p}_staging_sre_base_fact
                WHERE invoice_date BETWEEN toDate('{s}') AND toDate('{e}')
            ) AS processed_max_updated_at,
            'success' AS status,
            '{run_id}' AS run_id,
            now64(3) AS finished_at
        FROM system.one
    """


def sql_cleanup_window(window: dict) -> str:
    p = validate_identifier(window["principal"])
    s = window["start_date"]
    e = window["end_date"]
    return f"""
        DELETE FROM {p}.sre_base_fact
        WHERE invoice_date BETWEEN toDate('{s}') AND toDate('{e}')
    """


# =========================
# Task logic (pure Python)
# =========================
def get_ready_principals_logic(available: list[str]) -> list[str]:
    hook = ch_hook()
    ready: list[str] = []

    # Hard-enforce allow-list (prevents someone passing arbitrary schema/table names)
    allow = set(available)
    for principal in available:
        if principal not in allow:
            continue

        validate_identifier(principal)

        concurrent_ok = bool(first_cell(hook.execute(sql_tables_concurrent_ok(principal)), default=False))
        last_mode = first_cell(hook.execute(sql_last_success_mode(principal)), default=None)

        # original behavior preserved:
        # - if tables concurrent and never ran: ok
        # - if last success was backfill: ok
        if (concurrent_ok and last_mode is None) or (last_mode == "backfill"):
            ready.append(principal)

    return ready


def compute_monthly_windows_logic(principals: list[str]) -> list[dict]:
    hook = ch_hook()
    windows: list[dict] = []

    for principal in principals:
        validate_identifier(principal)

        last_end = first_cell(hook.execute(sql_last_success_end_date(principal)), default=None)
        if last_end:
            # last_end is a date from CH driver; add one day
            start = last_end + datetime.timedelta(days=1)
        else:
            start = DEFAULT_START_DATE

        start = month_start(start)
        end = month_end(start)

        windows.append(
            {
                "principal": principal,
                "mode": "backfill",
                "start_date": str(start),
                "end_date": str(end),
            }
        )

    return windows


# =========================
# TaskFlow tasks
# =========================
@task
def get_ready_principals(available: list[str]) -> list[str]:
    return get_ready_principals_logic(available)


@task.short_circuit
def require_ready_principals(principals: list[str]) -> bool:
    if not principals:
        print("No ready principals found. Short-circuiting downstream tasks.")
        return False
    print(f"Ready principals: {principals}")
    return True


@task
def compute_windows(principals: list[str]) -> list[dict]:
    return compute_monthly_windows_logic(principals)


@task
def filter_windows_for_features(windows: list[dict]) -> list[dict]:
    """
    Keep only windows whose start_date is at least LOOKBACK_DAYS-1 after DEFAULT_START_DATE.
    (Matches your prior intent, but replaces 364 with LOOKBACK_DAYS-1.)
    """
    threshold = DEFAULT_START_DATE + datetime.timedelta(days=LOOKBACK_DAYS - 1)

    ok: list[dict] = []
    for w in windows or []:
        s = datetime.date.fromisoformat(w["start_date"])
        if s >= threshold:
            ok.append(w)

    print(f"Filtered windows for features: {len(ok)} / {len(windows or [])} (threshold={threshold})")
    return ok


@task.branch
def branch_on_windows(windows: list[dict]) -> str:
    return "feature_start" if windows else "skip_features"


@task
def build_running_sqls(windows: list[dict]) -> list[str]:
    run_id = get_current_context()["run_id"]
    return [sql_watermark_running(w, run_id) for w in (windows or [])]


@task
def build_success_sqls(windows: list[dict]) -> list[str]:
    run_id = get_current_context()["run_id"]
    return [sql_watermark_success(w, run_id) for w in (windows or [])]


@task
def build_failed_sqls(windows: list[dict]) -> list[str]:
    run_id = get_current_context()["run_id"]
    return [sql_watermark_failed(w, run_id) for w in (windows or [])]


@task
def build_cleanup_sqls(windows: list[dict]) -> list[str]:
    return [sql_cleanup_window(w) for w in (windows or [])]


# =========================
# DAG
# =========================
with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 12, 28),
    schedule=SCHEDULE,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=3,
    max_active_tasks=16,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=1),
    },
    tags=["backfill", "training"],
) as dag:
    create_training_db = ClickHouseOperator(
        task_id="create_training_db",
        sql="CREATE DATABASE IF NOT EXISTS training",
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    )

    create_metadata_table = ClickHouseOperator(
        task_id="create_metadata_table",
        sql="""
            CREATE TABLE IF NOT EXISTS metadata.chief_to_training (
                principal String,
                model_name LowCardinality(String),
                mode Enum8('backfill'=1, 'incremental'=2),
                start_date Nullable(Date),
                end_date Nullable(Date),
                processed_end_date Nullable(Date),
                processed_max_updated_at Nullable(DateTime64(3)),
                status Enum8('running'=1, 'success'=2, 'failed'=3),
                run_id String,
                finished_at Nullable(DateTime64(3))
            )
            ENGINE = MergeTree()
            ORDER BY (principal, model_name, start_date, end_date, finished_at)
        """,
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    )

    ready_principals = get_ready_principals(AVAILABLE_PRINCIPALS)
    proceed = require_ready_principals(ready_principals)

    windows = compute_windows(ready_principals)
    feature_windows = filter_windows_for_features(windows)

    running_sqls = build_running_sqls(windows)
    success_sqls = build_success_sqls(windows)
    failed_sqls = build_failed_sqls(windows)
    cleanup_sqls = build_cleanup_sqls(windows)

    skip_features = EmptyOperator(task_id="skip_features")

    feature_start = EmptyOperator(
        task_id="feature_start",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    mark_running = ClickHouseOperator.partial(
        task_id="mark_running",
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        pool=CLICKHOUSE_POOL,
    ).expand(sql=running_sqls)

    mark_success = ClickHouseOperator.partial(
        task_id="mark_success",
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        pool=CLICKHOUSE_POOL,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    ).expand(sql=success_sqls)

    mark_failed = ClickHouseOperator.partial(
        task_id="mark_failed",
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        pool=CLICKHOUSE_POOL,
        trigger_rule=TriggerRule.ONE_FAILED,
    ).expand(sql=failed_sqls)

    cleanup_data = ClickHouseOperator.partial(
        task_id="cleanup_data",
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        pool=CLICKHOUSE_POOL,
        trigger_rule=TriggerRule.ONE_FAILED,
    ).expand(sql=cleanup_sqls)

    trigger_base_fact = TriggerDagRunOperator(
        task_id="trigger_base_fact",
        trigger_dag_id="base_fact",
        conf={"run_params": windows},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        pool="trigger_pool",
        execution_timeout=datetime.timedelta(hours=2),
    )

    trigger_customer_feat = TriggerDagRunOperator(
        task_id="trigger_customer_feat",
        trigger_dag_id="feat_customer",
        conf={"run_params": feature_windows},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        pool="trigger_pool",
        execution_timeout=datetime.timedelta(hours=2),
    )

    trigger_product_feat = TriggerDagRunOperator(
        task_id="trigger_product_feat",
        trigger_dag_id="feat_product",
        conf={"run_params": feature_windows},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        pool="trigger_pool",
        execution_timeout=datetime.timedelta(hours=2),
    )

    trigger_customer_product_feat = TriggerDagRunOperator(
        task_id="trigger_customer_product_feat",
        trigger_dag_id="feat_customer_product",
        conf={"run_params": feature_windows},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        pool="trigger_pool",
        execution_timeout=datetime.timedelta(hours=2),
    )

    trigger_area_feat = TriggerDagRunOperator(
        task_id="trigger_area_feat",
        trigger_dag_id="feat_area",
        conf={"run_params": feature_windows},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        pool="trigger_pool",
        execution_timeout=datetime.timedelta(hours=2),
    )

    trigger_join_final = TriggerDagRunOperator(
        task_id="trigger_final_join",
        trigger_dag_id="final_join",
        conf={"run_params": feature_windows},
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        pool="trigger_pool",
        execution_timeout=datetime.timedelta(hours=2),
    )

    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---- Dependencies
    create_training_db >> create_metadata_table >> ready_principals
    ready_principals >> proceed >> windows

    windows >> mark_running >> trigger_base_fact

    branch = branch_on_windows(feature_windows)
    feature_windows >> branch
    branch >> feature_start
    branch >> skip_features

    trigger_base_fact >> feature_start

    feature_start >> trigger_customer_feat >> trigger_product_feat >> trigger_customer_product_feat >> trigger_area_feat >> trigger_join_final

    skip_features >> pipeline_done
    trigger_join_final >> pipeline_done
    trigger_base_fact >> pipeline_done

    pipeline_done >> mark_success

    all_triggers = [
        trigger_base_fact,
        trigger_customer_feat,
        trigger_product_feat,
        trigger_customer_product_feat,
        trigger_area_feat,
        trigger_join_final,
    ]

    all_triggers >> cleanup_data
    all_triggers >> mark_failed