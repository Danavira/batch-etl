from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

import requests
from sqlalchemy import create_engine, text

# -----------------------
# Logging (works standalone AND inside Airflow)
# -----------------------
logger = logging.getLogger(__name__)

# -----------------------
# Config
# -----------------------
@dataclass(frozen=True)
class Config:
    pg_dsn: str
    latitude: float = -6.2     # Jakarta-ish
    longitude: float = 106.8
    timezone: str = "Asia/Jakarta"
    start_date: date = date.today() - timedelta(days=14)
    end_date: date = date.today()

def _daterange(d1: date, d2: date) -> Iterable[date]:
    d = d1
    while d <= d2:
        yield d
        d += timedelta(days=1)

# -----------------------
# DB setup
# -----------------------
DDL = """
CREATE TABLE IF NOT EXISTS raw_weather_daily (
    day DATE PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    tmax_c DOUBLE PRECISION,
    tmin_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

UPSERT = """
INSERT INTO raw_weather_daily (
    day, latitude, longitude, tmax_c, tmin_c, precipitation_mm, updated_at
)
VALUES (
    :day, :latitude, :longitude, :tmax_c, :tmin_c, :precipitation_mm, now()
)
ON CONFLICT (day)
DO UPDATE SET
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    tmax_c = EXCLUDED.tmax_c,
    tmin_c = EXCLUDED.tmin_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    updated_at = now();
"""

# -----------------------
# Extract
# -----------------------
def fetch_open_meteo_daily(cfg: Config) -> list[dict]:
    """
    Fetch daily aggregates from Open-Meteo for a date range.
    Returns list of dict rows compatible with UPSERT params.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": cfg.latitude,
        "longitude": cfg.longitude,
        "timezone": cfg.timezone,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "start_date": cfg.start_date.isoformat(),
        "end_date": cfg.end_date.isoformat(),
    }

    logger.info("Requesting Open-Meteo daily data: %s", params)
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    daily = payload.get("daily", {})
    days = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    prcp = daily.get("precipitation_sum", [])

    rows: list[dict] = []
    for i, day_str in enumerate(days):
        rows.append({
            "day": day_str,
            "latitude": cfg.latitude,
            "longitude": cfg.longitude,
            "tmax_c": tmax[i] if i < len(tmax) else None,
            "tmin_c": tmin[i] if i < len(tmin) else None,
            "precipitation_mm": prcp[i] if i < len(prcp) else None,
        })

    logger.info("Fetched %d rows from Open-Meteo", len(rows))
    return rows

# -----------------------
# Load
# -----------------------
def load_to_postgres(cfg: Config, rows: list[dict]) -> int:
    engine = create_engine(cfg.pg_dsn, pool_pre_ping=True)

    with engine.begin() as conn:
        conn.execute(text(DDL))
        if not rows:
            logger.warning("No rows to load.")
            return 0

        conn.execute(text("SET statement_timeout = '60s';"))
        conn.execute(text("SET lock_timeout = '10s';"))

        for row in rows:
            conn.execute(text(UPSERT), row)

    logger.info("Upserted %d rows into raw_weather_daily", len(rows))
    return len(rows)

# -----------------------
# Orchestrator-friendly entrypoint
# -----------------------
def run(cfg: Config) -> int:
    rows = fetch_open_meteo_daily(cfg)
    return load_to_postgres(cfg, rows)

if __name__ == "__main__":
    # Minimal standalone runner
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    dsn = os.getenv("PG_DSN", "postgresql://elt:elt@localhost:5432/raw")
    cfg = Config(pg_dsn=dsn)
    count = run(cfg)
    print(f"Loaded {count} rows.")