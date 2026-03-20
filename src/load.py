from __future__ import annotations

import csv
import logging
import sqlite3
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE_DB_PATH = PROJECT_ROOT / "dist/warehouse.db"
REGION_MAPPING_CSV_PATH = PROJECT_ROOT / "dist/region/region_mapping.csv"
CUSTOMERS_WEATHER_REGION_PATH = PROJECT_ROOT / "dist/region/customers_weather_region.csv"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )


def read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Input file not found: {csv_path}")

    with csv_path.open("r", newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS customer_weather_region")
    conn.execute("DROP TABLE IF EXISTS region_mapping")

    conn.execute(
        """
        CREATE TABLE region_mapping (
            Country TEXT PRIMARY KEY,
            RegionStarting2016 TEXT,
            RegionUntil2017 TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE customer_weather_region (
            CustomerID TEXT PRIMARY KEY,
            CompanyName TEXT,
            City TEXT,
            Country TEXT,
            TemperatureC REAL,
            FeelsLikeC REAL,
            WeatherMain TEXT,
            WeatherDescription TEXT,
            Humidity REAL,
            WindSpeed REAL,
            WeatherTimestamp INTEGER,
            RegionStarting2016 TEXT,
            RegionUntil2017 TEXT
        )
        """
    )


def load_region_mapping_table(
    conn: sqlite3.Connection, region_rows: list[dict[str, str]]
) -> None:
    conn.executemany(
        """
        INSERT INTO region_mapping (
            Country,
            RegionStarting2016,
            RegionUntil2017
        ) VALUES (?, ?, ?)
        """,
        [
            (
                normalize_text(row.get("Country")),
                normalize_text(row.get("RegionStarting2016")),
                normalize_text(row.get("RegionUntil2017")),
            )
            for row in region_rows
        ],
    )


def to_float(value: str | None) -> float | None:
    normalized = normalize_text(value)
    if normalized is None:
        return None
    return float(normalized)


def to_int(value: str | None) -> int | None:
    normalized = normalize_text(value)
    if normalized is None:
        return None
    return int(float(normalized))


def load_customer_weather_region_table(
    conn: sqlite3.Connection, customer_rows: list[dict[str, str]]
) -> None:
    conn.executemany(
        """
        INSERT INTO customer_weather_region (
            CustomerID,
            CompanyName,
            City,
            Country,
            TemperatureC,
            FeelsLikeC,
            WeatherMain,
            WeatherDescription,
            Humidity,
            WindSpeed,
            WeatherTimestamp,
            RegionStarting2016,
            RegionUntil2017
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                normalize_text(row.get("CustomerID")),
                normalize_text(row.get("CompanyName")),
                normalize_text(row.get("City")),
                normalize_text(row.get("Country")),
                to_float(row.get("TemperatureC")),
                to_float(row.get("FeelsLikeC")),
                normalize_text(row.get("WeatherMain")),
                normalize_text(row.get("WeatherDescription")),
                to_float(row.get("Humidity")),
                to_float(row.get("WindSpeed")),
                to_int(row.get("WeatherTimestamp")),
                normalize_text(row.get("RegionStarting2016")),
                normalize_text(row.get("RegionUntil2017")),
            )
            for row in customer_rows
        ],
    )


def fetch_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cursor.fetchone()[0])


def main() -> None:
    configure_logging()

    region_rows = read_csv_rows(REGION_MAPPING_CSV_PATH)
    customer_rows = read_csv_rows(CUSTOMERS_WEATHER_REGION_PATH)

    WAREHOUSE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(WAREHOUSE_DB_PATH) as conn:
        create_tables(conn)
        load_region_mapping_table(conn, region_rows)
        load_customer_weather_region_table(conn, customer_rows)
        conn.commit()

        region_row_count = fetch_row_count(conn, "region_mapping")
        customer_row_count = fetch_row_count(conn, "customer_weather_region")

    logging.info("Loaded region_mapping rows: %s", region_row_count)
    logging.info("Loaded customer_weather_region rows: %s", customer_row_count)
    logging.info("Saved warehouse database to %s", WAREHOUSE_DB_PATH)


if __name__ == "__main__":
    main()
