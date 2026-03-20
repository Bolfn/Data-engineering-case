from __future__ import annotations

import csv
import json
import logging
import sqlite3
import sys
from pathlib import Path

from region_mapping import EXPECTED_HEADERS, load_region_mapping_rows
from weather import COUNTRY_CODE_MAP


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "dist/northwind.db"
REGION_MAPPING_XLSX_PATH = PROJECT_ROOT / "region_mapping.xlsx"
ENRICHED_DATASET_PATH = PROJECT_ROOT / "dist/region/customers_weather_region.csv"
CITY_WEATHER_PATH = PROJECT_ROOT / "dist/weather/city_weather.csv"
OUTPUT_DIR = PROJECT_ROOT / "dist/quality"
QUALITY_REPORT_PATH = OUTPUT_DIR / "data_quality_report.json"

EXPECTED_CUSTOMER_COLUMNS = {
    "CustomerID": "TEXT",
    "CompanyName": "TEXT",
    "City": "TEXT",
    "Country": "TEXT",
}

EXPECTED_ORDER_COLUMNS = {
    "OrderID": "INTEGER",
    "CustomerID": "TEXT",
    "OrderDate": "DATETIME",
    "RequiredDate": "DATETIME",
    "ShippedDate": "DATETIME",
}

EXPECTED_ENRICHED_COLUMNS = {
    "CustomerID",
    "CompanyName",
    "City",
    "Country",
    "TemperatureC",
    "FeelsLikeC",
    "WeatherMain",
    "WeatherDescription",
    "Humidity",
    "WindSpeed",
    "WeatherTimestamp",
    "RegionStarting2016",
    "RegionUntil2017",
}

EXPECTED_NUMERIC_COLUMNS = {
    "TemperatureC",
    "FeelsLikeC",
    "Humidity",
    "WindSpeed",
    "WeatherTimestamp",
}


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


def fetch_table_schema(conn: sqlite3.Connection, table_name: str) -> list[dict[str, str]]:
    cursor = conn.execute(f"PRAGMA table_info([{table_name}])")
    return [
        {
            "name": row[1],
            "type": row[2],
        }
        for row in cursor.fetchall()
    ]


def validate_table_schema(
    schema_rows: list[dict[str, str]],
    expected_columns: dict[str, str],
    table_name: str,
) -> list[dict[str, object]]:
    schema_by_column = {row["name"]: row["type"].upper() for row in schema_rows}
    results = []

    for column_name, expected_type in expected_columns.items():
        actual_type = schema_by_column.get(column_name)
        status = "pass" if actual_type == expected_type else "fail"
        results.append(
            {
                "check": "source_schema_validation",
                "target": table_name,
                "status": status,
                "details": {
                    "column": column_name,
                    "expected_type": expected_type,
                    "actual_type": actual_type,
                },
            }
        )

    return results


def validate_region_mapping_schema(region_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    results = []
    actual_headers = set(region_rows[0].keys()) if region_rows else set()
    expected_normalized_headers = {
        "Country",
        "RegionStarting2016",
        "RegionUntil2017",
    }

    results.append(
        {
            "check": "region_mapping_schema_validation",
            "target": "region_mapping.xlsx",
            "status": "pass" if actual_headers == expected_normalized_headers else "fail",
            "details": {
                "expected_source_headers": sorted(EXPECTED_HEADERS),
                "expected_normalized_headers": sorted(expected_normalized_headers),
                "actual_headers": sorted(actual_headers),
            },
        }
    )
    return results


def validate_enriched_schema(enriched_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    actual_headers = set(enriched_rows[0].keys()) if enriched_rows else set()
    results = [
        {
            "check": "enriched_schema_validation",
            "target": "customers_weather_region",
            "status": "pass" if actual_headers == EXPECTED_ENRICHED_COLUMNS else "fail",
            "details": {
                "expected_headers": sorted(EXPECTED_ENRICHED_COLUMNS),
                "actual_headers": sorted(actual_headers),
            },
        }
    ]

    for column_name in sorted(EXPECTED_NUMERIC_COLUMNS):
        invalid_rows = []
        for index, row in enumerate(enriched_rows, start=2):
            value = normalize_text(row.get(column_name))
            if value is None:
                continue
            try:
                float(value)
            except ValueError:
                invalid_rows.append({"row_number": index, "value": value})

        results.append(
            {
                "check": "enriched_numeric_type_validation",
                "target": f"customers_weather_region.{column_name}",
                "status": "pass" if not invalid_rows else "fail",
                "details": {
                    "invalid_value_count": len(invalid_rows),
                    "sample_invalid_values": invalid_rows[:5],
                },
            }
        )

    return results


def validate_missing_values(enriched_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    checks = [
        ("CustomerID", "customer_id"),
        ("City", "customer_city"),
        ("Country", "country"),
        ("RegionStarting2016", "region_starting_2016"),
        ("RegionUntil2017", "region_until_2017"),
    ]
    results = []

    for column_name, label in checks:
        missing_count = sum(1 for row in enriched_rows if not normalize_text(row.get(column_name)))
        results.append(
            {
                "check": "missing_value_validation",
                "target": label,
                "status": "pass" if missing_count == 0 else "warn",
                "details": {
                    "missing_count": missing_count,
                },
            }
        )

    weather_missing_count = sum(
        1 for row in enriched_rows if not normalize_text(row.get("TemperatureC"))
    )
    results.append(
        {
            "check": "missing_value_validation",
            "target": "weather_data",
            "status": "pass" if weather_missing_count == 0 else "warn",
            "details": {
                "missing_count": weather_missing_count,
            },
        }
    )

    return results


def validate_duplicates(enriched_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    seen_customer_ids = set()
    duplicate_customer_ids = set()

    for row in enriched_rows:
        customer_id = normalize_text(row.get("CustomerID"))
        if not customer_id:
            continue
        if customer_id in seen_customer_ids:
            duplicate_customer_ids.add(customer_id)
        seen_customer_ids.add(customer_id)

    return [
        {
            "check": "duplicate_validation",
            "target": "customers_weather_region.CustomerID",
            "status": "pass" if not duplicate_customer_ids else "fail",
            "details": {
                "duplicate_count": len(duplicate_customer_ids),
                "duplicate_keys": sorted(duplicate_customer_ids),
            },
        }
    ]


def validate_weather_city_match(city_weather_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    mismatches = []

    for row in city_weather_rows:
        requested_city = normalize_text(row.get("City"))
        requested_country = normalize_text(row.get("Country"))
        resolved_country_code = normalize_text(row.get("ResolvedCountryCode"))
        expected_country_code = COUNTRY_CODE_MAP.get(requested_country or "")

        if resolved_country_code and expected_country_code and resolved_country_code != expected_country_code:
            mismatches.append(
                {
                    "city": requested_city,
                    "country": requested_country,
                    "expected_country_code": expected_country_code,
                    "resolved_country_code": resolved_country_code,
                }
            )

    return [
        {
            "check": "weather_city_match_validation",
            "target": "city_weather",
            "status": "pass" if not mismatches else "warn",
            "details": {
                "mismatch_count": len(mismatches),
                "sample_mismatches": mismatches[:5],
            },
        }
    ]


def validate_region_mapping_coverage(enriched_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    rows_missing_city = 0
    rows_missing_region = 0

    for row in enriched_rows:
        city = normalize_text(row.get("City"))
        region = normalize_text(row.get("RegionStarting2016"))
        if not city:
            rows_missing_city += 1
            continue
        if not region:
            rows_missing_region += 1

    return [
        {
            "check": "region_mapping_coverage_validation",
            "target": "customers_weather_region",
            "status": "pass" if rows_missing_region == 0 else "warn",
            "details": {
                "rows_missing_city": rows_missing_city,
                "rows_missing_region_for_present_city": rows_missing_region,
            },
        }
    ]


def build_summary(results: list[dict[str, object]]) -> dict[str, int]:
    summary = {"pass": 0, "warn": 0, "fail": 0}
    for result in results:
        summary[str(result["status"])] += 1
    return summary


def main() -> None:
    configure_logging()

    with sqlite3.connect(DB_PATH) as conn:
        customer_schema = fetch_table_schema(conn, "Customers")
        order_schema = fetch_table_schema(conn, "Orders")

    region_rows = load_region_mapping_rows(REGION_MAPPING_XLSX_PATH)
    enriched_rows = read_csv_rows(ENRICHED_DATASET_PATH)
    city_weather_rows = read_csv_rows(CITY_WEATHER_PATH)

    results = []
    results.extend(validate_table_schema(customer_schema, EXPECTED_CUSTOMER_COLUMNS, "Customers"))
    results.extend(validate_table_schema(order_schema, EXPECTED_ORDER_COLUMNS, "Orders"))
    results.extend(validate_region_mapping_schema(region_rows))
    results.extend(validate_enriched_schema(enriched_rows))
    results.extend(validate_missing_values(enriched_rows))
    results.extend(validate_duplicates(enriched_rows))
    results.extend(validate_weather_city_match(city_weather_rows))
    results.extend(validate_region_mapping_coverage(enriched_rows))

    report = {
        "summary": build_summary(results),
        "results": results,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    QUALITY_REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    logging.info("Data quality checks completed")
    logging.info("Summary: %s", report["summary"])
    logging.info("Saved data quality report to %s", QUALITY_REPORT_PATH)


if __name__ == "__main__":
    main()
