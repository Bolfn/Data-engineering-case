from __future__ import annotations

import csv
import logging
import sqlite3
import sys
from pathlib import Path


DB_PATH = Path("dist/northwind.db")
OUTPUT_DIR = Path("dist/extract")

REQUIRED_CUSTOMER_COLUMNS = {
    "CustomerID",
    "CompanyName",
    "City",
    "Country",
}

REQUIRED_ORDER_COLUMNS = {
    "OrderID",
    "CustomerID",
    "OrderDate",
    "RequiredDate",
    "ShippedDate",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )


def validate_source_exists(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Northwind database not found at {db_path}")


def fetch_table(conn: sqlite3.Connection, table_name: str) -> tuple[list[str], list[dict[str, object]]]:
    cursor = conn.execute(f"SELECT * FROM [{table_name}]")
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return columns, rows


def validate_columns(columns: list[str], required_columns: set[str], table_name: str) -> None:
    missing_columns = required_columns - set(columns)
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(f"{table_name} is missing required columns: {missing_list}")


def normalize_text(value: object) -> object:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_customer_city(customers_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    customer_city_rows = []
    for row in customers_rows:
        customer_city_rows.append(
            {
                "CustomerID": row["CustomerID"],
                "CompanyName": row["CompanyName"],
                "City": normalize_text(row["City"]),
                "Country": normalize_text(row["Country"]),
            }
        )
    return customer_city_rows


def build_orders_with_customer_city(
    orders_rows: list[dict[str, object]],
    customer_city_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    customers_by_id = {
        row["CustomerID"]: row
        for row in customer_city_rows
    }
    enriched_orders = []
    for row in orders_rows:
        customer = customers_by_id.get(row["CustomerID"], {})
        enriched_row = dict(row)
        enriched_row["CompanyName"] = customer.get("CompanyName")
        enriched_row["City"] = customer.get("City")
        enriched_row["Country"] = customer.get("Country")
        enriched_orders.append(enriched_row)
    return enriched_orders


def log_data_quality(
    customer_city_rows: list[dict[str, object]],
    orders_enriched_rows: list[dict[str, object]],
) -> None:
    missing_customer_city_rows = [row for row in customer_city_rows if not row["City"]]
    duplicate_customer_ids = len(customer_city_rows) - len(
        {row["CustomerID"] for row in customer_city_rows}
    )
    orders_without_city = sum(1 for row in orders_enriched_rows if not row["City"])
    orders_without_customer = sum(
        1 for row in orders_enriched_rows if not row["CompanyName"]
    )

    logging.info("Customers extracted: %s", len(customer_city_rows))
    logging.info("Orders extracted: %s", len(orders_enriched_rows))
    logging.info("Customers with missing city: %s", len(missing_customer_city_rows))
    logging.info("Duplicate CustomerID values: %s", duplicate_customer_ids)
    logging.info("Orders without matching customer: %s", orders_without_customer)
    logging.info("Orders with missing customer city: %s", orders_without_city)

    if missing_customer_city_rows:
        logging.warning(
            "Customers missing city:\n%s",
            "\n".join(str(row) for row in missing_customer_city_rows),
        )


def save_csv(rows: list[dict[str, object]], output_path: Path) -> None:
    if not rows:
        raise ValueError(f"No rows available for {output_path.name}")
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def save_outputs(
    customer_city_rows: list[dict[str, object]],
    orders_enriched_rows: list[dict[str, object]],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_csv(customer_city_rows, OUTPUT_DIR / "customer_city.csv")
    save_csv(orders_enriched_rows, OUTPUT_DIR / "orders_with_customer_city.csv")


def main() -> None:
    configure_logging()
    validate_source_exists(DB_PATH)

    with sqlite3.connect(DB_PATH) as conn:
        customer_columns, customer_rows = fetch_table(conn, "Customers")
        order_columns, order_rows = fetch_table(conn, "Orders")

    validate_columns(customer_columns, REQUIRED_CUSTOMER_COLUMNS, "Customers")
    validate_columns(order_columns, REQUIRED_ORDER_COLUMNS, "Orders")

    customer_city_rows = normalize_customer_city(customer_rows)
    orders_enriched_rows = build_orders_with_customer_city(order_rows, customer_city_rows)

    log_data_quality(customer_city_rows, orders_enriched_rows)
    save_outputs(customer_city_rows, orders_enriched_rows)

    logging.info("Saved extracted datasets to %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
