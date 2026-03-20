from __future__ import annotations

import csv
import logging
import sys
from collections import defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REGION_MAPPING_XLSX_PATH = PROJECT_ROOT / "region_mapping.xlsx"
CUSTOMERS_WITH_WEATHER_PATH = PROJECT_ROOT / "dist/weather/customers_with_weather.csv"
OUTPUT_DIR = PROJECT_ROOT / "dist/region"
REGION_MAPPING_CSV_PATH = OUTPUT_DIR / "region_mapping.csv"
CUSTOMERS_WEATHER_REGION_PATH = OUTPUT_DIR / "customers_weather_region.csv"
WEATHER_BY_REGION_PATH = OUTPUT_DIR / "weather_by_region.csv"

EXPECTED_HEADERS = {
    "Country",
    "Region starting 2016",
    "Region until 2017",
}

XML_NAMESPACES = {
    "sheet": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
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


def save_csv(rows: list[dict[str, object]], output_path: Path) -> None:
    if not rows:
        raise ValueError(f"No rows available for {output_path.name}")

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def load_shared_strings(zip_file: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zip_file.namelist():
        return []

    shared_strings_tree = ET.fromstring(zip_file.read("xl/sharedStrings.xml"))
    shared_strings = []

    for string_item in shared_strings_tree.findall("sheet:si", XML_NAMESPACES):
        texts = [text_node.text or "" for text_node in string_item.iterfind(".//sheet:t", XML_NAMESPACES)]
        shared_strings.append("".join(texts))

    return shared_strings


def get_first_sheet_path(zip_file: ZipFile) -> str:
    workbook_tree = ET.fromstring(zip_file.read("xl/workbook.xml"))
    relationships_tree = ET.fromstring(zip_file.read("xl/_rels/workbook.xml.rels"))

    relationship_map = {
        relationship.attrib["Id"]: relationship.attrib["Target"]
        for relationship in relationships_tree
    }

    first_sheet = workbook_tree.find("sheet:sheets/sheet:sheet", XML_NAMESPACES)
    if first_sheet is None:
        raise ValueError("Workbook does not contain any worksheet")

    relationship_id = first_sheet.attrib[
        "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    ]
    return f"xl/{relationship_map[relationship_id]}"


def parse_cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    value_node = cell.find("sheet:v", XML_NAMESPACES)
    if value_node is None or value_node.text is None:
        return ""

    raw_value = value_node.text
    if cell.attrib.get("t") == "s":
        return shared_strings[int(raw_value)]
    return raw_value


def load_region_mapping_rows(xlsx_path: Path) -> list[dict[str, str]]:
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Region mapping file not found: {xlsx_path}")

    with ZipFile(xlsx_path) as zip_file:
        shared_strings = load_shared_strings(zip_file)
        sheet_path = get_first_sheet_path(zip_file)
        worksheet_tree = ET.fromstring(zip_file.read(sheet_path))

    row_nodes = worksheet_tree.findall("sheet:sheetData/sheet:row", XML_NAMESPACES)
    if not row_nodes:
        raise ValueError("Region mapping worksheet is empty")

    header_values = [
        parse_cell_value(cell, shared_strings)
        for cell in row_nodes[0].findall("sheet:c", XML_NAMESPACES)
    ]

    if set(header_values) != EXPECTED_HEADERS:
        raise ValueError(
            "Unexpected region mapping headers. "
            f"Expected {sorted(EXPECTED_HEADERS)}, got {sorted(header_values)}"
        )

    rows = []
    for row_node in row_nodes[1:]:
        cell_values = [
            parse_cell_value(cell, shared_strings)
            for cell in row_node.findall("sheet:c", XML_NAMESPACES)
        ]
        if not any(cell_values):
            continue

        row_data = dict(zip(header_values, cell_values))
        rows.append(
            {
                "Country": normalize_text(row_data.get("Country")) or "",
                "RegionStarting2016": normalize_text(row_data.get("Region starting 2016")) or "",
                "RegionUntil2017": normalize_text(row_data.get("Region until 2017")) or "",
            }
        )

    return rows


def validate_region_mapping_rows(region_rows: list[dict[str, str]]) -> None:
    duplicate_countries = len(region_rows) - len({row["Country"] for row in region_rows})
    missing_country_rows = sum(1 for row in region_rows if not row["Country"])

    logging.info("Region mapping rows loaded: %s", len(region_rows))
    logging.info("Duplicate country keys in mapping: %s", duplicate_countries)
    logging.info("Rows missing country key: %s", missing_country_rows)


def enrich_customers_with_regions(
    customer_rows: list[dict[str, str]],
    region_rows: list[dict[str, str]],
) -> list[dict[str, object]]:
    region_by_country = {
        row["Country"]: row
        for row in region_rows
        if row["Country"]
    }

    enriched_rows = []
    unmatched_countries = set()

    for row in customer_rows:
        country = normalize_text(row.get("Country"))
        region = region_by_country.get(country or "", {})

        if country and not region:
            unmatched_countries.add(country)

        enriched_row = dict(row)
        enriched_row.update(
            {
                "RegionStarting2016": region.get("RegionStarting2016"),
                "RegionUntil2017": region.get("RegionUntil2017"),
            }
        )
        enriched_rows.append(enriched_row)

    logging.info("Customer rows enriched with region: %s", len(enriched_rows))
    logging.info("Countries without region match: %s", len(unmatched_countries))

    if unmatched_countries:
        logging.warning(
            "Countries missing from region mapping: %s",
            ", ".join(sorted(unmatched_countries)),
        )

    return enriched_rows


def to_float(value: str | None) -> float | None:
    normalized = normalize_text(value)
    if normalized is None:
        return None
    return float(normalized)


def build_weather_by_region(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped_rows: dict[tuple[str, str], dict[str, object]] = defaultdict(
        lambda: {
            "CustomerCount": 0,
            "CustomersWithWeather": 0,
            "TemperatureSum": 0.0,
        }
    )

    for row in rows:
        region_starting_2016 = row.get("RegionStarting2016") or "UNMAPPED"
        region_until_2017 = row.get("RegionUntil2017") or "UNMAPPED"
        key = (str(region_starting_2016), str(region_until_2017))

        grouped_rows[key]["CustomerCount"] += 1

        temperature = to_float(row.get("TemperatureC"))
        if temperature is not None:
            grouped_rows[key]["CustomersWithWeather"] += 1
            grouped_rows[key]["TemperatureSum"] += temperature

    summary_rows = []
    for (region_starting_2016, region_until_2017), metrics in sorted(grouped_rows.items()):
        customers_with_weather = metrics["CustomersWithWeather"]
        average_temperature = None
        if customers_with_weather:
            average_temperature = round(metrics["TemperatureSum"] / customers_with_weather, 2)

        summary_rows.append(
            {
                "RegionStarting2016": region_starting_2016,
                "RegionUntil2017": region_until_2017,
                "CustomerCount": metrics["CustomerCount"],
                "CustomersWithWeather": customers_with_weather,
                "AverageTemperatureC": average_temperature,
            }
        )

    return summary_rows


def main() -> None:
    configure_logging()

    region_rows = load_region_mapping_rows(REGION_MAPPING_XLSX_PATH)
    validate_region_mapping_rows(region_rows)

    customer_rows = read_csv_rows(CUSTOMERS_WITH_WEATHER_PATH)
    enriched_customer_rows = enrich_customers_with_regions(customer_rows, region_rows)
    weather_by_region_rows = build_weather_by_region(enriched_customer_rows)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_csv(region_rows, REGION_MAPPING_CSV_PATH)
    save_csv(enriched_customer_rows, CUSTOMERS_WEATHER_REGION_PATH)
    save_csv(weather_by_region_rows, WEATHER_BY_REGION_PATH)

    logging.info("Saved normalized region mapping to %s", REGION_MAPPING_CSV_PATH)
    logging.info("Saved customer weather region enrichment to %s", CUSTOMERS_WEATHER_REGION_PATH)
    logging.info("Saved weather by region summary to %s", WEATHER_BY_REGION_PATH)


if __name__ == "__main__":
    main()
