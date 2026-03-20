from __future__ import annotations

import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
CUSTOMER_CITY_PATH = PROJECT_ROOT / "dist/extract/customer_city.csv"
OUTPUT_DIR = PROJECT_ROOT / "dist/weather"
CITY_WEATHER_PATH = OUTPUT_DIR / "city_weather.csv"
CUSTOMERS_WITH_WEATHER_PATH = OUTPUT_DIR / "customers_with_weather.csv"

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
OPENWEATHER_GEOCODING_URL = "https://api.openweathermap.org/geo/1.0/direct"
REQUEST_TIMEOUT_SECONDS = 20

COUNTRY_CODE_MAP = {
    "Argentina": "AR",
    "Austria": "AT",
    "Belgium": "BE",
    "Brazil": "BR",
    "Canada": "CA",
    "Denmark": "DK",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Ireland": "IE",
    "Italy": "IT",
    "Mexico": "MX",
    "Norway": "NO",
    "Poland": "PL",
    "Portugal": "PT",
    "Spain": "ES",
    "Sweden": "SE",
    "Switzerland": "CH",
    "UK": "GB",
    "USA": "US",
    "Venezuela": "VE",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def get_api_key() -> str:
    load_env_file(ENV_PATH)
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is not set in the environment or .env file")
    return api_key


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


def build_unique_locations(customer_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    unique_locations: dict[tuple[str, str], dict[str, str]] = {}

    for row in customer_rows:
        city = normalize_text(row.get("City"))
        country = normalize_text(row.get("Country"))
        if not city or not country:
            continue

        location_key = (city, country)
        if location_key not in unique_locations:
            unique_locations[location_key] = {"City": city, "Country": country}

    return sorted(unique_locations.values(), key=lambda row: (row["Country"], row["City"]))


def get_country_code(country_name: str) -> str:
    country_code = COUNTRY_CODE_MAP.get(country_name)
    if not country_code:
        raise ValueError(f"Missing ISO country code mapping for country: {country_name}")
    return country_code


def fetch_json(url: str) -> Any:
    try:
        with urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return json.load(response)
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {error_body}") from exc
    except URLError as exc:
        raise RuntimeError(str(exc.reason)) from exc


def build_geocoding_url(city: str, country: str, api_key: str) -> str:
    country_code = get_country_code(country)
    query = urlencode(
        {
            "q": f"{city},{country_code}",
            "appid": api_key,
            "limit": 1,
        }
    )
    return f"{OPENWEATHER_GEOCODING_URL}?{query}"


def build_weather_url(latitude: float, longitude: float, api_key: str) -> str:
    query = urlencode(
        {
            "lat": latitude,
            "lon": longitude,
            "appid": api_key,
            "units": "metric",
        }
    )
    return f"{OPENWEATHER_URL}?{query}"


def geocode_location(city: str, country: str, api_key: str) -> dict[str, Any]:
    payload = fetch_json(build_geocoding_url(city, country, api_key))
    if not payload:
        raise RuntimeError(f"No geocoding match returned for {city}, {country}")
    return payload[0]


def fetch_weather_for_location(city: str, country: str, api_key: str) -> dict[str, Any]:
    geocoded_location = geocode_location(city, country, api_key)
    latitude = geocoded_location["lat"]
    longitude = geocoded_location["lon"]
    payload = fetch_json(build_weather_url(latitude, longitude, api_key))

    weather_items = payload.get("weather") or [{}]
    main_section = payload.get("main") or {}
    wind_section = payload.get("wind") or {}
    coord_section = payload.get("coord") or {}
    system_section = payload.get("sys") or {}

    country_code = get_country_code(country)

    return {
        "City": city,
        "Country": country,
        "CountryCode": country_code,
        "RequestedQuery": f"{city},{country_code}",
        "GeocodedName": geocoded_location.get("name"),
        "GeocodedState": geocoded_location.get("state"),
        "ResolvedCity": payload.get("name"),
        "ResolvedCountryCode": system_section.get("country"),
        "Latitude": coord_section.get("lat", latitude),
        "Longitude": coord_section.get("lon", longitude),
        "TemperatureC": main_section.get("temp"),
        "FeelsLikeC": main_section.get("feels_like"),
        "TempMinC": main_section.get("temp_min"),
        "TempMaxC": main_section.get("temp_max"),
        "Pressure": main_section.get("pressure"),
        "Humidity": main_section.get("humidity"),
        "WeatherMain": weather_items[0].get("main"),
        "WeatherDescription": weather_items[0].get("description"),
        "WindSpeed": wind_section.get("speed"),
        "Cloudiness": (payload.get("clouds") or {}).get("all"),
        "WeatherTimestamp": payload.get("dt"),
    }


def build_failed_weather_row(city: str, country: str) -> dict[str, Any]:
    country_code = get_country_code(country)
    return {
        "City": city,
        "Country": country,
        "CountryCode": country_code,
        "RequestedQuery": f"{city},{country_code}",
        "GeocodedName": None,
        "GeocodedState": None,
        "ResolvedCity": None,
        "ResolvedCountryCode": None,
        "Latitude": None,
        "Longitude": None,
        "TemperatureC": None,
        "FeelsLikeC": None,
        "TempMinC": None,
        "TempMaxC": None,
        "Pressure": None,
        "Humidity": None,
        "WeatherMain": None,
        "WeatherDescription": None,
        "WindSpeed": None,
        "Cloudiness": None,
        "WeatherTimestamp": None,
    }


def fetch_all_weather(unique_locations: list[dict[str, str]], api_key: str) -> list[dict[str, Any]]:
    weather_rows = []
    for location in unique_locations:
        city = location["City"]
        country = location["Country"]
        logging.info("Fetching weather for %s, %s", city, country)
        try:
            weather_rows.append(fetch_weather_for_location(city, country, api_key))
        except RuntimeError as exc:
            logging.warning("Weather fetch failed for %s, %s: %s", city, country, exc)
            weather_rows.append(build_failed_weather_row(city, country))
    return weather_rows


def enrich_customers_with_weather(
    customer_rows: list[dict[str, str]],
    weather_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    weather_by_location = {
        (row["City"], row["Country"]): row
        for row in weather_rows
    }

    enriched_rows = []
    for row in customer_rows:
        city = normalize_text(row.get("City"))
        country = normalize_text(row.get("Country"))
        weather = weather_by_location.get((city, country), {})
        enriched_row = dict(row)
        enriched_row.update(
            {
                "TemperatureC": weather.get("TemperatureC"),
                "FeelsLikeC": weather.get("FeelsLikeC"),
                "WeatherMain": weather.get("WeatherMain"),
                "WeatherDescription": weather.get("WeatherDescription"),
                "Humidity": weather.get("Humidity"),
                "WindSpeed": weather.get("WindSpeed"),
                "WeatherTimestamp": weather.get("WeatherTimestamp"),
            }
        )
        enriched_rows.append(enriched_row)
    return enriched_rows


def save_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    if not rows:
        raise ValueError(f"No rows available for {output_path.name}")

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    configure_logging()
    api_key = get_api_key()
    customer_rows = read_csv_rows(CUSTOMER_CITY_PATH)
    unique_locations = build_unique_locations(customer_rows)

    logging.info("Customers loaded: %s", len(customer_rows))
    logging.info("Unique city-country pairs to query: %s", len(unique_locations))

    weather_rows = fetch_all_weather(unique_locations, api_key)
    enriched_customer_rows = enrich_customers_with_weather(customer_rows, weather_rows)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_csv(weather_rows, CITY_WEATHER_PATH)
    save_csv(enriched_customer_rows, CUSTOMERS_WITH_WEATHER_PATH)

    logging.info("Saved city weather data to %s", CITY_WEATHER_PATH)
    logging.info("Saved customer weather enrichment to %s", CUSTOMERS_WITH_WEATHER_PATH)


if __name__ == "__main__":
    main()
