# Northwind Weather ETL Case Study

This repository contains an end-to-end ETL solution for the Data Engineering case study. It is built on top of the original `northwind-SQLite3` sample repository, but the ETL workflow, Docker setup, orchestration, data quality checks, and sample outputs in this project are custom.

## What It Does

The pipeline:

1. Builds the source Northwind SQLite database from the upstream repo build process.
2. Extracts `Orders` and `Customers` data.
3. Fetches current weather for unique customer cities from OpenWeatherMap.
4. Loads and normalizes the provided `region_mapping.xlsx`.
5. Enriches customer-weather data with region mapping.
6. Runs schema and data quality checks.
7. Loads the final curated data into a target SQLite warehouse.

## Project Structure

- `src/extract.py`: source extraction and initial customer-city enrichment
- `src/weather.py`: OpenWeatherMap integration and customer-weather enrichment
- `src/region_mapping.py`: region mapping load, enrichment, and region-level aggregation
- `src/data_quality.py`: schema validation and data validation checks
- `src/load.py`: loads curated outputs into `dist/warehouse.db`
- `src/pipeline.py`: Prefect orchestration flow
- `Dockerfile`: container image definition
- `docker-compose.yml`: simple run modes for one-off and scheduled execution
- `samples_output/`: sample deliverable outputs

## Prerequisites

- Docker installed
- Docker Compose available as `docker compose`
- An OpenWeatherMap API key

Optional for local non-Docker runs:

- Python 3.12-compatible environment
- `pip install -r requirements.txt`

## Configuration

Create a local `.env` file in the project root.

Example:

```env
OPENWEATHER_API_KEY=your_openweather_api_key
DO_NOT_TRACK=1
PREFECT_SERVER_ANALYTICS_ENABLED=false
PREFECT_SCHEDULE_CRON=0 7 * * *
```

Notes:

- `OPENWEATHER_API_KEY` is required.
- `DO_NOT_TRACK=1` disables Prefect client telemetry.
- `PREFECT_SERVER_ANALYTICS_ENABLED=false` disables Prefect temporary server analytics noise.
- `PREFECT_SCHEDULE_CRON` is only used by the scheduled Docker service.

## How to Build and Run

### Build the Docker image

```bash
docker compose build
```

### Run the ETL once

```bash
docker compose up etl
```

This will automatically:

- build the source Northwind DB with `make build`
- run the Prefect ETL flow
- write outputs under `dist/`

### Run the ETL on a schedule

```bash
docker compose up etl-scheduled
```

The scheduled service uses `PREFECT_SCHEDULE_CRON` from `.env`. The default sample value is:

```env
PREFECT_SCHEDULE_CRON=0 7 * * *
```

That means the workflow is scheduled daily at 07:00.

## How to Trigger and Monitor the Workflow

### Trigger

- One-off run: `docker compose up etl`
- Scheduled run: `docker compose up etl-scheduled`

### Monitor

The workflow is orchestrated with Prefect. During container execution, task and flow logs are printed directly to the container logs.

Useful commands:

```bash
docker compose logs etl
docker compose logs -f etl
docker compose logs -f etl-scheduled
```

## Outputs

Main runtime outputs are written under `dist/`:

- `dist/extract/customer_city.csv`
- `dist/extract/orders_with_customer_city.csv`
- `dist/weather/city_weather.csv`
- `dist/weather/customers_with_weather.csv`
- `dist/region/region_mapping.csv`
- `dist/region/customers_weather_region.csv`
- `dist/region/weather_by_region.csv`
- `dist/quality/data_quality_report.json`
- `dist/warehouse.db`

Sample deliverable outputs are included under `samples_output/`:

- `samples_output/customers_weather_region.csv`
- `samples_output/data_quality_report.json`
- `samples_output/weather_by_region.csv`

## Tool Choices

- Extraction: Python `sqlite3`, `csv`
- API integration: Python `urllib` with OpenWeatherMap current weather API and geocoding API
- Excel handling: direct `.xlsx` parsing with `zipfile` and `xml.etree.ElementTree`
- Data quality: custom validation logic in Python
- Orchestration: Prefect
- Target warehouse: SQLite
- Containerization: Docker and Docker Compose

Why these choices:

- `sqlite3` was used for database access because the source system is already SQLite, the target warehouse can also be SQLite, and this keeps the solution reproducible without extra infrastructure.
- `urllib` was used for API calls to avoid adding an unnecessary HTTP client dependency for a relatively simple REST integration.
- Custom Python-based data quality checks were chosen instead of a heavier framework because the validation requirements are specific, compact, and easy to express directly in code.
- Prefect was selected for orchestration because it is Python-native and adds clear task separation, retries, logging, and optional scheduling with much less setup than Airflow.
- Standard library-heavy implementation kept the project lightweight and Docker-friendly.

## Data Quality Checks

The pipeline validates:

- required source schema for `Customers` and `Orders`
- region mapping schema
- enriched dataset schema before load
- missing values in key fields
- duplicate customer records in the enriched dataset
- weather-country consistency
- region mapping coverage for available customer country data

The data quality report is written to:

- `dist/quality/data_quality_report.json`

## Known Data Issues

The pipeline intentionally surfaces source and integration issues instead of hiding them.

- Two customer records in the source data are missing `City` and `Country`.
- One city lookup, `Tsawassen, Canada`, does not resolve through the weather geocoding step and is reported as a controlled warning.
- These issues are reflected in the generated data quality report and do not crash the pipeline.

## Challenges and Decisions

### 1. Region mapping file vs. task wording

The task wording suggests city-based region enrichment, but the provided `region_mapping.xlsx` is country-based. Because the reference file does not contain a `City` column, region enrichment and coverage validation were implemented on `Country`, while customer `City` was preserved in the enriched dataset.

This means:

- weather integration is still performed at the `City + Country` level
- region enrichment is performed at the `Country` level
- the enriched output still retains city-level detail for downstream analysis

### 2. Weather lookup reliability

Simple city-name lookup was not reliable enough for all Northwind locations. To reduce mismatches, the weather integration first geocodes `City + Country` and then requests current weather by latitude and longitude.

### 3. Source data quality issues

The source data includes customers with missing city and country values. These are intentionally not silently dropped; they are logged and surfaced in the data quality report.

### 4. Docker reproducibility

Instead of re-implementing the upstream database bootstrap logic, the pipeline calls `make build` so the container follows the original repository build contract.

## Workflow Reliability

The workflow is orchestrated as explicit Prefect tasks in this order:

1. source database build
2. extraction
3. weather integration
4. region mapping enrichment
5. data quality validation
6. target loading

Reliability measures included in the workflow:

- task-level logging for each stage
- retries on the weather task because it depends on an external API
- fail-fast behavior for real pipeline failures
- controlled warnings for known data issues and non-blocking lookup mismatches

## Local Python Run

If you want to run the flow without Docker:

```bash
python src/pipeline.py
```

This requires a local Python environment with:

```bash
python -m pip install -r requirements.txt
```

## Repository Note

The original Northwind sample database structure and upstream repository metadata remain in this repository because this solution is built on top of that base project. The ETL implementation and case study deliverables are the custom part of this work.
