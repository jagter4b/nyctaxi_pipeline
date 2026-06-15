# NYC Taxi Data Pipeline

This project is a data pipeline built on **Databricks** using **PySpark**, **Delta Lake**, and **Unity Catalog**. It processes New York City Taxi and Limousine Commission (TLC) trip data using the **Medallion Architecture** (Bronze -> Silver -> Gold).

---

## Data Pipeline Architecture

The pipeline moves data through three maturity layers:

![Medallion Architecture](docs/medallion_final.gif)

### 1. Landing Layer (`data_sources` Volume)
Stores raw files exactly as they are downloaded:
- Taxi trip records: `.parquet` files
- Taxi zone lookup: `.csv` files

### 2. Bronze Layer (`01_bronze` schema)
- Table: `yellow_trips_raw`
- Ingests files from the landing volume and adds a processing timestamp (`processed_timestamp`).

### 3. Silver Layer (`02_silver` schema)
Cleans and formats the data:
- **`taxi_zone_lookup`**: Cleans headers and prepares lookup zones.
- **`yellow_trips_cleansed`**: Converts vendor, payment, and rate IDs to human-readable text (e.g., Curb Mobility, Credit Card). It also filters out incorrect dates and calculates trip duration in minutes.
- **`yellow_trips_enriched`**: Joins the cleansed trips table with the zone lookup table to map pickup and dropoff locations to actual borough and zone names.

### 4. Gold Layer (`03_gold` schema)
- Table: `daily_trip_summary`
- Aggregates the enriched trip records by pickup date. It calculates metrics such as total trips, average distance, average fare, and total revenue per day.

---

## Ingestion Modes

### 1. Historical Backfill
Processes 6 months of historical data from **October 2025 to March 2026** by overwriting existing tables. This creates a clean initial baseline.

### 2. Monthly Incremental Load
Runs on a schedule to process **1 month of data at a time** in append mode. To account for a typical 2-month vendor delay, it pulls data from 2 months prior (for example, a run in June 2026 processes April 2026 data). It also includes checks to avoid downloading files that were already processed.

---

## Workflow Orchestration

The pipeline is automated using a **Databricks Workflow Job**.

![Orchestration DAG](docs/orchestration.png)

### Tasks:
- **`00_ingest_lookup`** / **`00_ingest_yellow_trips`**: Download files from the web to the landing volume.
- **`continue_downstream_lookup`** / **`continue_downstream_yellow`**: Simple check nodes that stop execution if no new data is found.
- **`01_yellow_trips_raw`**: Ingests raw files to the Bronze layer.
- **`02_taxi_zone_lookup`** / **`02_yellow_trips_cleansed`**: Transforms files to the Silver layer.
- **`02_yellow_trips_enriched`**: Combines cleansed trips with lookup zones.
- **`03_daily_trip_summary`**: Aggregates data and saves to the Gold layer.

---

## Repository Structure

```text
nyctaxi_pipeline/
├── ad_hoc/                                     # Exploratory analysis notebook
│   └── yellow_taxi_eda.ipynb
├── docs/                                       # Images and documentation assets
│   ├── medallion_final.gif
│   └── orchestration.png
├── one_off/                                    # Initial deployment and catalog setup DDL
│   ├── creating_catalogs_schema_volume.ipynb
│   └── initial_load/                           # Historical baseline loading notebooks
│       └── notebooks/
│           ├── 00_landing/
│           ├── 01_bronze/
│           ├── 02_silver/
│           └── 03_gold/
├── transformations/                            # Production incremental notebooks
│   └── notebooks/
│       ├── 00_landing/
│       ├── 01_bronze/
│       ├── 02_silver/
│       └── 03_gold/
├── .gitignore
└── README.md
```

---

## How to Run the Pipeline

1. **Setup Catalog & Schemas:** Run the DDL notebook `one_off/creating_catalogs_schema_volume.ipynb` to create the catalog `nyctaxi`, target schemas, and storage volume.
2. **Run Historical Load:** Execute the notebooks inside `one_off/initial_load/notebooks/` to load the initial October 2025 to March 2026 data.
3. **Deploy the Scheduled Job:** Create a Databricks Job using the notebooks inside the `transformations/` folder, configured to run monthly.
