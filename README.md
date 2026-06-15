# NYC Taxi Data Pipeline

This project is a data pipeline built on **Databricks** using **PySpark**, **Delta Lake**, and **Unity Catalog**. It processes yellow taxi data using the **Medallion Architecture** (Bronze -> Silver -> Gold). The project works with six months of data.

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
- **`yellow_trips_cleansed`**: Converts vendor, payment, and rate IDs to text (e.g., Curb Mobility, Credit Card). It filters out incorrect dates and calculates trip duration in minutes.
- **`yellow_trips_enriched`**: Joins the cleansed trips table with the zone lookup table to map pickup and dropoff locations to actual borough and zone names.

### 4. Gold Layer (`03_gold` schema)
- Table: `daily_trip_summary`
- Aggregates the enriched trip records by pickup date. It calculates metrics such as total trips, average distance, average fare, and total revenue per day.

---

## Ingestion Modes

### 1. Historical Backfill
Processes 6 months of historical data from **October 2025 to March 2026** by overwriting existing tables. This creates a clean initial baseline.

### 2. Monthly Incremental Load
Runs on a schedule to process **1 month of data at a time** in append mode. It pulls data from 2 months prior to handle vendor delays (for example, a run in June 2026 processes April 2026 data).

---

## Workflow Orchestration

The pipeline is automated using a **Databricks Workflow Job**.

![Orchestration DAG](docs/orchestration.png)

### Tasks:
- **`00_ingest_lookup`** / **`00_ingest_yellow_trips`**: Download files to the landing volume.
- **`continue_downstream_lookup`** / **`continue_downstream_yellow`**: Stop execution if no new data is found.
- **`01_yellow_trips_raw`**: Ingests raw files to Bronze.
- **`02_taxi_zone_lookup`** / **`02_yellow_trips_cleansed`**: Transforms files to Silver.
- **`02_yellow_trips_enriched`**: Combines cleansed trips with lookup zones.
- **`03_daily_trip_summary`**: Aggregates data to Gold.

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

1. **Setup Catalog & Schemas:** Run the notebook `one_off/creating_catalogs_schema_volume.ipynb` to create the catalog `nyctaxi`, target schemas, and storage volume.
2. **Run Historical Load:** Execute the notebooks inside `one_off/initial_load/notebooks/` to load the 6 months of historical data.
3. **Deploy the Scheduled Job:** Create a Databricks Job using the notebooks inside the `transformations/` folder, configured to run monthly.
