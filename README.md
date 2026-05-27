# Industrial IoT Lakehouse Pipeline (Medallion Architecture)

[![Python Data Pipeline CI](https://github.com/<your-username>/industrial-iot-lakehouse/actions/workflows/python-ci.yml/badge.svg)](https://github.com/<your-username>/industrial-iot-lakehouse/actions/workflows/python-ci.yml)
[![Spark Version](https://img.shields.io/badge/Apache%20Spark-3.4.1-orange?logo=apachespark)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4.0-blue)](https://delta.io/)

A production-grade, end-to-end Big Data pipeline engineered using **PySpark** and **Delta Lake** over a simulated Cloud Data Lake environment. The project processes real-world industrial time-series sensor telemetry data leveraging the **Medallion Architecture** to deliver clean, business-ready analytical datasets optimized via physical data tuning strategies like **Z-Ordering**.

---

## Architecture & Emulation Strategy

To showcase an enterprise-level Cloud Data Lake infrastructure (such as Azure Databricks + Azure Data Lake Storage Gen2) entirely on a local development machine, this architecture decouples storage, compute, and configuration following the **Twelve-Factor App** principles:

- **Compute & Acid Engine**: PySpark acting as the distributed analytics engine coupled with Delta Lake core to guarantee transactional integrity (ACID properties), Schema Enforcement, and Time-Travel debugging capabilities.
- **Storage Emulation (S3A)**: A local **MinIO** containerized instance providing an S3-compatible API to simulate enterprise cloud blob storage.
- **Local File System Compliance**: Embedded native Hadoop binaries (`winutils.exe` and `hadoop.dll`) mapping POSIX file permissions directly onto the native OS storage layers to ensure transaction log consistency.
- **Environment Isolation**: Application paths, credentials, and run environments are decoupled completely using `.env` abstraction, making the codebase entirely cloud-agnostic.

---

## Medallion Pipeline Implementation

Data flows sequentially through three data maturity layers within the Lakehouse:

### 1. Bronze Layer (Immutable Raw Ingestion)
- **Source**: Raw streaming-simulated CSV file from the *AI4I 2020 Predictive Maintenance Dataset* containing machine operational parameters (temperatures, RPM, torque, tool wear, and discrete mechanical failures).
- **Engineering Pattern**: Ingestion is purely *Append-Only*. A regular-expression metadata sanitizer dynamically strips structural naming anomalies, whitespaces, and system-breaking characters (`[ ] , ;`) from incoming metadata.
- **Audit Logging**: Every record is enriched with data lineage markers: `_ingested_at`, `_source_file_name`, and `_row_status`.

### 2. Silver Layer (Cleanse, Standardize & Deduplicate)
- **Deduplication**: Resolves network-level or sensor-level data packet repetition by executing an Analytical Windowing function (`row_number() over (partition by device_id, row_id order by _ingested_at desc)`).
- **Idempotency**: Downstream processing jobs are protected against multiple executions using a programmatic Delta `MERGE INTO` statement (Upsert logic).
- **Type Safety**: Strictly casts incoming values to their designated schemas (`double`, `int`, `string`).

### 3. Gold Layer (Analytical & Business Feature Store)
Calculates analytical value tailored for Business Intelligence dashboards (e.g., Power BI) and Predictive Maintenance ML models. It splits data into a dimensional star-schema style output:
- **`agg_device_health`**: Computes dynamic moving averages for core telemetry variables (process temperature, torque thresholds) grouped by equipment type.
- **`fct_failures`**: An optimized Fact Table logging exact execution blocks where failure flags were triggered.
- **Performance Optimization**: Automatically invokes Delta Lake physical file compaction and performance optimization through **Z-ORDER BY** clustering indexes on high-cardinality keys (`device_id`, `device_type`), drastically boosting file skipping performance for analytics queries.

---

## Repository Structure

```text
industrial-iot-lakehouse/
│
├── .github/workflows/
│   └── python-ci.yml         # GitHub Actions CI (Linter + Automated PyTest with Java Setup)
├── config/
│   └── spark_manager.py      # Singleton manager initiating optimized Delta Session & Hadoop Context
├── data/                     # Local Data Lake Simulation (Git-ignored)
│   ├── landing/              # Raw telemetry files dropped by the source system
│   └── delta/                # Delta tables divided by layers (Bronze, Silver, Gold)
├── docker/
│   └── docker-compose.yml    # MinIO infrastructure configuration
├── src/                      # Source Core Logic
│   ├── common/delta_utils.py # Generic Delta operations abstraction (Write, Upsert / Merge)
│   ├── bronze/               # Raw schema-sanitized loaders
│   ├── silver/               # Cleansing, windowing, and schema verification engines
│   └── gold/                 # Analytical aggregations and fact table metrics generators
├── tests/
│   ├── conftest.py          # Session-scoped isolated Spark Context fixture for local testing
│   └── test_transformers.py # Unit tests evaluating deduplication logic and data transformers
├── .env.example              # Public structural template for environment configurations
├── main.py                   # Master Pipeline Orchestrator & entry point
└── requirements.txt         # System dependencies with explicit frozen versions