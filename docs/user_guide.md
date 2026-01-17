# User Guide

This guide describes how to set up, configure, and run the **coreason_etl_drugs_fda** pipeline.

## Prerequisites

Before starting, ensure you have the following installed:

*   **Python 3.12+**
*   **Poetry** (Python dependency manager)
*   **PostgreSQL** (Optional, but recommended as the default destination)

## Installation

1.  **Clone the Repository**:
    ```bash
    git clone https://github.com/CoReason-AI/coreason_etl_drugs_fda.git
    cd coreason_etl_drugs_fda
    ```

2.  **Install Dependencies**:
    ```bash
    poetry install
    ```

## Configuration

The pipeline uses `dlt` for configuration. You can configure the destination using environment variables or a `secrets.toml` file (not committed).

### Environment Variables

Set the following environment variables for a PostgreSQL destination:

```bash
export DESTINATION__POSTGRES__CREDENTIALS="postgresql://user:password@localhost:5432/db_name"
```

For development, you can also use `duckdb` (default for some dlt setups if not specified, but the code defaults to `postgres`).

### Pipeline Options

You can modify the default behavior by passing arguments to the `create_pipeline` function in `src/coreason_etl_drugs_fda/pipeline.py` or by modifying the source configuration.

## Running the Pipeline

To execute the full ETL pipeline:

```bash
poetry run python -m coreason_etl_drugs_fda.pipeline
```

### What Happens When It Runs?

1.  **Download**: The `drugs_fda_source` downloads the latest ZIP file from the FDA website. It uses `curl_cffi` to impersonate a browser.
2.  **Bronze Loading**: The raw text files inside the ZIP (`Products.txt`, `Applications.txt`, etc.) are loaded into the destination as `fda_drugs_bronze_<filename>`.
3.  **Silver Transformation**:
    *   `Products` are joined with `Submissions` to get original approval dates.
    *   IDs (`ApplNo`, `ProductNo`) are normalized.
    *   `coreason_id` (UUIDv5) is generated.
    *   Data is loaded as `fda_drugs_silver_products`.
4.  **Gold Transformation**:
    *   Data is denormalized by joining Applications, Marketing Status, TE Codes, and Exclusivity data.
    *   Business logic is applied (e.g., `is_generic`, `is_protected`).
    *   Data is loaded as `fda_drugs_gold_products`.
5.  **Post-Load**: Schemas in Postgres are organized (if applicable).

## Verifying Output

Check the logs in the console or in `logs/app.log`. You should see messages indicating the successful loading of resources.

If using Postgres, you can query the tables:

```sql
SELECT * FROM fda_data.fda_drugs_gold_products LIMIT 10;
```
