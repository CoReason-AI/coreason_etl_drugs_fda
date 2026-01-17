# API Reference

This section provides a reference for the core modules in the `coreason_etl_drugs_fda` package.

## Pipeline (`coreason_etl_drugs_fda.pipeline`)

The entry point for the ETL process.

### `create_pipeline`

```python
def create_pipeline(destination: str = "postgres", dataset_name: str = "fda_data") -> dlt.Pipeline
```

Creates and configures the `dlt` pipeline object.
- **destination**: The destination name (default: "postgres").
- **dataset_name**: The target dataset/schema name (default: "fda_data").

### `run_pipeline`

```python
def run_pipeline() -> None
```

The main execution function.
- Initializes the pipeline.
- Instantiates the `drugs_fda_source`.
- Runs the pipeline.
- Organizes schemas post-load.

## Source (`coreason_etl_drugs_fda.source`)

Contains the logic for extracting data from the FDA website.

### `drugs_fda_source`

```python
@dlt.source(name="drugs_fda")
def drugs_fda_source(base_url: str = ...) -> Iterator[DltResource]
```

The main DLT source.
- Downloads the ZIP file using `curl_cffi` to bypass bot detection.
- Yields:
    - Bronze resources (Raw files like `fda_drugs_bronze_products`).
    - Silver resources (`fda_drugs_silver_products`).
    - Gold resources (`fda_drugs_gold_products`).

## Silver Layer (`coreason_etl_drugs_fda.silver`)

Defines the schema and logic for the Silver layer.

### `ProductSilver` (Pydantic Model)

Schema for the Silver Products table.

- `coreason_id`: UUIDv5 (derived from ApplNo + ProductNo).
- `source_id`: String (ApplNo + ProductNo).
- `appl_no`: String (6 digits).
- `product_no`: String (3 digits).
- `form`: Title cased string.
- `strength`: String.
- `active_ingredients_list`: List[str].
- `original_approval_date`: Date.
- `is_historic_record`: Boolean.
- `hash_md5`: String.

## Gold Layer (`coreason_etl_drugs_fda.gold`)

Defines the schema and logic for the Gold layer.

### `ProductGold` (Pydantic Model)

Schema for the Gold Products table (One Big Table).

- Inherits core fields from Silver.
- Adds enriched fields:
    - `sponsor_name`
    - `appl_type`
    - `marketing_status_description`
    - `te_code`
    - `is_generic`
    - `is_protected`
    - `search_vector`

## Transformations (`coreason_etl_drugs_fda.transform`)

Contains Polars transformation functions.

### `prepare_silver_products`

```python
def prepare_silver_products(products_lazy, dates_lazy, ...) -> pl.LazyFrame
```

Constructs the Silver layer DataFrame by joining products with approval dates and normalizing IDs.

### `prepare_gold_products`

```python
def prepare_gold_products(silver_df, df_apps, df_marketing, ...) -> pl.LazyFrame
```

Constructs the Gold layer DataFrame by joining Silver products with all auxiliary tables.
