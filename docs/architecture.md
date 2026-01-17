# Architecture

This document outlines the architectural design of the `coreason_etl_drugs_fda` package.

## Medallion Architecture

The pipeline follows the Medallion Architecture pattern to organize data quality levels.

### Bronze (Raw)

*   **Source**: Drugs@FDA ZIP file downloaded from the FDA website.
*   **Content**: Raw CSV/TXT files exactly as they appear in the source archive.
*   **Format**: Loaded into the database with `fda_drugs_bronze_` prefix. Column names are snake_cased but values are untouched.
*   **Purpose**: Immutable historical record and audit trail. Re-ingestion allows reprocessing without re-downloading.

### Silver (Cleaned & Conformed)

*   **Source**: Bronze tables (or direct stream from ZIP).
*   **Transformation**:
    *   **Data Cleaning**: Whitespace stripping, date parsing.
    *   **Normalization**: `ApplNo` padded to 6 digits, `ProductNo` padded to 3 digits.
    *   **Enrichment**:
        *   `coreason_id`: Deterministic UUIDv5 generated from `ApplNo` and `ProductNo`.
        *   `hash_md5`: Row hash for change detection.
        *   `original_approval_date`: Derived from joining with `Submissions` table.
*   **Schema**: Strongly typed using Pydantic models.
*   **Purpose**: Trusted data foundation. Deduplicated and standardized.

### Gold (Enriched & Consumption)

*   **Source**: Silver Products joined with Bronze lookup tables (Applications, MarketingStatus, TE, Exclusivity).
*   **Transformation**:
    *   **Denormalization**: Combines multiple tables into a single wide table (`fda_drugs_gold_products`).
    *   **Business Logic**:
        *   `is_generic`: Derived from Application Type ("ANDA").
        *   `is_protected`: Derived from Exclusivity dates vs today.
        *   `search_vector`: Concatenated text field for search optimization.
*   **Purpose**: Analytics, reporting, and downstream application consumption.

## Tech Stack

### Ingestion: `dlt` (Data Load Tool)

*   Handles extraction, normalization, and loading.
*   Manages schema evolution (`schema_contract={"columns": "evolve"}`).
*   Provides resilience and retry mechanisms.

### Transformation: `Polars`

*   Used for high-performance in-memory data transformation.
*   Processes data from the ZIP stream lazily where possible.
*   Handles complex joins and vector operations (UUID generation) efficiently.

### Validation: `Pydantic`

*   Defines the schema for Silver and Gold resources.
*   Ensures type safety and data integrity at the application level.

### HTTP Client: `curl_cffi`

*   Used to bypass FDA's TLS fingerprinting bot detection.
*   Impersonates Chrome 120 to successfully download the data.

## Key Design Decisions

### "Borrow to Build"

We prioritize using established libraries (`dlt`, `polars`) over custom code. This reduces maintenance burden and improves reliability.

### UUIDv5 Stability

IDs are generated using a stable namespace (`uuid.uuid5(NAMESPACE_FDA, f"{ApplNo}|{ProductNo}")`). This ensures that the same drug always gets the same ID across different runs, enabling idempotent updates.

### Historic Date Handling

The FDA dataset contains legacy strings like "Approved prior to Jan 1, 1982". These are converted to `1982-01-01`, and a flag `is_historic_record` is set to True to preserve the semantic meaning while allowing strictly typed Date columns.
