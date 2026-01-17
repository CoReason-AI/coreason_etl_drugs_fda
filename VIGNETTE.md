# The Architecture and Utility of coreason-etl-drugs-fda

### 1. The Philosophy (The Why)

In the complex landscape of Life Sciences data, **Drugs@FDA** stands as the definitive legal "Source of Truth" for approved drug products. However, accessing this truth is often fraught with friction: static ZIP files, legacy encodings, and disconnected relational tables. The `coreason-etl-drugs-fda` package was built to bridge the gap between this raw government data and the high-precision needs of a modern **Knowledge Graph**.

We adhere strictly to a **"Borrow to Build"** philosophy. Rather than reinventing extraction logic or writing brittle custom parsers, we leverage the best-in-class open-source ecosystem. This package is not just a loader; it is a **Reasoning Enabler**. By resolving entities, tracking regulatory status (including patent exclusivity), and strictly typing data, it allows downstream consumers—from Data Scientists to Compliance Auditors—to answer complex questions like *"Find all discontinued cardiovascular drugs approved before 2000"* with confidence. It transforms a static regulatory dump into a dynamic, queryable asset.

### 2. Under the Hood (The Dependencies & logic)

The package's architecture is a testament to the power of a modern Python data stack, selected to balance performance, reliability, and type safety:

*   **`dlt` (Data Load Tool):** The backbone of our pipeline. `dlt` handles the complexity of schema evolution (e.g., if the FDA adds a new column next Tuesday, we handle it automatically), state management, and destination loading. It allows us to treat the FDA's ZIP file as a streaming source rather than a monolithic download.
*   **`curl_cffi`:** A critical component for reliable access. Standard HTTP libraries often fail against modern bot detection systems. We use `curl_cffi` to impersonate a browser, ensuring we can reliably fetch the data without being blocked by FDA's TLS fingerprinting.
*   **`polars`:** The engine of our "Refinery". We use Polars for its blazing-fast, memory-efficient processing of the TSV streams. It handles the heavy lifting of the **Silver Layer** logic:
    *   **Date Normalization:** Handling legacy artifacts like "Approved prior to Jan 1, 1982" by converting them to ISO-standard dates while flagging them as historic.
    *   **ID Standardization:** Padding `ApplNo` and `ProductNo` to strict 6- and 3-digit formats to ensure reliable joins.
    *   **Ingredient Parsing:** Splitting and cleaning the often-messy `ActiveIngredient` strings into structured arrays.
*   **`pydantic`:** Ensures that data entering our Silver Layer adheres to a strict contract. If a record doesn't match our schema, it fails fast, preventing data swamp issues.

The logic follows a **Medallion Architecture**:
1.  **Bronze:** A raw, lossless dump of the FDA's TSV files (Products, Applications, Submissions, Exclusivity).
2.  **Silver:** Cleaned, typed entities with deterministic UUIDs (`coreason_id`). This layer handles the complex business logic of joining "Submissions" to get true approval dates and "Exclusivity" to determine patent protection.
3.  **Gold:** A denormalized "One Big Table" (`dim_drug_product`) ready for immediate analysis and graph projection.

### 3. In Practice (The How)

The package is designed to be a "Zero-Maintenance" component. Here is how it operates in a production setting.

**The Happy Path: executing the Pipeline**

Running the pipeline is as simple as invoking the `dlt` pipeline runner. This single entry point orchestrates the download, extraction, transformation, and loading processes.

```python
import dlt
from coreason_etl_drugs_fda.source import drugs_fda_source

def run_fda_pipeline():
    # 1. Define the pipeline connecting to your destination (e.g., Postgres, BigQuery)
    pipeline = dlt.pipeline(
        pipeline_name="coreason_drugs_fda",
        destination="postgres",
        dataset_name="fda_data"
    )

    # 2. Initialize the source
    # This automatically handles the ZIP download, streaming extraction,
    # and Medallion transformations (Bronze -> Silver -> Gold).
    source = drugs_fda_source()

    # 3. Run the pipeline
    info = pipeline.run(source)
    print(info)

if __name__ == "__main__":
    run_fda_pipeline()
```

**Underlying Transformation Power**

Behind the scenes, we use Polars to enforce business logic elegantly. For example, here is how we handle the tricky "Approved prior to 1982" legacy date string:

```python
import polars as pl
from datetime import date

def fix_dates(df: pl.LazyFrame, col_name: str) -> pl.LazyFrame:
    """
    Standardizes FDA date columns, handling the 'Approved prior to Jan 1, 1982' artifact.
    """
    legacy_str = "Approved prior to Jan 1, 1982"
    legacy_date = date(1982, 1, 1)

    return df.with_columns(
        pl.when(pl.col(col_name) == legacy_str)
        .then(pl.lit(legacy_date))
        .otherwise(
            pl.col(col_name).str.to_date(format="%Y-%m-%d", strict=False)
        )
        .alias(col_name)
    )
```

This ensures that downstream consumers never have to parse "magic strings" and can rely on standard Date types for all temporal analysis.
