# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import io
import re
import zipfile
from datetime import date
from typing import Any, Dict, Iterator, List

import dlt
import polars as pl
import requests  # type: ignore[import-untyped]
from dlt.sources import DltResource

from coreason_etl_drugs_fda.gold import ProductGold
from coreason_etl_drugs_fda.silver import ProductSilver, generate_coreason_id, generate_row_hash
from coreason_etl_drugs_fda.transform import clean_form, clean_ingredients, fix_dates, normalize_ids
from coreason_etl_drugs_fda.utils.logger import logger

# List of files to extract from the FDA ZIP archive
TARGET_FILES = [
    "Products.txt",
    "Applications.txt",
    "MarketingStatus.txt",
    "TE.txt",
    "Submissions.txt",
    "Exclusivity.txt",
    "MarketingStatus_Lookup.txt",
]


def _to_snake_case(name: str) -> str:
    """Converts a string to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _clean_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the dataframe by:
    1. Converting column names to snake_case.
    2. Stripping leading/trailing whitespace from string columns.
    """
    new_cols = {col: _to_snake_case(col) for col in df.columns}
    df = df.rename(new_cols)

    df = df.with_columns(
        [pl.col(col).str.strip_chars() for col, dtype in zip(df.columns, df.dtypes, strict=True) if dtype == pl.String]
    )
    return df


def _read_csv_bytes(content: bytes) -> pl.DataFrame:
    """Reads CSV bytes into Polars DataFrame with standard settings."""
    if not content:
        return pl.DataFrame()

    return pl.read_csv(
        content,
        separator="\t",
        quote_char=None,
        encoding="cp1252",
        ignore_errors=True,
        truncate_ragged_lines=True,
        infer_schema_length=10000,
    )


def _read_file_from_zip(zip_content: bytes, filename: str) -> Iterator[List[Dict[str, Any]]]:
    """Reads a specific file from the zip content and yields it as dicts."""
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        if filename not in z.namelist():
            return

        with z.open(filename) as f:
            df = _read_csv_bytes(f.read())
            df = _clean_dataframe(df)
            yield df.to_dicts()


def _extract_approval_dates(zip_content: bytes) -> Dict[str, str]:
    """
    Extracts 'ORIG' submission dates from Submissions.txt using Lazy execution.
    Returns: Dict[ApplNo, DateString]
    """
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        if "Submissions.txt" not in z.namelist():
            return {}

        with z.open("Submissions.txt") as f:
            df_eager = _read_csv_bytes(f.read())
            if df_eager.is_empty():
                return {}
            # Convert to lazy for optimization
            df = _clean_dataframe(df_eager).lazy()

            # We can only check columns on lazy frame if schema allows.
            cols = df.collect_schema().names()
            if "submission_type" not in cols or "submission_status_date" not in cols:
                return {}

            df = df.filter(pl.col("submission_type") == "ORIG")

            df = df.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))

            df = df.with_columns(pl.col("submission_status_date").alias("sort_date"))
            df = fix_dates(df, ["sort_date"])

            df = df.sort("sort_date")

            # Unique on subset in lazy mode
            df = df.unique(subset=["appl_no"], keep="first")

            # Finally collect
            rows = df.select(["appl_no", "submission_status_date"]).collect().to_dicts()
            return {row["appl_no"]: row["submission_status_date"] for row in rows if row["submission_status_date"]}


def _create_silver_dataframe(zip_content: bytes) -> pl.LazyFrame:
    """
    Creates the Silver Products DataFrame (shared logic for Silver and Gold).
    Returns a LazyFrame to allow further lazy operations or optimization before collection.
    """
    # 1. Pre-fetch approval dates (this part collects internally)
    approval_map = _extract_approval_dates(zip_content)

    # 2. Read Products
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        if "Products.txt" not in z.namelist():
            # Return empty schema if Products is missing
            return pl.DataFrame().lazy()

        with z.open("Products.txt") as f:
            df_eager = _read_csv_bytes(f.read())
            if df_eager.is_empty():
                # Return empty LazyFrame with expected Silver schema to prevent downstream failures
                return pl.DataFrame(
                    schema={
                        "appl_no": pl.String,
                        "product_no": pl.String,
                        "form": pl.String,
                        "strength": pl.String,
                        "active_ingredients_list": pl.List(pl.String),
                        "original_approval_date": pl.Date,
                        "is_historic_record": pl.Boolean,
                        "coreason_id": pl.String,
                        "source_id": pl.String,
                        "hash_md5": pl.String,
                        "drug_name": pl.String,
                    }
                ).lazy()
            df = _clean_dataframe(df_eager).lazy()

    # 3. Normalize Products ApplNo for Join
    df = df.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))

    # 4. Join Approval Date
    # Create LazyFrame from the map
    dates_df_eager = pl.DataFrame(
        {"appl_no": list(approval_map.keys()), "original_approval_date": list(approval_map.values())}
    )
    if dates_df_eager.is_empty():
        dates_df_eager = pl.DataFrame(schema={"appl_no": pl.String, "original_approval_date": pl.String})
    else:
        dates_df_eager = dates_df_eager.with_columns(pl.col("appl_no").cast(pl.String))

    dates_df = dates_df_eager.lazy()

    df = df.join(dates_df, on="appl_no", how="left")

    # 5. Transformations
    df = normalize_ids(df)
    df = clean_form(df)
    df = clean_ingredients(df)
    df = fix_dates(df, ["original_approval_date"])

    # Explicitly fill nulls for string fields required by Pydantic model
    cols = df.collect_schema().names()

    if "form" in cols:
        df = df.with_columns(pl.col("form").fill_null(""))
    if "strength" in cols:
        df = df.with_columns(pl.col("strength").fill_null(""))

    df = generate_coreason_id(df)
    df = generate_row_hash(df)

    return df


@dlt.source  # type: ignore[misc]
def drugs_fda_source(base_url: str = "https://www.fda.gov/media/89850/download") -> Iterator[DltResource]:
    """
    The dlt source for Drugs@FDA.
    Downloads the ZIP file and yields resources for each target TSV file.
    Also yields a 'silver_products' resource with enriched data.
    """
    logger.info(f"Starting Drugs@FDA download from {base_url}")
    response = requests.get(base_url, stream=True)
    response.raise_for_status()
    zip_bytes = response.content

    files_present = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        all_files = set(z.namelist())
        for target in TARGET_FILES:
            if target in all_files:
                files_present.append(target)
            else:
                logger.warning(f"Expected file {target} not found in ZIP archive.")

    logger.info(f"Found {len(files_present)} target files in archive.")

    # 1. Yield Raw Resources (Bronze)
    for filename in files_present:

        @dlt.resource(  # type: ignore[misc]
            name=f"raw_fda__{_to_snake_case(filename.replace('.txt', ''))}",
            write_disposition="replace",
            schema_contract={"columns": "evolve"},
        )
        def file_resource(fname: str = filename, z_content: bytes = zip_bytes) -> Iterator[List[Dict[str, Any]]]:
            # For Raw, we just yield what we read (clean but not normalized types)
            yield from _read_file_from_zip(z_content, fname)

        yield file_resource()

    # 2. Yield Silver Products Resource
    if "Products.txt" in files_present and "Submissions.txt" in files_present:

        @dlt.resource(  # type: ignore[misc]
            name="silver_products",
            write_disposition="merge",
            primary_key="coreason_id",
            schema_contract={"columns": "evolve"},
        )
        def silver_products_resource(z_content: bytes = zip_bytes) -> Iterator[ProductSilver]:
            logger.info("Generating Silver Products layer...")
            df_lazy = _create_silver_dataframe(z_content)

            # Collect before iteration
            df = df_lazy.collect()

            # Validate rows against Pydantic model
            for row in df.to_dicts():
                # Filter out rows with null ID keys (critical for Linkage)
                if not row.get("appl_no") or not row.get("product_no"):
                    appl = row.get("appl_no")
                    prod = row.get("product_no")
                    logger.warning(f"Skipping row with missing keys: ApplNo={appl}, ProductNo={prod}")
                    continue
                yield ProductSilver(**row)
            logger.info("Silver Products layer generation complete.")

        yield silver_products_resource()

    # 3. Yield Gold Products Resource
    if "Products.txt" in files_present:

        @dlt.resource(  # type: ignore[misc]
            name="dim_drug_product", write_disposition="replace", schema_contract={"columns": "evolve"}
        )
        def gold_products_resource(z_content: bytes = zip_bytes) -> Iterator[ProductGold]:
            logger.info("Generating Gold Products layer...")
            # Get Base Silver Data (Lazy)
            silver_df_lazy = _create_silver_dataframe(z_content)
            # We will continue building lazily where possible

            # Helper to read file if exists, else empty
            def get_df_lazy(fname: str) -> pl.LazyFrame:
                with zipfile.ZipFile(io.BytesIO(z_content)) as z:
                    if fname not in z.namelist():
                        return pl.DataFrame().lazy()
                    with z.open(fname) as f:
                        eager = _read_csv_bytes(f.read())
                        if eager.is_empty():
                            return pl.DataFrame().lazy()
                        return _clean_dataframe(eager).lazy()

            # Read Aux Files
            df_apps = get_df_lazy("Applications.txt")
            df_marketing = get_df_lazy("MarketingStatus.txt")
            df_te = get_df_lazy("TE.txt")
            df_exclusivity = get_df_lazy("Exclusivity.txt")

            # Helper to check cols on LazyFrame safely
            def has_col(ldf: pl.LazyFrame, col: str) -> bool:
                return col in ldf.collect_schema().names()

            # Normalize Keys in Aux Files
            if has_col(df_apps, "appl_no"):
                df_apps = df_apps.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))

            if has_col(df_marketing, "appl_no"):
                df_marketing = df_marketing.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))
            if has_col(df_marketing, "product_no"):
                df_marketing = df_marketing.with_columns(pl.col("product_no").cast(pl.String).str.pad_start(3, "0"))

            if has_col(df_te, "appl_no"):
                df_te = df_te.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))
            if has_col(df_te, "product_no"):
                df_te = df_te.with_columns(pl.col("product_no").cast(pl.String).str.pad_start(3, "0"))

            if has_col(df_exclusivity, "appl_no"):
                df_exclusivity = df_exclusivity.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))
            if has_col(df_exclusivity, "product_no"):
                df_exclusivity = df_exclusivity.with_columns(pl.col("product_no").cast(pl.String).str.pad_start(3, "0"))

            # 1. Join Applications
            if has_col(df_apps, "sponsor_name"):
                cols = ["appl_no", "sponsor_name"]
                if has_col(df_apps, "appl_type"):
                    cols.append("appl_type")
                df_apps_sub = df_apps.select(cols).unique(subset=["appl_no"])
                silver_df_lazy = silver_df_lazy.join(df_apps_sub, on="appl_no", how="left")
            else:
                silver_df_lazy = silver_df_lazy.with_columns(
                    [pl.lit(None).alias("sponsor_name"), pl.lit(None).alias("appl_type")]
                )

            # 2. Join MarketingStatus
            if has_col(df_marketing, "marketing_status_id"):
                df_marketing_sub = df_marketing.select(["appl_no", "product_no", "marketing_status_id"]).unique(
                    subset=["appl_no", "product_no"]
                )
                silver_df_lazy = silver_df_lazy.join(df_marketing_sub, on=["appl_no", "product_no"], how="left")
            else:
                silver_df_lazy = silver_df_lazy.with_columns(pl.lit(None).alias("marketing_status_id"))

            # 2.5. Join MarketingStatus_Lookup
            df_marketing_lookup = get_df_lazy("MarketingStatus_Lookup.txt")
            if has_col(df_marketing_lookup, "marketing_status_id") and has_col(
                df_marketing_lookup, "marketing_status_description"
            ):
                df_marketing_lookup = df_marketing_lookup.with_columns(
                    pl.col("marketing_status_id").cast(pl.Int64, strict=False)
                )
                if "marketing_status_id" in silver_df_lazy.collect_schema().names():
                    silver_df_lazy = silver_df_lazy.with_columns(
                        pl.col("marketing_status_id").cast(pl.Int64, strict=False)
                    )

                    df_lookup_sub = df_marketing_lookup.select(
                        ["marketing_status_id", "marketing_status_description"]
                    ).unique(subset=["marketing_status_id"])

                    silver_df_lazy = silver_df_lazy.join(df_lookup_sub, on="marketing_status_id", how="left")
            else:
                silver_df_lazy = silver_df_lazy.with_columns(pl.lit(None).alias("marketing_status_description"))

            # 3. Join TE
            if has_col(df_te, "te_code"):
                df_te_sub = df_te.select(["appl_no", "product_no", "te_code"]).unique(subset=["appl_no", "product_no"])
                silver_df_lazy = silver_df_lazy.join(df_te_sub, on=["appl_no", "product_no"], how="left")
            else:
                silver_df_lazy = silver_df_lazy.with_columns(pl.lit(None).alias("te_code"))

            # 4. Exclusivity
            if has_col(df_exclusivity, "exclusivity_date"):
                df_exclusivity = fix_dates(df_exclusivity, ["exclusivity_date"])
                df_excl_agg = df_exclusivity.group_by(["appl_no", "product_no"]).agg(
                    pl.col("exclusivity_date").max().alias("max_exclusivity_date")
                )
                silver_df_lazy = silver_df_lazy.join(df_excl_agg, on=["appl_no", "product_no"], how="left")
                today = date.today()
                silver_df_lazy = silver_df_lazy.with_columns(
                    pl.when(pl.col("max_exclusivity_date") > today).then(True).otherwise(False).alias("is_protected")
                )
            else:
                silver_df_lazy = silver_df_lazy.with_columns(pl.lit(False).alias("is_protected"))

            # 5. Derive is_generic
            if "appl_type" in silver_df_lazy.collect_schema().names():
                silver_df_lazy = silver_df_lazy.with_columns(
                    (pl.col("appl_type") == "A").fill_null(False).alias("is_generic")
                )
            else:
                silver_df_lazy = silver_df_lazy.with_columns(pl.lit(False).alias("is_generic"))

            # 6. Derive search_vector
            search_components = []
            final_cols = silver_df_lazy.collect_schema().names()

            if "drug_name" in final_cols:
                search_components.append(pl.col("drug_name").fill_null(""))
            else:
                search_components.append(pl.lit(""))

            search_components.append(pl.col("active_ingredients_list").list.join(" ").fill_null(""))
            search_components.append(pl.col("sponsor_name").fill_null(""))
            search_components.append(pl.col("te_code").fill_null(""))

            silver_df_lazy = silver_df_lazy.with_columns(
                pl.concat_str(search_components, separator=" ").str.strip_chars().alias("search_vector")
            )
            silver_df_lazy = silver_df_lazy.with_columns(pl.col("search_vector").str.to_uppercase())

            if "marketing_status_id" in final_cols:
                silver_df_lazy = silver_df_lazy.with_columns(pl.col("marketing_status_id").cast(pl.Int64, strict=False))

            # Final Collect
            silver_df = silver_df_lazy.collect()

            if silver_df.is_empty():
                return

            for row in silver_df.to_dicts():
                yield ProductGold(**row)

        yield gold_products_resource()
