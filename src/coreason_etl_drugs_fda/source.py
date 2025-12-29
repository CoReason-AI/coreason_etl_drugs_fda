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
from coreason_etl_drugs_fda.transform import clean_ingredients, fix_dates, normalize_ids

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
        [pl.col(col).str.strip_chars() for col, dtype in zip(df.columns, df.dtypes, strict=True) if dtype == pl.Utf8]
    )
    return df


def _read_csv_bytes(content: bytes) -> pl.DataFrame:
    """Reads CSV bytes into Polars DataFrame with standard settings."""
    if not content:
        return pl.DataFrame()

    # Ensure all columns are read as UTF8 to avoid type inference issues (0004 -> 4)
    # This is critical for IDs like ApplNo.
    # We can try to infer schema safe, but infer_schema_length=0 forces string usually?
    # Or strict dtypes?
    # Spec says "dlt infers types" for Bronze.
    # But for our internal logic (Silver Join), we need control.
    # Let's rely on infer_schema_length=10000 or similar, but IDs are tricky.
    # If we want to guarantee strings for everything initially (safest for ETL), we can use infer_schema_length=0
    # But that might make everything String.
    # Let's try to just cast ApplNo explicitly if possible? No, we don't know if it's there yet.
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
    Extracts 'ORIG' submission dates from Submissions.txt.
    Returns: Dict[ApplNo, DateString]
    """
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        if "Submissions.txt" not in z.namelist():
            return {}

        with z.open("Submissions.txt") as f:
            df = _read_csv_bytes(f.read())
            df = _clean_dataframe(df)

            if "submission_type" not in df.columns or "submission_status_date" not in df.columns:
                return {}

            df = df.filter(pl.col("submission_type") == "ORIG")

            # Force ApplNo to string (handling potential int inference)
            # 000004 (int 4) -> "4".
            # 000004 (str "000004") -> "000004".
            # To normalize, we should pad it?
            # normalize_ids in transform.py pads to 6.
            # We should probably normalize here too to ensure keys match.
            # But wait, Silver logic calls `normalize_ids` LATER.
            # If we normalize here, we get "000004".
            # If Products is int 4, we need to normalize that before join too.
            # Let's normalize ApplNo to padded string HERE and in Products before join.
            df = df.with_columns(pl.col("appl_no").cast(pl.Utf8).str.pad_start(6, "0"))

            df = df.sort("submission_status_date")
            df = df.unique(subset=["appl_no"], keep="first")

            rows = df.select(["appl_no", "submission_status_date"]).to_dicts()
            return {row["appl_no"]: row["submission_status_date"] for row in rows if row["submission_status_date"]}


def _create_silver_dataframe(zip_content: bytes) -> pl.DataFrame:
    """Creates the Silver Products DataFrame (shared logic for Silver and Gold)."""
    # 1. Pre-fetch approval dates
    approval_map = _extract_approval_dates(zip_content)

    # 2. Read Products
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        if "Products.txt" not in z.namelist():
            # Return empty schema if Products is missing
            return pl.DataFrame()

        with z.open("Products.txt") as f:
            df = _read_csv_bytes(f.read())
            df = _clean_dataframe(df)

    if df.is_empty():
        return pl.DataFrame()

    # 3. Normalize Products ApplNo for Join
    df = df.with_columns(pl.col("appl_no").cast(pl.Utf8).str.pad_start(6, "0"))

    # 4. Join Approval Date
    dates_df = pl.DataFrame(
        {"appl_no": list(approval_map.keys()), "original_approval_date": list(approval_map.values())}
    )
    # Ensure schema for empty map case
    if dates_df.is_empty():
        dates_df = pl.DataFrame(schema={"appl_no": pl.Utf8, "original_approval_date": pl.Utf8})
    else:
        dates_df = dates_df.with_columns(pl.col("appl_no").cast(pl.Utf8))

    df = df.join(dates_df, on="appl_no", how="left")

    # 5. Transformations
    df = normalize_ids(df)
    df = clean_ingredients(df)
    df = fix_dates(df, ["original_approval_date"])
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
    response = requests.get(base_url, stream=True)
    response.raise_for_status()
    zip_bytes = response.content

    files_present = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        all_files = set(z.namelist())
        for target in TARGET_FILES:
            if target in all_files:
                files_present.append(target)

    # 1. Yield Raw Resources (Bronze)
    for filename in files_present:

        @dlt.resource(name=f"raw_fda__{_to_snake_case(filename.replace('.txt', ''))}", write_disposition="replace")  # type: ignore[misc]
        def file_resource(fname: str = filename, z_content: bytes = zip_bytes) -> Iterator[List[Dict[str, Any]]]:
            # For Raw, we just yield what we read (clean but not normalized types)
            yield from _read_file_from_zip(z_content, fname)

        yield file_resource()

    # 2. Yield Silver Products Resource
    if "Products.txt" in files_present and "Submissions.txt" in files_present:

        @dlt.resource(name="silver_products", write_disposition="merge", primary_key="coreason_id")  # type: ignore[misc]
        def silver_products_resource(z_content: bytes = zip_bytes) -> Iterator[ProductSilver]:
            df = _create_silver_dataframe(z_content)

            # Validate rows against Pydantic model
            for row in df.to_dicts():
                yield ProductSilver(**row)

        yield silver_products_resource()

    # 3. Yield Gold Products Resource
    # Depends on Products, Applications, MarketingStatus, TE, Exclusivity (optional?), Submissions
    # Check if we have minimal files (Products+Submissions is baseline Silver, plus Applications etc.)
    # If some auxiliary files missing, we can still produce Gold with nulls?
    # BRD says "Left Join", so yes.
    if "Products.txt" in files_present:

        @dlt.resource(name="dim_drug_product", write_disposition="replace")  # type: ignore[misc]
        def gold_products_resource(z_content: bytes = zip_bytes) -> Iterator[ProductGold]:
            # Get Base Silver Data
            silver_df = _create_silver_dataframe(z_content)
            if silver_df.is_empty():
                return

            # Helper to read file if exists, else empty
            def get_df(fname: str) -> pl.DataFrame:
                with zipfile.ZipFile(io.BytesIO(z_content)) as z:
                    if fname not in z.namelist():
                        return pl.DataFrame()
                    with z.open(fname) as f:
                        return _clean_dataframe(_read_csv_bytes(f.read()))

            # Read Aux Files
            df_apps = get_df("Applications.txt")
            df_marketing = get_df("MarketingStatus.txt")
            df_te = get_df("TE.txt")
            df_exclusivity = get_df("Exclusivity.txt")

            # Normalize Keys in Aux Files
            # Applications: appl_no
            if "appl_no" in df_apps.columns:
                df_apps = df_apps.with_columns(pl.col("appl_no").cast(pl.Utf8).str.pad_start(6, "0"))

            # MarketingStatus: appl_no, product_no
            if "appl_no" in df_marketing.columns:
                df_marketing = df_marketing.with_columns(pl.col("appl_no").cast(pl.Utf8).str.pad_start(6, "0"))
            if "product_no" in df_marketing.columns:
                df_marketing = df_marketing.with_columns(pl.col("product_no").cast(pl.Utf8).str.pad_start(3, "0"))

            # TE: appl_no, product_no
            if "appl_no" in df_te.columns:
                df_te = df_te.with_columns(pl.col("appl_no").cast(pl.Utf8).str.pad_start(6, "0"))
            if "product_no" in df_te.columns:
                df_te = df_te.with_columns(pl.col("product_no").cast(pl.Utf8).str.pad_start(3, "0"))

            # Exclusivity: appl_no, product_no
            if "appl_no" in df_exclusivity.columns:
                df_exclusivity = df_exclusivity.with_columns(pl.col("appl_no").cast(pl.Utf8).str.pad_start(6, "0"))
            if "product_no" in df_exclusivity.columns:
                df_exclusivity = df_exclusivity.with_columns(pl.col("product_no").cast(pl.Utf8).str.pad_start(3, "0"))

            # 1. Join Applications (SponsorName, ApplType)
            # We select only needed columns to avoid collisions
            if "sponsor_name" in df_apps.columns:
                # Need appl_no for join
                cols = ["appl_no", "sponsor_name"]
                if "appl_type" in df_apps.columns:
                    cols.append("appl_type")
                df_apps_sub = df_apps.select(cols).unique(
                    subset=["appl_no"]
                )  # Ensure 1:1 or handle dupes? Applications are 1 per ApplNo usually?
                silver_df = silver_df.join(df_apps_sub, on="appl_no", how="left")
            else:
                silver_df = silver_df.with_columns(
                    [pl.lit(None).alias("sponsor_name"), pl.lit(None).alias("appl_type")]
                )

            # 2. Join MarketingStatus (MarketingStatusID)
            if "marketing_status_id" in df_marketing.columns:
                df_marketing_sub = df_marketing.select(["appl_no", "product_no", "marketing_status_id"]).unique(
                    subset=["appl_no", "product_no"]
                )
                silver_df = silver_df.join(df_marketing_sub, on=["appl_no", "product_no"], how="left")
            else:
                silver_df = silver_df.with_columns(pl.lit(None).alias("marketing_status_id"))

            # 2.5. Join MarketingStatus_Lookup (Description)
            df_marketing_lookup = get_df("MarketingStatus_Lookup.txt")
            if (
                "marketing_status_id" in df_marketing_lookup.columns
                and "marketing_status_description" in df_marketing_lookup.columns
            ):
                # Ensure join key types match (Int64)
                df_marketing_lookup = df_marketing_lookup.with_columns(
                    pl.col("marketing_status_id").cast(pl.Int64, strict=False)
                )
                if "marketing_status_id" in silver_df.columns:
                    silver_df = silver_df.with_columns(pl.col("marketing_status_id").cast(pl.Int64, strict=False))

                    df_lookup_sub = df_marketing_lookup.select(
                        ["marketing_status_id", "marketing_status_description"]
                    ).unique(subset=["marketing_status_id"])

                    silver_df = silver_df.join(df_lookup_sub, on="marketing_status_id", how="left")
            else:
                silver_df = silver_df.with_columns(pl.lit(None).alias("marketing_status_description"))

            # 3. Join TE (TECode)
            if "te_code" in df_te.columns:
                df_te_sub = df_te.select(["appl_no", "product_no", "te_code"]).unique(subset=["appl_no", "product_no"])
                silver_df = silver_df.join(df_te_sub, on=["appl_no", "product_no"], how="left")
            else:
                silver_df = silver_df.with_columns(pl.lit(None).alias("te_code"))

            # 4. Exclusivity Logic (is_protected)
            # Logic: True if current_date < Max(ExclusivityDate)
            # We need to aggregate Exclusivity by ApplNo+ProductNo first.
            if "exclusivity_date" in df_exclusivity.columns:
                # Convert to date
                df_exclusivity = fix_dates(df_exclusivity, ["exclusivity_date"])

                # Group by and get max date
                df_excl_agg = df_exclusivity.group_by(["appl_no", "product_no"]).agg(
                    pl.col("exclusivity_date").max().alias("max_exclusivity_date")
                )

                silver_df = silver_df.join(df_excl_agg, on=["appl_no", "product_no"], how="left")

                # Check protection
                # We need current date. In pipeline, this is execution date.
                # For reproducibility/testing, we might want to pass it or use today.
                today = date.today()

                # If max_exclusivity_date > today -> True
                silver_df = silver_df.with_columns(
                    pl.when(pl.col("max_exclusivity_date") > today).then(True).otherwise(False).alias("is_protected")
                )
            else:
                silver_df = silver_df.with_columns(pl.lit(False).alias("is_protected"))

            # 5. Derive is_generic
            # True if ApplType == 'A' (ANDA), False if ApplType == 'N' (NDA).
            # Note: ApplType column might be missing if apps join failed
            if "appl_type" in silver_df.columns:
                silver_df = silver_df.with_columns((pl.col("appl_type") == "A").fill_null(False).alias("is_generic"))
            else:
                silver_df = silver_df.with_columns(pl.lit(False).alias("is_generic"))

            # 6. Derive search_vector
            # Concatenated string of DrugName + ActiveIngredient + SponsorName + TECode.
            # DrugName should be in Products (silver_df source).
            # ActiveIngredient is active_ingredients_list (List[str]).
            # SponsorName and TECode from joins.
            # We need to handle nulls and convert list to string.

            # Ensure columns exist or lit("")
            search_components = []

            # DrugName (check if exists, snake_case)
            if "drug_name" in silver_df.columns:
                search_components.append(pl.col("drug_name").fill_null(""))
            else:
                search_components.append(pl.lit(""))

            # ActiveIngredient (List[str]) -> join with space
            if "active_ingredients_list" in silver_df.columns:
                search_components.append(pl.col("active_ingredients_list").list.join(" ").fill_null(""))
            else:
                search_components.append(pl.lit(""))
                # If active_ingredients_list is missing, we must add it as empty list because it's required by Pydantic
                silver_df = silver_df.with_columns(pl.lit([]).alias("active_ingredients_list"))

            # SponsorName
            # logic above ensures sponsor_name exists (joined or created as null)
            search_components.append(pl.col("sponsor_name").fill_null(""))

            # TECode
            # logic above ensures te_code exists (joined or created as null)
            search_components.append(pl.col("te_code").fill_null(""))

            silver_df = silver_df.with_columns(
                pl.concat_str(search_components, separator=" ").str.strip_chars().alias("search_vector")
            )
            # Upper case search_vector for consistency? BRD doesn't specify case, but search vectors usually upper.
            # ActiveIngredients are already upper. DrugName might not be.
            # Let's uppercase it.
            silver_df = silver_df.with_columns(pl.col("search_vector").str.to_uppercase())

            # Fill Nones for optional fields that might be missing after join
            # Pydantic Optional handles None, but Polars might have Nulls.
            # to_dicts() handles it.

            # Cast marketing_status_id to int if possible (it's ID)
            if "marketing_status_id" in silver_df.columns:
                silver_df = silver_df.with_columns(pl.col("marketing_status_id").cast(pl.Int64, strict=False))

            for row in silver_df.to_dicts():
                yield ProductGold(**row)

        yield gold_products_resource()
