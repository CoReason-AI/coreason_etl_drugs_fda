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
from typing import Any, Dict, Iterator, List, Optional

import dlt
import polars as pl
import requests  # type: ignore[import-untyped]
from dlt.sources import DltResource

from coreason_etl_drugs_fda.silver import generate_coreason_id, generate_row_hash
from coreason_etl_drugs_fda.transform import clean_ingredients, fix_dates, normalize_ids

# List of files to extract from the FDA ZIP archive
TARGET_FILES = [
    "Products.txt",
    "Applications.txt",
    "MarketingStatus.txt",
    "TE.txt",
    "Submissions.txt",
    "Exclusivity.txt",
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
        def silver_products_resource(z_content: bytes = zip_bytes) -> Iterator[List[Dict[str, Any]]]:
            # 1. Pre-fetch approval dates
            # This returns normalized (padded) ApplNo keys.
            approval_map = _extract_approval_dates(z_content)

            # 2. Read Products
            with zipfile.ZipFile(io.BytesIO(z_content)) as z:
                with z.open("Products.txt") as f:
                    df = _read_csv_bytes(f.read())
                    df = _clean_dataframe(df)

            # 3. Normalize Products ApplNo for Join
            # We must pad it to match the keys from _extract_approval_dates
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
            # Normalize again? It's already done for ApplNo, but do ProductNo
            df = normalize_ids(df)
            df = clean_ingredients(df)
            df = fix_dates(df, ["original_approval_date"])
            df = generate_coreason_id(df)
            df = generate_row_hash(df)

            yield df.to_dicts()

        yield silver_products_resource()
