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
from typing import Any, Dict, Iterator, List

import dlt
import polars as pl
import requests  # type: ignore[import-untyped]
from dlt.sources import DltResource

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
    # Insert underscore before capital letters
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Insert underscore before capital letters at the end of words or acronyms
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _clean_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the dataframe by:
    1. Converting column names to snake_case.
    2. Stripping leading/trailing whitespace from string columns.
    """
    # Rename columns to snake_case
    new_cols = {col: _to_snake_case(col) for col in df.columns}
    df = df.rename(new_cols)

    # Strip whitespace from all string columns
    df = df.with_columns(
        [pl.col(col).str.strip_chars() for col, dtype in zip(df.columns, df.dtypes, strict=True) if dtype == pl.Utf8]
    )
    return df


def _read_file_from_zip(zip_content: bytes, filename: str) -> Iterator[List[Dict[str, Any]]]:
    """Reads a specific file from the zip content and yields it as dicts."""
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        with z.open(filename) as f:
            # Read directly into Polars
            df = pl.read_csv(
                f.read(),
                separator="\t",
                quote_char=None,
                encoding="cp1252",
                ignore_errors=True,
                truncate_ragged_lines=True,
            )

            # Clean the dataframe
            df = _clean_dataframe(df)

            # Yield as dicts
            yield df.to_dicts()


@dlt.source  # type: ignore[misc]
def drugs_fda_source(base_url: str = "https://www.fda.gov/media/89850/download") -> Iterator[DltResource]:
    """
    The dlt source for Drugs@FDA.
    Downloads the ZIP file and yields resources for each target TSV file.
    """
    response = requests.get(base_url, stream=True)
    response.raise_for_status()

    # Read the content into memory once
    zip_bytes = response.content

    # We need to know which files are actually in the zip to avoid creating empty resources
    # checking existence.
    # To do this effectively without re-opening zip many times, we can just assume standard files
    # OR open it once to check namelist.

    files_present = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        all_files = set(z.namelist())
        for target in TARGET_FILES:
            if target in all_files:
                files_present.append(target)

    for filename in files_present:
        # Define a resource for this file
        # We capture zip_bytes and filename in the closure

        @dlt.resource(name=f"raw_fda__{_to_snake_case(filename.replace('.txt', ''))}", write_disposition="replace")  # type: ignore[misc]
        def file_resource(fname: str = filename, z_content: bytes = zip_bytes) -> Iterator[List[Dict[str, Any]]]:
            yield from _read_file_from_zip(z_content, fname)

        yield file_resource()
