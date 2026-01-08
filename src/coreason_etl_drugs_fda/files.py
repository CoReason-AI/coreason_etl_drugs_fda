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
import zipfile
from typing import Any, Dict, Iterator, List

import polars as pl

from coreason_etl_drugs_fda.transform import clean_dataframe


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
            # For Bronze, we keep it eager as we yield dicts immediately
            df = clean_dataframe(df)
            # Since we pass DF, we get DF back.
            if isinstance(df, pl.LazyFrame):
                # Should not happen given input is DataFrame, but for type safety if logic changes
                df = df.collect()  # pragma: no cover
            yield df.to_dicts()


def _get_lazy_df_from_zip(zip_content: bytes, filename: str) -> pl.LazyFrame:
    """Helper to get a LazyFrame from a file in zip content, handling existence and emptiness."""
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        if filename not in z.namelist():
            return pl.DataFrame().lazy()
        with z.open(filename) as f:
            eager = _read_csv_bytes(f.read())
            if eager.is_empty():
                return pl.DataFrame().lazy()
            # Convert to lazy IMMEDIATELY, then clean
            df = eager.lazy()
            return clean_dataframe(df)
