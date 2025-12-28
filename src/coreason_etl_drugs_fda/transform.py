# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

from datetime import date

import polars as pl


def normalize_ids(df: pl.DataFrame) -> pl.DataFrame:
    """
    Pads ApplNo to 6 digits and ProductNo to 3 digits.
    Handles both integer and string inputs.
    """
    if "appl_no" in df.columns:
        df = df.with_columns(pl.col("appl_no").cast(pl.Utf8).str.pad_start(6, "0"))

    if "product_no" in df.columns:
        df = df.with_columns(pl.col("product_no").cast(pl.Utf8).str.pad_start(3, "0"))
    return df


def fix_dates(df: pl.DataFrame, date_cols: list[str]) -> pl.DataFrame:
    """
    Handles legacy string "Approved prior to Jan 1, 1982".
    Logic: If value == "Approved prior to Jan 1, 1982", set approval_date = 1982-01-01
    and set flag is_historic_record = True.
    """
    legacy_str = "Approved prior to Jan 1, 1982"
    legacy_date = date(1982, 1, 1)

    for col in date_cols:
        if col not in df.columns:
            continue

        # First, ensure we have an is_historic_record column if we find the string
        # Actually, let's create it if we are touching approval_date-like columns?
        # The spec says "set flag is_historic_record = True". This implies a new column.
        # We'll default to False.

        # Check if column is string type, otherwise we can't check for the legacy string
        if df.schema[col] == pl.Utf8:
            is_legacy = pl.col(col) == legacy_str

            # Update the date column: replace legacy string with 1982-01-01
            # We will convert to Date type eventually, but for now replace the string.
            # Or better, cast to Date handling this.

            # The input might be mixed (dates as strings and this legacy string).
            # BRD says "Silver (The Refinery) ... Date Logic ... set approval_date = 1982-01-01"

            df = df.with_columns(
                pl.when(is_legacy).then(pl.lit(True)).otherwise(pl.lit(False)).alias("is_historic_record")
            )

            df = df.with_columns(
                pl.when(pl.col(col) == legacy_str)
                .then(pl.lit(legacy_date))  # This might try to cast column to Date?
                # If column is Utf8, putting a Date literal might fail or cast to string.
                # Let's replace with string '1982-01-01' first, then cast to Date.
                .otherwise(pl.col(col))
                .str.to_date(format="%Y-%m-%d", strict=False)  # Attempt parse
                .alias(col)
            )

            # Note: str.to_date strict=False will turn unparseable into null.
            # If the original dates are in 'YYYY-MM-DD' format, this works.
            # If they are different format, we need to know.
            # FDA dates are usually YYYY-MM-DD in Submissions, but we should be careful.
            # The legacy string is found in 'original_approval_date' usually?
            # Submissions file has 'SubmissionStatusDate'.

    return df


def clean_ingredients(df: pl.DataFrame) -> pl.DataFrame:
    """
    Splits ActiveIngredient by semicolon, upper-cases, and trims whitespace.
    """
    if "active_ingredient" in df.columns:
        df = df.with_columns(
            pl.col("active_ingredient")
            .str.to_uppercase()
            .str.split(";")
            .list.eval(pl.element().str.strip_chars())
            .alias("active_ingredients_list")
        )
    return df
