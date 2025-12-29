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

        # Check if column is string type, otherwise we can't check for the legacy string
        if df.schema[col] == pl.Utf8:
            is_legacy = pl.col(col) == legacy_str

            df = df.with_columns(
                pl.when(is_legacy).then(pl.lit(True)).otherwise(pl.lit(False)).alias("is_historic_record")
            )

            df = df.with_columns(
                pl.when(pl.col(col) == legacy_str)
                .then(pl.lit(legacy_date))
                .otherwise(pl.col(col))
                .str.to_date(format="%Y-%m-%d", strict=False)  # Attempt parse
                .alias(col)
            )

    return df


def clean_ingredients(df: pl.DataFrame) -> pl.DataFrame:
    """
    Splits ActiveIngredient by semicolon, upper-cases, and trims whitespace.
    Ensures 'active_ingredients_list' column always exists (as empty list if missing input).
    Strictly removes 'active_ingredient' column.
    Handles null values by converting them to empty lists.
    """
    if "active_ingredient" in df.columns:
        df = df.with_columns(
            pl.col("active_ingredient")
            .str.to_uppercase()
            .str.split(";")
            .list.eval(pl.element().str.strip_chars())
            .fill_null(pl.lit([], dtype=pl.List(pl.Utf8)))  # Ensure nulls become typed empty lists
            .alias("active_ingredients_list")
        )
        # Drop the original column
        df = df.drop("active_ingredient")
    else:
        # Create empty list column if input missing, explicitly typed as List[Utf8]
        df = df.with_columns(pl.lit([], dtype=pl.List(pl.Utf8)).alias("active_ingredients_list"))

    return df


def clean_form(df: pl.DataFrame) -> pl.DataFrame:
    """
    Converts the 'form' column to Title Case.
    """
    if "form" in df.columns:
        df = df.with_columns(pl.col("form").str.to_titlecase())
    return df
