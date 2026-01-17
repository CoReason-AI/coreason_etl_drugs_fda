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
from typing import Dict, TypeVar, Union

import polars as pl
from dlt.common.normalizers.naming.snake_case import NamingConvention

# Initialize DLT Naming Convention
naming = NamingConvention()

FrameT = TypeVar("FrameT", bound=Union[pl.DataFrame, pl.LazyFrame])


def to_snake_case(name: str) -> str:
    """Converts a string to snake_case using dlt standard."""
    return str(naming.normalize_identifier(name))


def clean_dataframe(df: FrameT) -> FrameT:
    """
    Cleans the dataframe by:
    1. Converting column names to snake_case.
    2. Stripping leading/trailing whitespace from string columns.
    """
    if isinstance(df, pl.LazyFrame):
        cols = df.collect_schema().names()
        dtypes = df.collect_schema().dtypes()
    else:
        cols = df.columns
        dtypes = df.dtypes

    new_cols = {col: to_snake_case(col) for col in cols}
    df = df.rename(new_cols)

    df = df.with_columns(
        [pl.col(new_cols[col]).str.strip_chars() for col, dtype in zip(cols, dtypes, strict=True) if dtype == pl.String]
    )
    return df


def normalize_ids(df: FrameT) -> FrameT:
    """
    Pads ApplNo to 6 digits and ProductNo to 3 digits.
    Handles both integer and string inputs.
    Expects column names to be in snake_case (run clean_dataframe first).
    """
    if isinstance(df, pl.LazyFrame):
        cols = df.collect_schema().names()
    else:
        cols = df.columns

    if "appl_no" in cols:
        df = df.with_columns(
            pl.col("appl_no")
            .cast(pl.String)
            .str.strip_chars()
            .str.replace_all(r"[^0-9]", "")
            .replace("", None)
            .str.pad_start(6, "0")
        )

    if "product_no" in cols:
        df = df.with_columns(
            pl.col("product_no")
            .cast(pl.String)
            .str.strip_chars()
            .str.replace_all(r"[^0-9]", "")
            .replace("", None)
            .str.pad_start(3, "0")
        )
    return df


def fix_dates(df: FrameT, date_cols: list[str]) -> FrameT:
    """
    Handles legacy string "Approved prior to Jan 1, 1982" AND dates before 1982.

    Logic:
    1. is_historic_record = True if:
       - Value is explicitly "Approved prior to Jan 1, 1982"
       - OR Value is a valid date strictly before 1982-01-01
    2. original_approval_date is normalized to Date type (Legacy becomes 1982-01-01).
    """
    legacy_str = "Approved prior to Jan 1, 1982"
    legacy_date = date(1982, 1, 1)

    if isinstance(df, pl.LazyFrame):
        schema = df.collect_schema()
        cols = schema.names()
    else:
        schema = df.schema
        cols = df.columns

    for col in date_cols:
        if col not in cols:
            continue

        # Check if column is string type
        if schema[col] == pl.String:
            # --- FIX 1: Enhanced Historic Logic ---
            # Parse the date tentatively to check its value
            parsed_date_expr = pl.col(col).str.slice(0, 10).str.to_date(format="%Y-%m-%d", strict=False)

            # Historic if matches legacy string OR parsed date is older than 1982
            is_historic_expr = (pl.col(col) == legacy_str) | (parsed_date_expr < legacy_date)

            df = df.with_columns(is_historic_expr.fill_null(False).alias("is_historic_record"))

            # --- Update Date Column ---
            df = df.with_columns(
                pl.when(pl.col(col) == legacy_str).then(pl.lit(legacy_date)).otherwise(parsed_date_expr).alias(col)
            )

    return df


def clean_ingredients(df: FrameT) -> FrameT:
    """
    Splits ActiveIngredient by semicolon, upper-cases, and trims whitespace.
    Ensures 'active_ingredients_list' column always exists.
    """
    if isinstance(df, pl.LazyFrame):
        cols = df.collect_schema().names()
    else:
        cols = df.columns

    if "active_ingredient" in cols:
        df = df.with_columns(
            pl.col("active_ingredient")
            .str.to_uppercase()
            .str.split(";")
            .list.eval(pl.element().str.strip_chars())
            .list.eval(pl.element().filter(pl.element().str.len_bytes() > 0))  # Filter out empty strings
            .fill_null(pl.lit([], dtype=pl.List(pl.String)))
            .alias("active_ingredients_list")
        )
        # Drop the original column
        df = df.drop("active_ingredient")
    else:
        # Create empty list column if input missing
        df = df.with_columns(pl.lit([], dtype=pl.List(pl.String)).alias("active_ingredients_list"))

    return df


def clean_form(df: FrameT) -> FrameT:
    """
    Converts the 'form' column to Title Case.
    """
    if isinstance(df, pl.LazyFrame):
        cols = df.collect_schema().names()
    else:
        cols = df.columns

    if "form" in cols:
        df = df.with_columns(pl.col("form").str.to_titlecase())
    return df


def _get_empty_silver_schema() -> pl.LazyFrame:
    """Returns an empty LazyFrame with the correct schema for Silver Products."""
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


def prepare_silver_products(
    products_lazy: pl.LazyFrame, approval_dates_lazy: pl.LazyFrame, approval_dates_map_exists: bool
) -> pl.LazyFrame:
    """
    Constructs the Silver Products LazyFrame logic.
    """
    df = products_lazy
    df = clean_dataframe(df)

    # Check if we have the minimum required columns or if it's empty
    cols = df.collect_schema().names()
    if not cols or "appl_no" not in cols:
        return _get_empty_silver_schema()

    # Normalize Products ApplNo for Join
    df = df.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))

    # Join Approval Date
    if approval_dates_map_exists:
        dates_df = approval_dates_lazy.with_columns(pl.col("appl_no").cast(pl.String))
    else:
        # Empty schema matches approval_map expectations
        dates_df = pl.DataFrame(schema={"appl_no": pl.String, "original_approval_date": pl.String}).lazy()

    df = df.join(dates_df, on="appl_no", how="left")

    # Transformations
    df = normalize_ids(df)
    df = clean_form(df)
    df = clean_ingredients(df)
    df = fix_dates(df, ["original_approval_date"])

    # Explicitly fill nulls for string fields
    cols = df.collect_schema().names()

    if "form" in cols:
        df = df.with_columns(pl.col("form").fill_null(""))
    if "strength" in cols:
        df = df.with_columns(pl.col("strength").fill_null(""))

    from coreason_etl_drugs_fda.silver import generate_coreason_id, generate_row_hash

    df = generate_coreason_id(df)
    df = generate_row_hash(df)

    return df


def prepare_gold_products(
    silver_df: pl.LazyFrame,
    df_apps: pl.LazyFrame,
    df_marketing: pl.LazyFrame,
    df_marketing_lookup: pl.LazyFrame,
    df_te: pl.LazyFrame,
    df_exclusivity: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Constructs the Gold Products LazyFrame logic.
    """
    # If Silver base is empty, return empty
    if silver_df.collect_schema().len() == 0:
        return silver_df

    # CLEAN ALL AUXILIARY DATAFRAMES FIRST
    df_apps = clean_dataframe(df_apps)
    df_marketing = clean_dataframe(df_marketing)
    df_marketing_lookup = clean_dataframe(df_marketing_lookup)
    df_te = clean_dataframe(df_te)
    df_exclusivity = clean_dataframe(df_exclusivity)

    # Helper to check cols on LazyFrame safely
    def has_col(ldf: pl.LazyFrame, col: str) -> bool:
        return col in ldf.collect_schema().names()

    # Normalize Keys in Aux Files
    df_apps = normalize_ids(df_apps)
    df_marketing = normalize_ids(df_marketing)
    df_te = normalize_ids(df_te)
    df_exclusivity = normalize_ids(df_exclusivity)

    # 1. Join Applications
    if has_col(df_apps, "sponsor_name"):
        cols = ["appl_no", "sponsor_name"]
        if has_col(df_apps, "appl_type"):
            cols.append("appl_type")
        # Deterministic Deduplication
        df_apps_sub = df_apps.select(cols).sort(cols).unique(subset=["appl_no"], keep="first")
        silver_df = silver_df.join(df_apps_sub, on="appl_no", how="left")
    else:
        silver_df = silver_df.with_columns(
            [
                pl.lit(None, dtype=pl.String).alias("sponsor_name"),
                pl.lit(None, dtype=pl.String).alias("appl_type"),
            ]
        )

    # 2. Join MarketingStatus
    if has_col(df_marketing, "marketing_status_id"):
        cols_marketing = ["appl_no", "product_no", "marketing_status_id"]
        df_marketing_sub = (
            df_marketing.select(cols_marketing)
            .sort(cols_marketing)
            .unique(subset=["appl_no", "product_no"], keep="first")
        )
        silver_df = silver_df.join(df_marketing_sub, on=["appl_no", "product_no"], how="left")
    else:
        silver_df = silver_df.with_columns(pl.lit(None, dtype=pl.Int64).alias("marketing_status_id"))

    # 2.5. Join MarketingStatus_Lookup
    if has_col(df_marketing_lookup, "marketing_status_id") and has_col(
        df_marketing_lookup, "marketing_status_description"
    ):
        df_marketing_lookup = df_marketing_lookup.with_columns(
            pl.col("marketing_status_id").cast(pl.Int64, strict=False)
        )
        if "marketing_status_id" in silver_df.collect_schema().names():
            silver_df = silver_df.with_columns(pl.col("marketing_status_id").cast(pl.Int64, strict=False))

            cols_lookup = ["marketing_status_id", "marketing_status_description"]
            df_lookup_sub = (
                df_marketing_lookup.select(cols_lookup)
                .sort(cols_lookup)
                .unique(subset=["marketing_status_id"], keep="first")
            )
            silver_df = silver_df.join(df_lookup_sub, on="marketing_status_id", how="left")
    else:
        silver_df = silver_df.with_columns(pl.lit(None, dtype=pl.String).alias("marketing_status_description"))

    # 3. Join TE
    if has_col(df_te, "te_code"):
        cols_te = ["appl_no", "product_no", "te_code"]
        df_te_sub = df_te.select(cols_te).sort(cols_te).unique(subset=["appl_no", "product_no"], keep="first")
        silver_df = silver_df.join(df_te_sub, on=["appl_no", "product_no"], how="left")
    else:
        silver_df = silver_df.with_columns(pl.lit(None, dtype=pl.String).alias("te_code"))

    # 4. Exclusivity
    if has_col(df_exclusivity, "exclusivity_date"):
        df_exclusivity = fix_dates(df_exclusivity, ["exclusivity_date"])
        df_excl_agg = df_exclusivity.group_by(["appl_no", "product_no"]).agg(
            pl.col("exclusivity_date").max().alias("max_exclusivity_date")
        )
        silver_df = silver_df.join(df_excl_agg, on=["appl_no", "product_no"], how="left")
        today = date.today()
        silver_df = silver_df.with_columns(
            pl.when(pl.col("max_exclusivity_date") > today).then(True).otherwise(False).alias("is_protected")
        )
    else:
        silver_df = silver_df.with_columns(pl.lit(False).alias("is_protected"))

    # --- FIX 2: Enhanced is_generic Logic ---
    if "appl_type" in silver_df.collect_schema().names():
        # Check for standard "A" code OR "ANDA" (and normalize case/whitespace)
        is_generic_expr = pl.col("appl_type").str.to_uppercase().str.strip_chars().is_in(["A", "ANDA"])
        silver_df = silver_df.with_columns(is_generic_expr.fill_null(False).alias("is_generic"))
    else:
        silver_df = silver_df.with_columns(pl.lit(False).alias("is_generic"))

    # 6. Derive search_vector
    search_components = []
    final_cols = silver_df.collect_schema().names()

    if "drug_name" in final_cols:
        search_components.append(pl.col("drug_name").fill_null(""))
    else:
        search_components.append(pl.lit(""))

    search_components.append(pl.col("active_ingredients_list").list.join(" ").fill_null(""))
    search_components.append(pl.col("sponsor_name").fill_null(""))
    search_components.append(pl.col("te_code").fill_null(""))

    silver_df = silver_df.with_columns(
        pl.concat_str(search_components, separator=" ").str.strip_chars().alias("search_vector")
    )
    silver_df = silver_df.with_columns(pl.col("search_vector").str.to_uppercase())

    if "marketing_status_id" in final_cols:
        silver_df = silver_df.with_columns(pl.col("marketing_status_id").cast(pl.Int64, strict=False))

    return silver_df


def extract_orig_dates(submissions_lazy: pl.LazyFrame) -> Dict[str, str]:
    """
    Business logic to extract ORIG dates from LazyFrame.
    """
    df = clean_dataframe(submissions_lazy)

    cols = df.collect_schema().names()
    if "submission_type" not in cols or "submission_status_date" not in cols:
        return {}

    df = df.filter(pl.col("submission_type") == "ORIG")

    df = df.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))

    legacy_str = "Approved prior to Jan 1, 1982"
    legacy_date = date(1982, 1, 1)

    parsed_date = pl.col("submission_status_date").str.slice(0, 10).str.to_date(format="%Y-%m-%d", strict=False)

    df = df.with_columns(
        pl.when(pl.col("submission_status_date") == legacy_str)
        .then(pl.lit(legacy_date))
        .otherwise(parsed_date)
        .alias("sort_date")
    )

    df = df.sort("sort_date")

    df = df.unique(subset=["appl_no"], keep="first")

    rows = df.select(["appl_no", "submission_status_date"]).collect().to_dicts()
    return {row["appl_no"]: row["submission_status_date"] for row in rows if row["submission_status_date"]}
