# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import polars as pl

from coreason_etl_drugs_fda.transform import (
    _get_empty_silver_schema,
    prepare_gold_products,
    prepare_silver_products,
)


def test_prepare_gold_products_empty_silver() -> None:
    """
    Test prepare_gold_products returns empty result immediately if silver_df is empty.
    Hits line 235 in transform.py.
    """
    # Create empty Silver LazyFrame (using the helper for correct schema)
    empty_silver = _get_empty_silver_schema()

    # Pass empty frames for others
    empty_aux = pl.DataFrame().lazy()

    result = prepare_gold_products(
        silver_df=empty_silver,
        df_apps=empty_aux,
        df_marketing=empty_aux,
        df_marketing_lookup=empty_aux,
        df_te=empty_aux,
        df_exclusivity=empty_aux,
    )

    # Should be empty
    # Note: _get_empty_silver_schema returns schema with cols.
    # But collect_schema().len() check in prepare_gold_products logic:
    # `if silver_df.collect_schema().len() == 0:`
    # Wait, if I pass `_get_empty_silver_schema()`, it HAS a schema (len > 0).
    # It just has no rows.
    # The check `collect_schema().len() == 0` implies "no columns", i.e. completely empty unknown schema.
    # Let's verify what `prepare_gold_products` checks.
    # It checks `silver_df.collect_schema().len() == 0`.
    # If I pass `pl.DataFrame().lazy()`, schema len is 0.
    # If I pass `_get_empty_silver_schema()`, schema len is ~11.

    # To hit the specific line `return silver_df` inside the `if ... == 0` block,
    # I must pass a LazyFrame with NO columns.
    schemaless_silver = pl.DataFrame().lazy()

    result = prepare_gold_products(
        silver_df=schemaless_silver,
        df_apps=empty_aux,
        df_marketing=empty_aux,
        df_marketing_lookup=empty_aux,
        df_te=empty_aux,
        df_exclusivity=empty_aux,
    )

    # Should verify result is indeed what we passed back
    # collect_schema should still be empty
    assert result.collect_schema().len() == 0


def test_prepare_silver_products_empty_input() -> None:
    """
    Verify prepare_silver_products handles empty input by returning empty schema.
    This covers the other defensive check if not already covered.
    """
    empty_input = pl.DataFrame().lazy()
    dates = pl.DataFrame().lazy()
    res = prepare_silver_products(empty_input, dates, False)
    # Should have Silver schema
    assert "coreason_id" in res.collect_schema().names()
