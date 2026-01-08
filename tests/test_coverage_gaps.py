# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import hashlib
import uuid

import polars as pl

from coreason_etl_drugs_fda.silver import NAMESPACE_FDA, generate_coreason_id, generate_row_hash
from coreason_etl_drugs_fda.transform import clean_ingredients, fix_dates, prepare_gold_products


def test_prepare_gold_products_missing_aux_columns() -> None:
    """
    Test prepare_gold_products when auxiliary dataframes exist but lack required columns.
    This targets the 'else' branches in prepare_gold_products.
    """
    # 1. Base Silver DataFrame
    silver_schema = {
        "appl_no": pl.String,
        "product_no": pl.String,
        "appl_type": pl.String,
        "drug_name": pl.String,
        "active_ingredients_list": pl.List(pl.String),
        "sponsor_name": pl.String,
        "te_code": pl.String,
        "marketing_status_id": pl.Int64,
    }
    silver_df = pl.DataFrame(
        {
            "appl_no": ["000001"],
            "product_no": ["001"],
            "appl_type": ["N"],
            "drug_name": ["DrugA"],
            "active_ingredients_list": [["IngA"]],
            "sponsor_name": ["SponsorA"],
            "te_code": ["TE1"],
            "marketing_status_id": [1],
        },
        schema=silver_schema,
    ).lazy()

    # 2. Aux DataFrames with MISSING columns (but some other columns to simulate file existence)
    # Applications: Missing 'sponsor_name' and 'appl_type'
    df_apps = pl.DataFrame({"appl_no": ["000001"], "other_col": ["X"]}).lazy()

    # Marketing: Missing 'marketing_status_id'
    df_marketing = pl.DataFrame({"appl_no": ["000001"], "product_no": ["001"], "other_col": ["X"]}).lazy()

    # Marketing Lookup: Missing 'marketing_status_description'
    df_marketing_lookup = pl.DataFrame({"marketing_status_id": [1], "other_col": ["X"]}).lazy()

    # TE: Missing 'te_code'
    df_te = pl.DataFrame({"appl_no": ["000001"], "product_no": ["001"], "other_col": ["X"]}).lazy()

    # Exclusivity: Missing 'exclusivity_date'
    df_exclusivity = pl.DataFrame({"appl_no": ["000001"], "product_no": ["001"], "other_col": ["X"]}).lazy()

    # 3. Run Transformation
    gold_df = prepare_gold_products(
        silver_df, df_apps, df_marketing, df_marketing_lookup, df_te, df_exclusivity
    ).collect()

    row = gold_df.row(0, named=True)

    assert row["sponsor_name"] is None
    assert row["marketing_status_id"] is None  # Overwritten by None because marketing df missed the col
    assert row["marketing_status_description"] is None
    assert row["te_code"] is None
    assert row["is_protected"] is False  # Default when exclusivity missing


def test_prepare_gold_products_empty_base() -> None:
    """Test that empty silver dataframe returns empty result immediately."""
    silver_schema = {
        "appl_no": pl.String,
        "product_no": pl.String,
        "active_ingredients_list": pl.List(pl.String),
        "drug_name": pl.String,
        "sponsor_name": pl.String,
        "te_code": pl.String,
        "marketing_status_id": pl.Int64,
    }
    silver_df = pl.DataFrame(schema=silver_schema).lazy()

    # Aux frames can be anything
    res = prepare_gold_products(silver_df, silver_df, silver_df, silver_df, silver_df, silver_df)

    assert res.collect().height == 0


def test_prepare_gold_products_truly_empty_schema() -> None:
    """Test that a dataframe with NO columns returns immediately."""
    silver_df = pl.DataFrame().lazy()
    res = prepare_gold_products(silver_df, silver_df, silver_df, silver_df, silver_df, silver_df)
    assert res.collect_schema().len() == 0


def test_clean_ingredients_missing_column() -> None:
    """Test clean_ingredients when 'active_ingredient' column is missing."""
    df = pl.DataFrame({"other": [1]}).lazy()
    res = clean_ingredients(df).collect()
    assert "active_ingredients_list" in res.columns
    assert res["active_ingredients_list"].dtype == pl.List(pl.String)
    assert res["active_ingredients_list"].to_list() == [[]]


def test_fix_dates_missing_column() -> None:
    """Test fix_dates when target column is missing."""
    df = pl.DataFrame({"other": ["2020-01-01"]}).lazy()
    res = fix_dates(df, ["missing_date_col"]).collect()
    assert "missing_date_col" not in res.columns
    assert res.height == 1


def test_fix_dates_non_string() -> None:
    """Test fix_dates when target column exists but is not string (already date?)."""
    from datetime import date

    df = pl.DataFrame({"my_date": [date(2023, 1, 1)]}).lazy()
    res = fix_dates(df, ["my_date"]).collect()
    assert res["my_date"][0] == date(2023, 1, 1)


def test_prepare_gold_products_missing_appl_type_and_marketing_id() -> None:
    """
    Test prepare_gold_products when 'appl_type' and 'marketing_status_id' are missing from BASE Silver DataFrame.
    """
    # Base Silver DataFrame missing 'appl_type' and 'marketing_status_id'
    silver_schema = {
        "appl_no": pl.String,
        "product_no": pl.String,
        # "appl_type": pl.String, # MISSING
        "drug_name": pl.String,
        "active_ingredients_list": pl.List(pl.String),
        "sponsor_name": pl.String,
        "te_code": pl.String,
        # "marketing_status_id": pl.Int64 # MISSING
    }
    silver_df = pl.DataFrame(
        {
            "appl_no": ["000001"],
            "product_no": ["001"],
            "drug_name": ["DrugA"],
            "active_ingredients_list": [["IngA"]],
            "sponsor_name": ["SponsorA"],
            "te_code": ["TE1"],
        },
        schema=silver_schema,
    ).lazy()

    df_empty = pl.DataFrame(schema={"appl_no": pl.String}).lazy()
    df_marketing_lookup = pl.DataFrame(
        schema={"marketing_status_id": pl.Int64, "marketing_status_description": pl.String}
    ).lazy()

    gold_df = prepare_gold_products(silver_df, df_empty, df_empty, df_marketing_lookup, df_empty, df_empty).collect()

    row = gold_df.row(0, named=True)

    # 1. Check is_generic logic when appl_type is missing
    assert row["is_generic"] is False

    # 2. Check search_vector generation when marketing_status_id is missing
    assert "search_vector" in row

    # 3. Check marketing_status_description logic when marketing_status_id is missing in silver
    assert "marketing_status_description" in row
    assert row["marketing_status_description"] is None


def test_generate_coreason_id_coverage() -> None:
    """Test generation of coreason_id to ensure coverage of internal UDF."""
    df = pl.DataFrame({"appl_no": ["000123"], "product_no": ["001"]}).lazy()

    res = generate_coreason_id(df).collect()

    row = res.row(0, named=True)
    assert "coreason_id" in row
    expected_uuid = str(uuid.uuid5(NAMESPACE_FDA, "000123|001"))
    assert row["coreason_id"] == expected_uuid


def test_generate_row_hash_list_coverage() -> None:
    """Test generate_row_hash with List columns to ensure coverage."""
    # col_list comes before col_str alphabetically (l vs s)
    df = pl.DataFrame({"col_str": ["A"], "col_list": [["X", "Y"]]}).lazy()

    res = generate_row_hash(df).collect()

    row = res.row(0, named=True)
    assert "hash_md5" in row
    # Hash of "X;Y|A" (col_list | col_str)
    expected = hashlib.md5("X;Y|A".encode()).hexdigest()
    assert row["hash_md5"] == expected


def test_generate_row_hash_nulls() -> None:
    """Test generate_row_hash with nulls."""
    df = pl.DataFrame(
        {"col_str": [None], "col_list": [None]}, schema={"col_str": pl.String, "col_list": pl.List(pl.String)}
    ).lazy()

    res = generate_row_hash(df).collect()
    row = res.row(0, named=True)
    # Nulls become empty strings. "|".
    expected = hashlib.md5("|".encode()).hexdigest()
    assert row["hash_md5"] == expected
