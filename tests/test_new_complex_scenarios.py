# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

from datetime import date, timedelta

import polars as pl

from coreason_etl_drugs_fda.transform import (
    clean_ingredients,
    extract_orig_dates,
    prepare_gold_products,
)


def test_dirty_ingredients_normalization() -> None:
    """
    Test clean_ingredients with dirty input:
    - Multiple delimiters
    - Extra whitespace
    - Empty tokens
    """
    df = pl.DataFrame(
        {
            "active_ingredient": [
                "Ingredient A;  Ingredient B ;",  # Standard trailing semi
                "Ing A;;Ing B",  # Double semi
                "  Ing A  ;  Ing B  ",  # Whitespace
                ";;",  # Only delimiters
                None,  # Null
            ]
        }
    ).lazy()

    res = clean_ingredients(df).collect()

    # 1. Standard
    assert res["active_ingredients_list"][0].to_list() == ["INGREDIENT A", "INGREDIENT B"]
    # 2. Double semi
    assert res["active_ingredients_list"][1].to_list() == ["ING A", "ING B"]
    # 3. Whitespace
    assert res["active_ingredients_list"][2].to_list() == ["ING A", "ING B"]
    # 4. Only delimiters -> Empty list
    assert res["active_ingredients_list"][3].to_list() == []
    # 5. Null -> Empty list
    assert res["active_ingredients_list"][4].to_list() == []


def test_marketing_lookup_missing_key() -> None:
    """
    Test Gold join when MarketingStatusID exists in Product but NOT in Lookup table.
    Should result in Null description.
    """
    # Silver DF must use normalized IDs as prepare_gold_products expects them
    # AND explicitly typed list for ingredients
    silver_schema = {
        "appl_no": pl.String,
        "product_no": pl.String,
        "drug_name": pl.String,
        "active_ingredients_list": pl.List(pl.String),
        "sponsor_name": pl.String,
        "te_code": pl.String,
    }

    silver_df = pl.DataFrame(
        {
            "appl_no": ["000001"],
            "product_no": ["001"],
            "drug_name": ["D"],
            "active_ingredients_list": [[]],
            "sponsor_name": ["S"],
            "te_code": ["T"],
        },
        schema=silver_schema,
    ).lazy()

    # Lookup table has other IDs (1), but we will inject 999 via marketing_df
    lookup_df = pl.DataFrame({"marketing_status_id": [1], "marketing_status_description": ["Rx"]}).lazy()

    empty = pl.DataFrame().lazy()

    # Marketing DF provides the link: ApplNo -> MarketingStatusID
    # prepare_gold_products will normalize this ID (001 -> 000001)
    marketing_df = pl.DataFrame({"appl_no": ["001"], "product_no": ["001"], "marketing_status_id": [999]}).lazy()

    gold_df = prepare_gold_products(silver_df, empty, marketing_df, lookup_df, empty, empty).collect()

    # Join 1: Silver (000001) <-> Marketing (000001) -> gets ID 999
    # Join 2: ID 999 <-> Lookup (has 1) -> No match -> description is None

    row = gold_df.row(0, named=True)
    assert row["marketing_status_id"] == 999
    assert row["marketing_status_description"] is None


def test_exclusivity_expiry_boundary() -> None:
    """
    Test is_protected logic at the date boundary.
    Protected if today < max_exclusivity_date.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    silver_schema = {
        "appl_no": pl.String,
        "product_no": pl.String,
        "drug_name": pl.String,
        "active_ingredients_list": pl.List(pl.String),
    }

    silver_df = pl.DataFrame(
        {
            "appl_no": ["000001", "000002", "000003"],
            "product_no": ["001", "001", "001"],
            "drug_name": ["D1", "D2", "D3"],
            "active_ingredients_list": [[], [], []],
        },
        schema=silver_schema,
    ).lazy()

    excl_df = pl.DataFrame(
        {
            "appl_no": ["001", "002", "003"],  # Will be normalized
            "product_no": ["001", "001", "001"],
            "exclusivity_date": [
                yesterday.isoformat(),
                today.isoformat(),
                tomorrow.isoformat(),
            ],
        }
    ).lazy()

    empty = pl.DataFrame().lazy()

    gold_df = prepare_gold_products(silver_df, empty, empty, empty, empty, excl_df).collect()

    gold_df = gold_df.sort("appl_no")

    # 000001: Yesterday -> Not Protected
    assert gold_df.filter(pl.col("appl_no") == "000001")["is_protected"][0] is False

    # 000002: Today -> Not Protected (Strict inequality: today < today is False)
    assert gold_df.filter(pl.col("appl_no") == "000002")["is_protected"][0] is False

    # 000003: Tomorrow -> Protected
    assert gold_df.filter(pl.col("appl_no") == "000003")["is_protected"][0] is True


def test_submission_type_filtering() -> None:
    """
    Test that extract_orig_dates ignores non-ORIG submissions.
    """
    df = pl.DataFrame(
        {
            "appl_no": ["001", "002"],
            "submission_type": ["SUPPL", "ORIG"],
            "submission_status_date": ["2000-01-01", "2020-01-01"],
        }
    ).lazy()

    res = extract_orig_dates(df)

    # 001 has only SUPPL -> Should not be in result (dict keys are normalized)
    assert "000001" not in res

    # 002 has ORIG -> Should be in result as 000002
    assert "000002" in res
    assert str(res["000002"]) == "2020-01-01"
