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

from coreason_etl_drugs_fda.transform import fix_dates, prepare_gold_products


def test_marketing_status_optimistic_availability() -> None:
    """
    Verify that if a product has multiple marketing statuses,
    the logic picks the 'best' one based on ID sort order.
    1=Rx (Best?), 2=OTC, 3=Discontinued, 4=None
    Actually, the requirement says "sort MarketingStatusID in ascending order ... and selecting the first record".
    ID 1 (Rx) comes before 3 (Discontinued).
    So if both exist, it picks 1 (Active). This is 'Optimistic'.
    """
    # Setup Silver Data
    silver_df = pl.DataFrame(
        {
            "appl_no": ["000001"],
            "product_no": ["001"],
            "is_generic": [False],
            "is_protected": [False],
            "active_ingredients_list": [["Ing"]],
            "sponsor_name": ["Sponsor"],
            "te_code": [None],
        }
    ).lazy()

    # Marketing Data with Duplicates for same product
    # 1 = Rx (Prescription)
    # 3 = Discontinued
    # 4 = None (Tentative)
    marketing_df = pl.DataFrame(
        {
            "appl_no": ["000001", "000001", "000001"],
            "product_no": ["001", "001", "001"],
            "marketing_status_id": [3, 1, 4],  # Discontinued, Rx, None
        }
    ).lazy()

    # Empty Aux
    empty_df = pl.DataFrame().lazy()

    # Run Gold Logic
    gold_df = prepare_gold_products(
        silver_df,
        empty_df,  # Apps
        marketing_df,
        empty_df,  # Lookup
        empty_df,  # TE
        empty_df,  # Excl
    ).collect()

    # Verify
    assert len(gold_df) == 1
    # Should pick 1 (Rx) because it's the smallest ID
    assert gold_df["marketing_status_id"][0] == 1


def test_search_vector_complex_inputs() -> None:
    """
    Test search_vector generation with nulls, whitespace, and special chars.
    """
    silver_df = pl.DataFrame(
        {
            "appl_no": ["000001"],
            "product_no": ["001"],
            "drug_name": ["  My Drug  "],  # Whitespace
            "active_ingredients_list": [["Ing A", "Ing B"]],
            "sponsor_name": [None],  # Null
            # "te_code": ["AB"], # Removed from silver, provided via TE aux
            "marketing_status_id": [1],  # needed to prevent join errors if schema expects it
        }
    ).lazy()

    # Need generic cols
    silver_df = silver_df.with_columns(pl.lit(False).alias("is_generic"), pl.lit(False).alias("is_protected"))

    # Aux
    empty_df = pl.DataFrame().lazy()
    # Provide TE
    te_df = pl.DataFrame({"appl_no": ["000001"], "product_no": ["001"], "te_code": ["AB"]}).lazy()

    gold_df = prepare_gold_products(silver_df, empty_df, empty_df, empty_df, te_df, empty_df).collect()

    vector = gold_df["search_vector"][0]

    # Logic: DrugName + ActiveIngredients + SponsorName + TECode
    # "  My Drug  " -> "MY DRUG" (Upper + Strip?)
    # "Ing A", "Ing B" -> "ING A ING B"
    # Sponsor -> ""
    # TE -> "AB"
    # Concat with space: "MY DRUG ING A ING B  AB" -> normalized?
    # transform.py:
    #   pl.concat_str(..., separator=" ").str.strip_chars()
    #   AND ingredients are joined by " "
    #   AND drug_name is filled null ""

    # Note: The code doesn't explicitly strip inner whitespace of DrugName before concat,
    # but does .str.strip_chars() on the FINAL result.
    # Wait, transform.py says:
    # search_components.append(pl.col("drug_name").fill_null(""))
    # ...
    # pl.concat_str(..., separator=" ").str.strip_chars()
    # It does NOT strip input components individually before concat (except via clean_dataframe which strips strings).
    # clean_dataframe is called on Silver input? Yes, in pipeline.
    # But here we pass a raw dataframe to prepare_gold_products.
    # prepare_gold_products does NOT call clean_dataframe.
    # So "  My Drug  " might remain "  My Drug  " if not cleaned.
    # Then Uppercased.

    # Result: "  MY DRUG   ING A ING B  AB" (approx)
    # Then stripped: "MY DRUG   ING A ING B  AB"

    assert "MY DRUG" in vector
    assert "ING A" in vector
    assert "AB" in vector
    # Ensure no "None" string
    assert "None" not in vector


def test_fix_dates_historic_edge_cases() -> None:
    """
    Test fix_dates with:
    1. Exact legacy string.
    2. Variations (should NOT match).
    3. Valid dates.
    4. Invalid dates.
    """
    df = pl.DataFrame(
        {
            "date_col": [
                "Approved prior to Jan 1, 1982",
                "approved prior to jan 1, 1982",  # Case mismatch
                "1999-12-31",
                "2023-02-30",  # Invalid date
            ]
        }
    ).lazy()

    res = fix_dates(df, ["date_col"]).collect()

    # 1. Exact match -> 1982-01-01
    assert res["date_col"][0] == date(1982, 1, 1)
    assert res["is_historic_record"][0] is True

    # 2. Case mismatch -> Not replaced, tried to parse -> Null (parse failure)
    # The code uses `is_legacy = pl.col(col) == legacy_str` (exact match).
    # Then `.str.to_date(..., strict=False)`
    assert res["date_col"][1] is None
    assert res["is_historic_record"][1] is False

    # 3. Valid -> Parsed
    assert res["date_col"][2] == date(1999, 12, 31)

    # 4. Invalid -> Null
    assert res["date_col"][3] is None
