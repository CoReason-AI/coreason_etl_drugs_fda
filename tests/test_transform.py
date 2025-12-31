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

from coreason_etl_drugs_fda.transform import clean_form, clean_ingredients, fix_dates, normalize_ids


def test_normalize_ids() -> None:
    # Test with integers
    df = pl.DataFrame({"appl_no": [4, 123456], "product_no": [4, 1]})
    result = normalize_ids(df)
    assert result["appl_no"][0] == "000004"
    assert result["appl_no"][1] == "123456"
    assert result["product_no"][0] == "004"
    assert result["product_no"][1] == "001"

    # Test with strings (mixed length)
    df_str = pl.DataFrame({"appl_no": ["123", "001234"], "product_no": ["1", "002"]})
    result_str = normalize_ids(df_str)
    assert result_str["appl_no"][0] == "000123"
    assert result_str["appl_no"][1] == "001234"
    assert result_str["product_no"][0] == "001"
    assert result_str["product_no"][1] == "002"


def test_normalize_ids_empty_strings() -> None:
    """Test that empty strings or whitespace become nulls, not '000000'."""
    df = pl.DataFrame({"appl_no": ["", "   ", None, "123"], "product_no": ["", " ", None, "1"]})

    result = normalize_ids(df)

    # Check appl_no
    assert result["appl_no"][0] is None
    assert result["appl_no"][1] is None
    assert result["appl_no"][2] is None
    assert result["appl_no"][3] == "000123"

    # Check product_no
    assert result["product_no"][0] is None
    assert result["product_no"][1] is None
    assert result["product_no"][2] is None
    assert result["product_no"][3] == "001"


def test_fix_dates() -> None:
    legacy = "Approved prior to Jan 1, 1982"
    df = pl.DataFrame({"approval_date": [legacy, "2023-01-01", "invalid"], "other_col": [1, 2, 3]})

    # We expect fix_dates to handle the conversion to Date type as well, based on implementation logic
    result = fix_dates(df, ["approval_date"])

    # Check is_historic_record
    assert "is_historic_record" in result.columns
    assert result["is_historic_record"][0]
    assert not result["is_historic_record"][1]

    # Check date conversion
    assert result["approval_date"].dtype == pl.Date
    assert result["approval_date"][0] == date(1982, 1, 1)
    assert result["approval_date"][1] == date(2023, 1, 1)
    # Invalid should be null
    assert result["approval_date"][2] is None


def test_fix_dates_missing_column() -> None:
    df = pl.DataFrame({"a": [1]})
    # Should not crash
    result = fix_dates(df, ["non_existent_col"])
    assert "non_existent_col" not in result.columns


def test_clean_ingredients() -> None:
    df = pl.DataFrame({"active_ingredient": ["Ingredient A; Ingredient B ", "INGREDIENT C", "  ingredient d  "]})

    result = clean_ingredients(df)

    # Check output column exists
    assert "active_ingredients_list" in result.columns
    assert "active_ingredient" not in result.columns

    # Check splitting and cleaning
    row1 = result["active_ingredients_list"][0]
    assert len(row1) == 2
    assert row1[0] == "INGREDIENT A"
    assert row1[1] == "INGREDIENT B"

    row2 = result["active_ingredients_list"][1]
    assert len(row2) == 1
    assert row2[0] == "INGREDIENT C"

    row3 = result["active_ingredients_list"][2]
    assert len(row3) == 1
    assert row3[0] == "INGREDIENT D"


def test_clean_ingredients_missing_column() -> None:
    """Test behavior when active_ingredient column is missing."""
    df = pl.DataFrame({"other_col": [1, 2]})
    result = clean_ingredients(df)

    assert "active_ingredients_list" in result.columns
    # Should be empty list
    assert result["active_ingredients_list"][0].to_list() == []
    # Original column definitely not there
    assert "active_ingredient" not in result.columns
    assert "other_col" in result.columns


def test_clean_ingredients_null_values() -> None:
    """Test behavior with null values."""
    df = pl.DataFrame({"active_ingredient": [None, "A; B"]})
    result = clean_ingredients(df)

    # We now expect empty list for null input
    assert result["active_ingredients_list"][0].to_list() == []
    assert result["active_ingredients_list"][1].to_list() == ["A", "B"]


def test_clean_ingredients_empty_strings() -> None:
    """Test that empty strings result in empty lists, not ['']."""
    df = pl.DataFrame({"active_ingredient": ["", "  ", "A; ;B", ";"]})
    result = clean_ingredients(df)

    # "" -> split -> [""] -> filter len>0 -> []
    assert result["active_ingredients_list"][0].to_list() == []
    # "  " -> split -> ["  "] -> strip -> [""] -> filter -> []
    assert result["active_ingredients_list"][1].to_list() == []
    # "A; ;B" -> split -> ["A", " ", "B"] -> strip -> ["A", "", "B"] -> filter -> ["A", "B"]
    assert result["active_ingredients_list"][2].to_list() == ["A", "B"]
    # ";" -> split -> ["", ""] -> strip -> ["", ""] -> filter -> []
    assert result["active_ingredients_list"][3].to_list() == []


def test_clean_form() -> None:
    """Test clean_form Title Casing."""
    df = pl.DataFrame({"form": ["TABLET", "solution/drops"]})
    result = clean_form(df)
    assert result["form"][0] == "Tablet"
    assert result["form"][1] == "Solution/Drops"


def test_clean_form_missing_column() -> None:
    """Test clean_form with missing 'form' column."""
    df = pl.DataFrame({"other": [1]})
    result = clean_form(df)
    assert "form" not in result.columns
    assert "other" in result.columns
