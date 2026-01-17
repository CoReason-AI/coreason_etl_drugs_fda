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

from coreason_etl_drugs_fda.transform import clean_ingredients, normalize_ids


def test_normalize_ids_complex_whitespace() -> None:
    """
    Test normalize_ids with various whitespace characters (tabs, newlines).
    These should be stripped and treated as empty (Null).
    """
    df = pl.DataFrame(
        {"appl_no": ["\t", "\n", "\r", " \t \n ", "123"], "product_no": ["\t", "\n", "\r", " \t \n ", "1"]}
    )

    result = normalize_ids(df)

    # All whitespace-only strings should become None
    assert result["appl_no"][0] is None
    assert result["appl_no"][1] is None
    assert result["appl_no"][2] is None
    assert result["appl_no"][3] is None
    # Valid value preserved and padded
    assert result["appl_no"][4] == "000123"

    assert result["product_no"][0] is None
    assert result["product_no"][4] == "001"


def test_normalize_ids_zero_handling() -> None:
    """
    Verify the distinction between empty strings (Ghost Records -> Null)
    and explicit "0" strings (Data -> Padded).
    """
    df = pl.DataFrame({"appl_no": ["", "0", "00", "000"], "product_no": ["", "0", "00", "000"]})

    result = normalize_ids(df)

    # Empty -> None
    assert result["appl_no"][0] is None

    # "0" -> "000000" (It is technically value '0', processed as data)
    # This behavior confirms we are not aggressively killing zeros, only empty/whitespace.
    assert result["appl_no"][1] == "000000"
    assert result["appl_no"][2] == "000000"
    assert result["appl_no"][3] == "000000"


def test_clean_ingredients_massive_chaos() -> None:
    """
    Test clean_ingredients with a massive string of delimiters and whitespace.
    """
    # Construct a chaos string: 1000 semicolons, spaces, and valid ingredients
    chaos = ";" * 1000 + "  Ingredient A  ;  " + "; " * 500 + "Ingredient B" + ";" * 1000

    df = pl.DataFrame({"active_ingredient": [chaos]})
    result = clean_ingredients(df)

    ingredients = result["active_ingredients_list"][0].to_list()

    # Should cleanly reduce to just the two ingredients
    assert len(ingredients) == 2
    assert ingredients[0] == "INGREDIENT A"
    assert ingredients[1] == "INGREDIENT B"


def test_clean_ingredients_all_delimiters() -> None:
    """
    Test input that is ONLY delimiters.
    """
    df = pl.DataFrame({"active_ingredient": [";;;;;;;;;", "   ;   ;   "]})
    result = clean_ingredients(df)

    assert result["active_ingredients_list"][0].to_list() == []
    assert result["active_ingredients_list"][1].to_list() == []


def test_mixed_types_resilience() -> None:
    """
    Test that the functions handle mixed types (though Polars schema usually enforces one).
    If we somehow have Ints and Strings, verify normalize_ids handles casting robustly.
    """
    # Polars DataFrame enforces column type, so we can't easily have mixed Int/Str in one col
    # unless it's Object (which Polars discourages/doesn't fully support like Pandas).
    # But we can test Integer column input specifically again.

    df = pl.DataFrame(
        {
            "appl_no": [1, 10, 100, 1000, 10000, 100000, 1000000],
            "product_no": [1, 10, 100, 1000, 10000, 100000, 1000000],
        }
    )

    result = normalize_ids(df)

    assert result["appl_no"][0] == "000001"
    assert result["appl_no"][5] == "100000"
    assert result["appl_no"][6] == "1000000"  # Should not truncate

    assert result["product_no"][0] == "001"
    assert result["product_no"][2] == "100"
    assert result["product_no"][3] == "1000"  # Should not truncate
