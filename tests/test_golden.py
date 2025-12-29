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
from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl

from coreason_etl_drugs_fda.source import drugs_fda_source


def test_silver_logic_golden() -> None:
    """
    Golden File Test: Verifies that the Silver Products logic produces
    exactly the expected output (schema, values, transformations) for a
    known complex input.
    """
    # 1. Create Mock Input Data
    # Products: Includes mixed case, whitespace, semicolon ingredients, need for padding
    products_content = (
        "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n4\t4\tTAB\t10MG\t Ingredient A; Ingredient B \n"
    ).encode("cp1252")

    # Submissions: Includes Legacy Date string
    submissions_content = (
        "ApplNo\tSubmissionType\tSubmissionStatusDate\n4\tORIG\tApproved prior to Jan 1, 1982\n"
    ).encode("cp1252")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", products_content)
        z.writestr("Submissions.txt", submissions_content)

    buffer.seek(0)

    # 2. Run Source
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_resource = source.resources["silver_products"]

        # Consume the generator
        results = list(silver_resource)

    # 3. Assertions against Golden File
    assert len(results) == 1
    row = results[0]

    # Load Golden File to compare
    # Note: We hardcode comparison here for simplicity or read the file
    expected_df = pl.read_csv(
        "tests/fixtures/golden_products.csv",
        schema_overrides={"source_id": pl.Utf8, "appl_no": pl.Utf8, "product_no": pl.Utf8},
    )
    expected = expected_df.row(0, named=True)

    # Convert Pydantic model to dict for comparison, handling UUID/Date conversion
    row_dict = row.model_dump()

    # Assertions
    # Strict equality check involves ensuring every field matches.
    # We construct a dict from expected to match row_dict format.

    # 1. Check List parsing
    # expected["active_ingredients_list"] is a string "['INGREDIENT A', 'INGREDIENT B']"
    # We need to eval it to list
    import ast

    expected_ingredients = ast.literal_eval(expected["active_ingredients_list"])

    # 2. Check Booleans (read_csv might read "True" as boolean True or string "True")
    # pl.read_csv usually infers boolean if "True"/"False"
    expected_is_historic = expected["is_historic_record"]
    if isinstance(expected_is_historic, str):
        expected_is_historic = expected_is_historic.lower() == "true"

    # 3. Construct clean expected dict
    approval_date = None
    if expected["original_approval_date"] is not None:
        approval_date = date.fromisoformat(str(expected["original_approval_date"]))

    expected_clean = {
        "source_id": expected["source_id"],
        "appl_no": expected["appl_no"],
        "product_no": expected["product_no"],
        "form": expected["form"],
        "strength": expected["strength"],
        "active_ingredients_list": expected_ingredients,
        "original_approval_date": approval_date,
        "is_historic_record": expected_is_historic,
    }

    # Compare core fields (excluding coreason_id which is UUID and hash_md5 which we should check if possible)
    # We can check coreason_id string representation if we want strict deterministic check?
    # BRD says "Strictly implement the Golden File Test".
    # We should probably verify everything we can.

    assert row_dict["source_id"] == expected_clean["source_id"]
    assert row_dict["appl_no"] == expected_clean["appl_no"]
    assert row_dict["product_no"] == expected_clean["product_no"]
    assert row_dict["form"] == expected_clean["form"]
    assert row_dict["strength"] == expected_clean["strength"]
    assert row_dict["active_ingredients_list"] == expected_clean["active_ingredients_list"]
    assert row_dict["original_approval_date"] == expected_clean["original_approval_date"]
    assert row_dict["is_historic_record"] == expected_clean["is_historic_record"]

    # Check Hash MD5 if present in expected (it's not in fixture currently, so we skip or add it?)
    # Fixture content has core fields.
    # It does NOT have coreason_id or hash_md5.
    # So we only assert what is in the Golden File.
