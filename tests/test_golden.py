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
    assert row_dict["source_id"] == expected["source_id"]
    assert row_dict["appl_no"] == expected["appl_no"]
    assert row_dict["product_no"] == expected["product_no"]
    assert row_dict["form"] == expected["form"]
    assert row_dict["strength"] == expected["strength"]

    # Check List
    # expected["active_ingredients_list"] is a string representation "['INGREDIENT A', 'INGREDIENT B']"
    # We need to parse it or just check elements
    assert row_dict["active_ingredients_list"] == ["INGREDIENT A", "INGREDIENT B"]

    # Check Date
    assert str(row_dict["original_approval_date"]) == expected["original_approval_date"]

    # Check Flag
    assert str(row_dict["is_historic_record"]) == str(expected["is_historic_record"])
