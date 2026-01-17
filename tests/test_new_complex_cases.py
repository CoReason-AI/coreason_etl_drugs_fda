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
from coreason_etl_drugs_fda.transform import fix_dates


def test_te_code_determinism() -> None:
    """
    Test which TE code is picked when multiple exist for the same product.
    Polars `unique(subset=..., keep='first')` should pick the first one encountered in the file.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # TE File has two codes: "AB" first, "XY" second.
        z.writestr("TE.txt", "ApplNo\tProductNo\tTECode\n000001\t001\tAB\n000001\t001\tXY")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["fda_drugs_gold_products"])

        assert len(gold_prods) == 1
        # Should pick "AB" (first)
        assert gold_prods[0]["te_code"] == "AB"


def test_date_parsing_invalid_dates() -> None:
    """
    Test parsing of logically invalid dates (e.g. Feb 30th).
    Should result in null/None without crashing.
    """
    df = pl.DataFrame({"date_col": ["2023-02-30", "2023-02-28"]})

    # fix_dates modifies in place (returns new df with same name)
    result = fix_dates(df, ["date_col"])

    # Invalid date -> None
    assert result["date_col"][0] is None
    # Valid date -> Date object
    from datetime import date

    assert result["date_col"][1] == date(2023, 2, 28)


def test_massive_ingredient_list() -> None:
    """
    Test handling of a very large ingredient string (e.g., 1000 ingredients).
    Ensures no buffer overflows or unexpected truncation.
    """
    # Create 1000 ingredients: "ING0", "ING1", ...
    ingredients = [f"ING{i}" for i in range(1000)]
    ing_str = ";".join(ingredients)

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Write large content
        content = f"ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\t{ing_str}"
        z.writestr("Products.txt", content)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_res = list(source.resources["fda_drugs_silver_products"])

        assert len(silver_res) == 1
        row = silver_res[0]

        assert len(row["active_ingredients_list"]) == 1000
        assert row["active_ingredients_list"][0] == "ING0"
        assert row["active_ingredients_list"][999] == "ING999"
