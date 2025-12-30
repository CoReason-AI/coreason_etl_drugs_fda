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

from coreason_etl_drugs_fda.source import drugs_fda_source


def test_ghost_records_in_aux_files() -> None:
    """
    Verify that records in auxiliary files (Marketing, TE) that do not match
    any `ApplNo` in `Products` are correctly ignored (no "ghost" rows in Gold).
    The pipeline is "Products-driven".
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Product 000001
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tIng1")
        # Submissions
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # MarketingStatus contains 000001 (Valid) AND 000999 (Ghost)
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1\n000999\t001\t2")

        # TE contains 000999 (Ghost)
        z.writestr("TE.txt", "ApplNo\tProductNo\tTECode\n000999\t001\tAB")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])

        # Should strictly be 1 row (000001)
        assert len(gold_prods) == 1
        assert gold_prods[0].appl_no == "000001"


def test_legacy_date_vs_older_real_date() -> None:
    """
    Verify that a real date (e.g., 1980) is selected over the
    "Approved prior to 1982" proxy date (1982-01-01) if the real date is earlier.
    This handles cases where data might be mixed.
    Legacy Logic Proxy: 1982-01-01.
    If we have another ORIG submission with 1980-01-01, min() should pick 1980.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tIng1")

        # Two ORIG submissions
        # 1. "Approved prior to Jan 1, 1982" -> Becomes 1982-01-01
        # 2. "1980-01-01" -> 1980-01-01 (Strictly earlier)
        content = (
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n"
            "000001\tORIG\tApproved prior to Jan 1, 1982\n"
            "000001\tORIG\t1980-01-01"
        )
        z.writestr("Submissions.txt", content)

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["silver_products"])

        assert len(silver_prods) == 1
        # 1980 is earlier than 1982, so it should win.
        assert silver_prods[0].original_approval_date == date(1980, 1, 1)


def test_search_vector_all_nulls() -> None:
    """
    Verify `search_vector` generation produces an empty string (not "None" or null)
    when all inputs (DrugName, Ingredient, Sponsor, TE) are missing.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Product with NO DrugName, NO Ingredient (empty)
        # Note: ActiveIngredient is required col in input usually, but if we provide it as empty or missing?
        # If missing col, clean_ingredients adds empty list.
        # "Products.txt" with minimal cols.
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\n000001\t001\tTab\t10mg")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # No Applications (Sponsor missing)
        # No TE (TE missing)

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])
        assert len(gold_prods) == 1
        row = gold_prods[0]

        # search_vector should be "" (empty string)
        # Logic: "" + "" + "" + "" -> ""
        assert row.search_vector == ""


def test_product_no_ingredients() -> None:
    """
    Verify handling of products with null/empty `ActiveIngredient`.
    Should result in empty list `active_ingredients_list` and not crash.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # ActiveIngredient is empty string
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\t")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["silver_products"])
        assert len(silver_prods) == 1
        row = silver_prods[0]

        # clean_ingredients: split(";") on "" -> [""]? Or if null -> []?
        # Correct behavior verified: it produces [] (empty list)
        assert row.active_ingredients_list == []


def test_submission_date_missing() -> None:
    """
    Verify that an `ORIG` submission with a missing/empty date field
    results in `None` for approval date, not a crash.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tIng1")
        # SubmissionStatusDate is empty
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["silver_products"])
        assert len(silver_prods) == 1
        # Should be None
        assert silver_prods[0].original_approval_date is None
