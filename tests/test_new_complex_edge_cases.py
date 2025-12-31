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


def test_submissions_mixed_case_filtering() -> None:
    """
    Test that 'ORIG' filtering is strict (case-sensitive) or flexible.
    Looking at source.py: `df.filter(pl.col("submission_type") == "ORIG")`
    This implies strict case sensitivity. "orig" should be IGNORED.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Product 001
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tDrugA")
        # Submissions: "orig" (lowercase) - should be ignored?
        # If strict, date will be None.
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\torig\t2000-01-01")

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["silver_products"])
        row = silver_prods[0]

        # Expect None because "orig" != "ORIG"
        assert row.original_approval_date is None


def test_exclusivity_invalid_dates() -> None:
    """
    Test Exclusivity date aggregation when dates are invalid.
    Invalid dates become Null (None). Max(None, Valid) -> Valid? Max(None) -> None?
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tDrugA")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2000-01-01")

        # Exclusivity:
        # Row 1: Invalid Date "INVALID" -> None
        # Row 2: Future Date "3000-01-01" -> Valid
        # Result should be Protected (Max > Today)
        z.writestr(
            "Exclusivity.txt",
            "ApplNo\tProductNo\tExclusivityDate\n000001\t001\tINVALID\n000001\t001\t3000-01-01",
        )

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])
        row = gold_prods[0]

        assert row.is_protected is True


def test_ghost_records_filtering() -> None:
    """
    Verify that records in auxiliary files (Marketing, TE, Exclusivity)
    that do not match a valid Product ApplNo are ignored (no ghost records).
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Product 001 exists.
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tDrugA")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2000-01-01")

        # Marketing Status has entry for 999999 (Non-existent Product)
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n999999\t001\t1")

        # TE has entry for 999999
        z.writestr("TE.txt", "ApplNo\tProductNo\tTECode\n999999\t001\tAB")

        # Exclusivity has entry for 999999
        z.writestr("Exclusivity.txt", "ApplNo\tProductNo\tExclusivityDate\n999999\t001\t3000-01-01")

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])

        # Should strictly contain 1 row (000001)
        assert len(gold_prods) == 1
        assert gold_prods[0].appl_no == "000001"


def test_empty_exclusivity_file() -> None:
    """
    Test that an Exclusivity file with only header (no rows) results in is_protected=False.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tDrugA")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2000-01-01")

        # Empty Exclusivity
        z.writestr("Exclusivity.txt", "ApplNo\tProductNo\tExclusivityDate")

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])
        row = gold_prods[0]

        assert row.is_protected is False


def test_submission_same_date_determinism() -> None:
    """
    Test multiple 'ORIG' submissions with the EXACT SAME date.
    Logic is `sort("sort_date").unique(subset=["appl_no"], keep="first")`.
    If dates are same, `unique` keeps the first one encountered in sort order.
    Sort is stable? Polars sort is stable.
    But original order in file matters.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tDrugA")
        # Two ORIG entries with same date but effectively duplicates.
        # This shouldn't crash or duplicate rows.
        z.writestr(
            "Submissions.txt",
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2000-01-01\n000001\tORIG\t2000-01-01",
        )

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["silver_products"])

        # Should be 1 row
        assert len(silver_prods) == 1
        assert silver_prods[0].original_approval_date == date(2000, 1, 1)
