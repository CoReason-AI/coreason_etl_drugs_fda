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

import pytest

from coreason_etl_drugs_fda.source import _extract_approval_dates, _read_file_from_zip, drugs_fda_source


@pytest.fixture  # type: ignore[misc]
def mock_zip_content() -> bytes:
    """Creates a mock ZIP file in memory containing sample TSV files."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Create Products.txt
        # ApplNo 000004 has match in Submissions.
        # ApplNo 000005 has NO match.
        products_content = (
            "ApplNo\tProductNo\tForm\tStrength\tReferenceDrug\tDrugName\tActiveIngredient\tReferenceStandard\n"
            "000004\t004\tSOLUTION/DROPS;OPHTHALMIC\t1%\t0\tPAREDRINE\tHYDROXYAMPHETAMINE HYDROBROMIDE\t0\n"
            "   000005   \t005\tTABLET   \t5MG   \t0\tTESTDRUG\tTESTINGREDIENT\t0"
        )
        z.writestr("Products.txt", products_content)

        # Create Submissions.txt
        # 000004: ORIG, AP, 1982-01-01
        submissions_content = (
            "ApplNo\tSubmissionClassCodeID\tSubmissionType\tSubmissionNo\tSubmissionStatus\tSubmissionStatusDate\tReviewPriorityID\n"
            "000004\t7\tORIG\t1\tAP\t1982-01-01\t0\n"
            "000006\t7\tSUPPL\t2\tAP\t2023-01-01\t0"
        )
        z.writestr("Submissions.txt", submissions_content)

    buffer.seek(0)
    return buffer.getvalue()


def test_drugs_fda_source_extraction(mock_zip_content: bytes) -> None:
    """
    Test that the source correctly extracts, parses, and cleans data from the ZIP.
    Also verifies the 'silver_products' resource.
    """
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_zip_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Initialize the source
        source = drugs_fda_source()

        # Check resources
        resources = source.resources
        assert "raw_fda__products" in resources
        assert "raw_fda__submissions" in resources
        assert "silver_products" in resources

        # 1. Verify Raw Products
        raw_prod = list(resources["raw_fda__products"])
        assert len(raw_prod) == 2
        assert raw_prod[0]["appl_no"] == "000004"
        assert raw_prod[0]["active_ingredient"] == "HYDROXYAMPHETAMINE HYDROBROMIDE"

        # 2. Verify Silver Products
        silver_prod = list(resources["silver_products"])
        assert len(silver_prod) == 2

        row1 = silver_prod[0]
        # Check Padded IDs
        assert row1["appl_no"] == "000004"
        assert row1["product_no"] == "004"
        # Check Date Join
        assert row1["original_approval_date"] == date(1982, 1, 1)
        # Check Historic Record Logic
        assert not row1["is_historic_record"]
        # Check Active Ingredient List
        assert row1["active_ingredient"] == ["HYDROXYAMPHETAMINE HYDROBROMIDE"]
        # Check UUID
        assert "coreason_id" in row1
        assert "hash_md5" in row1

        row2 = silver_prod[1]
        assert row2["appl_no"] == "000005"
        # Check No Date Join
        assert row2["original_approval_date"] is None


def test_silver_products_legacy_date(mock_zip_content: bytes) -> None:
    """Test legacy date string handling in silver_products."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        products = "ApplNo\tProductNo\tActiveIngredient\n000007\t001\tIng"
        z.writestr("Products.txt", products)
        # Submissions with legacy string
        submissions = "ApplNo\tSubmissionType\tSubmissionStatusDate\n000007\tORIG\tApproved prior to Jan 1, 1982"
        z.writestr("Submissions.txt", submissions)
    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prod = list(source.resources["silver_products"])
        row = silver_prod[0]

        assert row["original_approval_date"] == date(1982, 1, 1)
        assert row["is_historic_record"] is True


def test_read_file_from_zip_missing() -> None:
    """Test _read_file_from_zip with non-existent file."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("exists.txt", "col\nval")

    # This generator should yield nothing
    gen = _read_file_from_zip(buffer.getvalue(), "missing.txt")
    assert list(gen) == []


def test_extract_approval_dates_missing_file() -> None:
    """Test _extract_approval_dates when Submissions.txt is missing."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "col\nval")

    dates = _extract_approval_dates(buffer.getvalue())
    assert dates == {}


def test_extract_approval_dates_missing_columns() -> None:
    """Test _extract_approval_dates with malformed Submissions.txt."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Missing SubmissionType or SubmissionStatusDate
        z.writestr("Submissions.txt", "ApplNo\tWrongCol\n123\tval")

    dates = _extract_approval_dates(buffer.getvalue())
    assert dates == {}


def test_silver_products_empty_dates() -> None:
    """Test silver_products_resource when no approval dates are found (empty dates_df)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        products = "ApplNo\tProductNo\tActiveIngredient\n000008\t001\tIng"
        z.writestr("Products.txt", products)
        # Submissions has no ORIG
        submissions = "ApplNo\tSubmissionType\tSubmissionStatusDate\n000008\tSUPPL\t2023-01-01"
        z.writestr("Submissions.txt", submissions)
    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        # Should yield silver products, but with null dates
        silver_prod = list(source.resources["silver_products"])
        assert len(silver_prod) == 1
        assert silver_prod[0]["original_approval_date"] is None
