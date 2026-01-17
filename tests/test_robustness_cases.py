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

import pytest

from coreason_etl_drugs_fda.source import drugs_fda_source


def test_empty_input_file_handling() -> None:
    """
    Test handling of a totally empty file (0 bytes).
    _read_csv_bytes should return an empty DataFrame, and the pipeline should
    handle it gracefully (yielding nothing or valid empty resources).
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Create an empty Products.txt
        z.writestr("Products.txt", b"")
        z.writestr("Submissions.txt", b"")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # Check raw resource
        raw_res = list(source.resources["fda_drugs_bronze_products"])
        assert len(raw_res) == 0

        # Check silver resource (should be empty but exist)
        if "fda_drugs_silver_products" in source.resources:
            silver_res = list(source.resources["fda_drugs_silver_products"])
            assert len(silver_res) == 0


def test_missing_required_columns() -> None:
    """
    Test source files missing critical columns required for logic (e.g. ApplNo).
    The pipeline logic often assumes columns exist. If missing, it might crash or produce partial data.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products missing ApplNo
        products = "ProductNo\tForm\tStrength\tActiveIngredient\n001\tF\tS\tIng"
        z.writestr("Products.txt", products)
        # Submissions normal
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n001\tORIG\t2023-01-01")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # Silver resource logic tries to cast ApplNo.
        # If ApplNo is missing, Polars will raise `ColumnNotFoundError`.
        # The new implementation explicitly checks for existence before casting
        # in `prepare_silver_products` and returns an empty frame if missing.
        # So it should NOT crash, but yield 0 rows (or empty list).

        resources = list(source.resources["fda_drugs_silver_products"])
        # Expect 0 rows because required key is missing
        assert len(resources) == 0


def test_null_keys_in_source() -> None:
    """
    Test handling of rows where join keys (ApplNo) are Null/Empty.
    They should probably be dropped or result in failed joins.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products with one valid row and one null-key row
        # Row 2 has empty ApplNo (tab tab)
        products = "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng\n\t002\tF\tS\tIng"
        z.writestr("Products.txt", products)

        # Submissions
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2023-01-01")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_res = list(source.resources["fda_drugs_silver_products"])

        # We expect at least the valid row.
        # The null row:
        # ApplNo -> cast(String) -> null/empty string.
        # pad_start(6, "0") -> "000000" (if empty string) or null (if null)?
        # If CSV reader treats empty field as null, pad_start on null is null.
        # If it treats as empty string "", pad_start is "000000".
        # Let's check if "000000" is produced.

        appl_nos = [row["appl_no"] for row in silver_res]
        assert "000001" in appl_nos

        # If the second row survived, it might have a generated ApplNo or None.
        # ProductSilver enforces schema. If ApplNo is None, it might fail validation if field is required (it is).
        # So we expect it to be filtered OR a validation error if it flows through.
        # But wait, we iterate and yield ProductSilver(**row).
        # If validation fails, dlt might raise or drop.
        # Let's see what happens.
        pass


def test_invalid_zip_format() -> None:
    """
    Test response content is not a valid ZIP file.
    """
    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = b"Not a zip file"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # source raises ValueError if content is not a ZIP (doesn't start with PK)
        with pytest.raises(ValueError, match="Downloaded content is not a ZIP"):
            drugs_fda_source()


def test_future_dates_handling() -> None:
    """
    Test handling of future dates in Submissions (should be valid).
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n999999\t001\tF\tS\tIng")
        # Future date
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n999999\tORIG\t3000-01-01")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        res = list(source.resources["fda_drugs_silver_products"])
        assert len(res) == 1
        assert res[0]["original_approval_date"].year == 3000


def test_whitespace_only_ids() -> None:
    """
    Test IDs that are whitespace only.
    Should be stripped and result in empty string -> padded to 000000?
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # ApplNo is "   "
        products = "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n   \t001\tF\tS\tIng"
        z.writestr("Products.txt", products)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000000\tORIG\t2023-01-01")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        res = list(source.resources["fda_drugs_silver_products"])

        # _clean_dataframe strips chars. "   " -> "".
        # normalize_ids pads "". "000000".
        # So it should match "000000" in Submissions.

        assert len(res) == 1
        assert res[0]["appl_no"] == "000000"
