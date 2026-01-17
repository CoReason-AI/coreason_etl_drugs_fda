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
from dlt.extract.exceptions import ResourceExtractionError

from coreason_etl_drugs_fda.source import drugs_fda_source


def test_lazy_zero_row_inputs() -> None:
    """
    Test pipeline resilience when input files contain only headers (0 rows).
    The LazyFrame logic should handle this without error and yield 0 rows.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Header only files
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        # Should yield empty list, not crash
        silver_prods = list(source.resources["fda_drugs_silver_products"])
        assert len(silver_prods) == 0

        gold_prods = list(source.resources["fda_drugs_gold_products"])
        assert len(gold_prods) == 0


def test_lazy_missing_columns() -> None:
    """
    Test pipeline when `Products.txt` is missing required columns (e.g., Form).
    The Pydantic model requires 'form', but the transformation `clean_form` operates on it.
    If column is missing, `_clean_dataframe` works (renames what exists).
    But `clean_form` checks if "form" in cols. If not, it skips.
    Eventually `ProductSilver` Pydantic model will fail validation if field is missing.
    We expect ResourceExtractionError (wrapping ValidationError).
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Missing 'Form' column
        z.writestr("Products.txt", "ApplNo\tProductNo\tStrength\tActiveIngredient\n000001\t001\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # Should fail when validating against ProductSilver
        with pytest.raises(ResourceExtractionError) as excinfo:
            list(source.resources["fda_drugs_silver_products"])

        # dlt wraps the exception. Check message or cause.
        from dlt.common.schema.exceptions import DataValidationError

        assert isinstance(excinfo.value.__cause__, DataValidationError)


def test_lazy_join_type_mismatch() -> None:
    """
    Test joining when keys have mismatched types in source (Int vs String).
    Products: ApplNo is Int (123)
    Submissions: ApplNo is String ("000123")
    The lazy logic must cast both to String and pad them correctly to join.
    Refactored `source.py` adds `cast(pl.String).str.pad_start(6, '0')`.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products: ApplNo is unquoted int 123
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n123\t1\tF\tS\tIng")
        # Submissions: ApplNo is unquoted 000123 (might be read as int or string depending on parser)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000123\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["fda_drugs_silver_products"])

        assert len(silver_prods) == 1
        row = silver_prods[0]
        # Should have joined date
        assert row["appl_no"] == "000123"
        assert str(row["original_approval_date"]) == "2020-01-01"


def test_lazy_whitespace_keys() -> None:
    """
    Test keys that are only whitespace.
    `_clean_dataframe` strips chars, making it "".
    Then `pad_start(6, '0')` makes it "000000".
    It should NOT be skipped, but treated as ID "000000".
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # ApplNo is whitespace
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n   \t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n   \tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        prods = list(source.resources["fda_drugs_silver_products"])

        # It should be present as 000000
        assert len(prods) == 1
        assert prods[0]["appl_no"] == "000000"
