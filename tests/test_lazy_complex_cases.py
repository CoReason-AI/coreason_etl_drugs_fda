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


def test_lazy_type_inference_trap() -> None:
    """
    Complex Case: "Type Inference Trap".
    Simulate a CSV where the first chunk implies Int64 (e.g., '123') but later rows
    contain non-numeric strings (e.g., 'A123').
    Polars lazy reader with `infer_schema_length` might decide on Int64 and fail later.
    We verify if `_read_csv_bytes` (configured with `infer_schema_length=10000`) handles it,
    or if we need to adjust settings.
    Since we use `read_csv` (eager) then convert to lazy, the eager read happens first.
    If eager read fails, the whole pipeline fails.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Create a large-ish file where first N rows are ints
        rows = ["ApplNo\tProductNo\tForm\tStrength\tActiveIngredient"]
        # 100 rows of ints
        for i in range(100):
            rows.append(f"{i}\t001\tF\tS\tIng")
        # Then a string ID
        rows.append("A123\t001\tF\tS\tIng")

        content = "\n".join(rows)
        z.writestr("Products.txt", content)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n0\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        # If infer_schema_length is small (default 100) and we have 100 ints, it might infer Int.
        # Then fail on "A123".
        # We set `infer_schema_length=10000` in `source.py`. 100 rows should be fine (it reads all 10000 to infer).
        # So it should see "A123" and infer String.
        # This test ensures that configuration holds.

        # "A123" is technically invalid for the domain (ApplNo must be digits).
        # However, this test verifies that Polars READS it as a string (inference success)
        # instead of crashing with a schema mismatch (inference trap).
        # If Polars inferred Int64 based on first 100 rows, it would crash reading "A123".
        # Since it reads it, it passes it to Pydantic, which THEN raises ValidationError.
        # We expect ResourceExtractionError wrapping a ValidationError.

        from pydantic import ValidationError

        with pytest.raises(ResourceExtractionError) as excinfo:
            list(source.resources["silver_products"])

        # Verify it reached Pydantic validation (proving Polars read it successfully)
        assert isinstance(excinfo.value.__cause__, ValidationError)
        assert "string_pattern_mismatch" in str(excinfo.value.__cause__)


def test_lazy_deduplication_fanout() -> None:
    """
    Complex Case: Verify LazyFrame deduplication.
    Simulate `MarketingStatus` with duplicate entries for the same `ApplNo`/`ProductNo`.
    The code uses `unique` on LazyFrame. We verify this prevents fan-out.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # Duplicate Marketing Status
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1\n000001\t001\t1")
        z.writestr("MarketingStatus_Lookup.txt", "MarketingStatusID\tMarketingStatusDescription\n1\tDesc")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])

        assert len(gold_prods) == 1
        assert gold_prods[0].marketing_status_description == "Desc"


def test_massive_field_handling() -> None:
    """
    Edge Case: Massive String Field.
    Inject a row with a very large string value (e.g., 50k chars) to ensure buffer handling works.
    Polars usually handles large strings fine, but CSV parser limits might exist.
    """
    massive_str = "A" * 50000
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr(
            "Products.txt", f"ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\t{massive_str}"
        )
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
        # Check that ingredient list has the massive string
        assert len(silver_prods[0].active_ingredients_list[0]) == 50000


def test_mixed_newline_formats() -> None:
    """
    Edge Case: Mixed CRLF and LF in source files.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Mixed newlines
        content = (
            b"ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\r\n000001\t001\tF\tS\tIng1\n000002\t001\tF\tS\tIng2"
        )
        z.writestr("Products.txt", content)
        z.writestr(
            "Submissions.txt",
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01\n000002\tORIG\t2020-01-01",
        )

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["silver_products"])

        assert len(silver_prods) == 2
        ids = sorted([p.appl_no for p in silver_prods])
        assert ids == ["000001", "000002"]


def test_lazy_schema_evolution_extra_columns() -> None:
    """
    Complex Case: Extra columns in source files should not break the Lazy pipeline.
    Polars LazyFrame should carry them through or ignore them depending on selection.
    Gold logic selects specific columns for joins.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products has extra col
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\tExtraCol\n000001\t001\tF\tS\tIng\tExtraVal",
        )
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
        # Marketing has extra col
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\tNotes\n000001\t001\t1\tNote")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])

        assert len(gold_prods) == 1
        # Pipeline should succeed despite extra columns
