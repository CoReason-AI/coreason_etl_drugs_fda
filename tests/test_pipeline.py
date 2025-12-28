# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_drugs_fda.pipeline import create_pipeline, run_pipeline, silver_products_transformer
from coreason_etl_drugs_fda.source import drugs_fda_source


@pytest.fixture  # type: ignore[misc]
def mock_zip_content_integration() -> bytes:
    import io
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products
        products = (
            "ApplNo\tProductNo\tForm\tStrength\tReferenceDrug\tDrugName\tActiveIngredient\tReferenceStandard\n"
            "000004\t004\tSOLUTION/DROPS;OPHTHALMIC\t1%\t0\tPAREDRINE\tHYDROXYAMPHETAMINE HYDROBROMIDE\t0"
        )
        z.writestr("Products.txt", products)

        # Submissions
        submissions = (
            "ApplNo\tSubmissionClassCodeID\tSubmissionType\tSubmissionNo\tSubmissionStatus\tSubmissionStatusDate\tReviewPriorityID\n"
            "000004\t7\tORIG\t1\tAP\t1982-01-01\t0"
        )
        z.writestr("Submissions.txt", submissions)

        # Exclusivity
        exclusivity = "ApplNo\tProductNo\tExclusivityCode\tExclusivityDate\n000004\t004\tODE\t2025-01-01"
        z.writestr("Exclusivity.txt", exclusivity)

    buffer.seek(0)
    return buffer.getvalue()


def test_pipeline_bronze_ingestion(mock_zip_content_integration: bytes) -> None:
    """
    Test that the pipeline extracts all required files (Products, Submissions, Exclusivity).
    """
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_zip_content_integration
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Create pipeline
        # We use a custom destination or 'duckdb' if available, else 'dummy'
        # For testing logic, we can just inspect the source resources again or run with destination='dummy'

        source = drugs_fda_source()

        # Check resources exist
        resources = source.resources
        assert "raw_fda__products" in resources
        assert "raw_fda__submissions" in resources
        assert "raw_fda__exclusivity" in resources

        # Check content of Exclusivity
        excl_data = list(resources["raw_fda__exclusivity"])
        assert len(excl_data) == 1
        assert excl_data[0]["exclusivity_code"] == "ODE"


def test_silver_products_transformer() -> None:
    """
    Test the silver transformer logic on a chunk of data.
    """
    # Input data (raw dicts from dlt)
    # Note: Bronze layer might give ints or strings depending on Polars inference.
    # Based on previous tests, '004' became 4.
    raw_chunk = [
        {
            "appl_no": 4,
            "product_no": 4,
            "form": "TABLET",
            "active_ingredient": "INGREDIENT A; INGREDIENT B",
            "foo": "bar",  # Extra column
        }
    ]

    # Run transformer
    # It expects iterator of lists
    transformer = silver_products_transformer(iter([raw_chunk]))

    result_chunk = next(transformer)
    assert len(result_chunk) == 1
    row = result_chunk[0]

    # Check Normalization
    assert row["appl_no"] == "000004"
    assert row["product_no"] == "004"

    # Check Cleaning
    assert row["active_ingredients_list"] == ["INGREDIENT A", "INGREDIENT B"]

    # Check IDs
    assert "coreason_id" in row
    assert "hash_md5" in row


def test_run_pipeline_execution() -> None:
    """
    Test that run_pipeline executes without error.
    We mock create_pipeline and drugs_fda_source.
    """
    with patch("coreason_etl_drugs_fda.pipeline.create_pipeline") as mock_create:
        with patch("coreason_etl_drugs_fda.pipeline.drugs_fda_source") as mock_source:
            mock_pipeline = MagicMock()
            mock_create.return_value = mock_pipeline

            mock_source.return_value = ["res1"]  # Mock source yielding resources or being iterable

            run_pipeline()

            # Verify pipeline.run was called with source
            mock_pipeline.run.assert_called_once()


def test_create_pipeline() -> None:
    p = create_pipeline(destination="dummy", dataset_name="test_ds")
    assert p.pipeline_name == "coreason_drugs_fda"
    assert p.dataset_name.startswith("test_ds")


def test_silver_products_transformer_empty() -> None:
    """Test handling of empty chunks or none."""
    transformer = silver_products_transformer(iter([[]]))
    # It should yield nothing or handle empty list gracefully
    # Our implementation checks `if not chunk: continue`
    results = list(transformer)
    assert len(results) == 0
