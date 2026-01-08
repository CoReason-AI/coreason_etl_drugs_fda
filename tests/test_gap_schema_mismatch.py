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

from coreason_etl_drugs_fda.source import drugs_fda_source


def test_submissions_schema_mismatch_missing_columns() -> None:
    """
    Test resilience when `Submissions.txt` exists but is missing required columns
    (e.g., `SubmissionType` or `SubmissionStatusDate`).
    The `extract_orig_dates` function should gracefully handle this by returning an empty map,
    rather than crashing with a ColumnNotFoundError during lazy evaluation.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")

        # Submissions file exists but MISSING `SubmissionType`
        z.writestr("Submissions.txt", "ApplNo\tWrongColumn\n000001\tData")

    buffer.seek(0)

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # This should NOT crash.
        # extract_orig_dates checks for columns before filtering.
        silver_prods = list(source.resources["fda_drugs_silver_products"])

        assert len(silver_prods) == 1
        row = silver_prods[0]

        # Approval date should be None since we couldn't parse submissions
        assert row.original_approval_date is None


def test_marketing_lookup_schema_mismatch() -> None:
    """
    Test resilience when `MarketingStatus_Lookup.txt` is missing the `MarketingStatusDescription` column.
    The join logic should verify columns exist before joining, avoiding a crash.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1")

        # Lookup file exists but missing Description column
        z.writestr("MarketingStatus_Lookup.txt", "MarketingStatusID\tWrongCol\n1\tVal")

    buffer.seek(0)

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["fda_drugs_gold_drug_product"])

        assert len(gold_prods) == 1
        row = gold_prods[0]

        # Should be None/Null, not crash
        assert row.marketing_status_description is None
