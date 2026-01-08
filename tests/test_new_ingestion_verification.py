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


def test_submissions_ingestion_and_orig_filtering() -> None:
    """
    Verifies that Submissions.txt is ingested and strictly filtered for 'ORIG' types
    when determining the Original Approval Date.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Product 001
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tDrugA",
        )
        # Submissions:
        # - ORIG on 2000-01-01
        # - SUPPL on 1999-01-01 (Earlier, but should be ignored)
        # - UNKNOWN on 2001-01-01
        z.writestr(
            "Submissions.txt",
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n"
            "000001\tORIG\t2000-01-01\n"
            "000001\tSUPPL\t1999-01-01\n"
            "000001\tUNKNOWN\t2001-01-01",
        )

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        # check silver products for original approval date
        silver_prods = list(source.resources["fda_drugs_silver_products"])
        assert len(silver_prods) == 1
        row = silver_prods[0]

        # Should match the ORIG date, ignoring SUPPL (even if SUPPL is earlier/later)
        assert row.original_approval_date == date(2000, 1, 1)


def test_exclusivity_aggregation_and_protection_status() -> None:
    """
    Verifies that Exclusivity.txt is ingested, dates are aggregated (Max),
    and is_protected is derived correctly based on today's date.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Product 001: Protected (Max date in future)
        # Product 002: Not Protected (Max date in past)
        # Product 003: No Exclusivity info
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n"
            "000001\t001\tTab\t10mg\tDrugA\n"
            "000002\t001\tTab\t10mg\tDrugB\n"
            "000003\t001\tTab\t10mg\tDrugC",
        )
        # Submissions (required for Silver/Gold base)
        z.writestr(
            "Submissions.txt",
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n"
            "000001\tORIG\t2000-01-01\n"
            "000002\tORIG\t2000-01-01\n"
            "000003\tORIG\t2000-01-01",
        )
        # Exclusivity:
        # 000001: Has one past date, one future date (Max should be future)
        # 000002: Has only past dates
        z.writestr(
            "Exclusivity.txt",
            "ApplNo\tProductNo\tExclusivityDate\n"
            "000001\t001\t2000-01-01\n"
            "000001\t001\t3000-01-01\n"
            "000002\t001\t2000-01-01\n"
            "000002\t001\t2010-01-01",
        )

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["fda_drugs_gold_drug_product"])
        assert len(gold_prods) == 3

        # Row 1: Protected
        row1 = next(p for p in gold_prods if p.appl_no == "000001")
        assert row1.is_protected is True

        # Row 2: Not Protected
        row2 = next(p for p in gold_prods if p.appl_no == "000002")
        assert row2.is_protected is False

        # Row 3: No Exclusivity -> Not Protected
        row3 = next(p for p in gold_prods if p.appl_no == "000003")
        assert row3.is_protected is False
