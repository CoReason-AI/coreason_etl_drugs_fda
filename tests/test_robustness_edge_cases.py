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


def test_robustness_duplicate_lookups_no_explosion() -> None:
    """
    Verify that duplicate entries in MarketingStatus_Lookup.txt do not cause
    row multiplication (fan-out) in the Gold layer.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # 1 Product
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tIng1")
        # 1 Submission
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
        # 1 MarketingStatus (Link to ID 1)
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1")
        # Duplicate Lookup for ID 1
        # If not deduplicated, joining ID 1 would produce 2 rows for the single product
        z.writestr(
            "MarketingStatus_Lookup.txt",
            "MarketingStatusID\tMarketingStatusDescription\n1\tDescription A\n1\tDescription B",
        )

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])

        # Should strictly be 1 row
        assert len(gold_prods) == 1
        row = gold_prods[0]
        # It should pick one of the descriptions (indeterminately if not sorted, but Polars unique takes one)
        # We just care that it IS one of them and not 2 rows.
        assert row.marketing_status_description in ["Description A", "Description B"]


def test_robustness_earliest_orig_date_selection() -> None:
    """
    Verify that when multiple 'ORIG' submissions exist for an ApplNo,
    the earliest date is deterministically selected.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # 1 Product
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tIng1")
        # 3 ORIG Submissions for same ApplNo, mixed order
        # 2022 (Later)
        # 2020 (Earliest)
        # 2021 (Middle)
        submissions = (
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n"
            "000001\tORIG\t2022-01-01\n"
            "000001\tORIG\t2020-01-01\n"
            "000001\tORIG\t2021-01-01"
        )
        z.writestr("Submissions.txt", submissions)

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["silver_products"])

        assert len(silver_prods) == 1
        # Must be 2020-01-01
        assert silver_prods[0].original_approval_date == date(2020, 1, 1)


def test_robustness_id_padding_mismatch() -> None:
    """
    Verify that an unpadded `ApplNo` (e.g., "4") in `Products.txt` correctly matches
    a padded `ApplNo` (e.g., "000004") in auxiliary files (Applications/Submissions).
    This ensures join keys are normalized before joining.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products: ApplNo "4" (unpadded, maybe int inferred)
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n4\t1\tT\tS\tI")
        # Submissions: ApplNo "000004" (padded)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000004\tORIG\t2020-01-01")
        # Applications: ApplNo "000004" (padded)
        z.writestr("Applications.txt", "ApplNo\tSponsorName\n000004\tSponsorX")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # Check Silver (Product + Submission join)
        silver_prods = list(source.resources["silver_products"])
        assert len(silver_prods) == 1
        s_row = silver_prods[0]
        # Should have joined date successfully
        assert s_row.appl_no == "000004"  # Normalized
        assert s_row.product_no == "001"  # Normalized
        assert s_row.original_approval_date == date(2020, 1, 1)

        # Check Gold (Product + Application join)
        gold_prods = list(source.resources["dim_drug_product"])
        assert len(gold_prods) == 1
        g_row = gold_prods[0]
        # Should have joined sponsor successfully
        assert g_row.sponsor_name == "SponsorX"
