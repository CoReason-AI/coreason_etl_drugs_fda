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


def test_gold_products_marketing_status_lookup() -> None:
    """
    Test Gold layer joins MarketingStatus_Lookup to get marketing_status_description.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")

        # Submissions (required for Silver base)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # MarketingStatus (Links ApplNo+ProductNo to MarketingStatusID)
        # ID 1 -> Prescription
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1")

        # MarketingStatus_Lookup (Links MarketingStatusID to Description)
        # ID 1 -> Prescription
        z.writestr("MarketingStatus_Lookup.txt", "MarketingStatusID\tMarketingStatusDescription\n1\tPrescription")

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["FDA@DRUGS_gold_drug_product"])
        assert len(gold_prods) == 1
        row = gold_prods[0]

        # Verify ID was joined
        assert row.marketing_status_id == 1

        # Verify Description was enriched (This should FAIL before implementation)
        assert row.marketing_status_description == "Prescription"
