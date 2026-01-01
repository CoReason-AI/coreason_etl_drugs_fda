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


def test_lookup_determinism() -> None:
    """
    Complex Case: MarketingStatus_Lookup contains duplicate IDs with different descriptions.
    The pipeline must be deterministic (e.g., picking the lexicographically first description)
    regardless of input order.
    """
    # Case A: "Alpha" comes before "Beta" in file
    content_a = "MarketingStatusID\tMarketingStatusDescription\n1\tAlpha\n1\tBeta\n"

    # Case B: "Beta" comes before "Alpha" in file
    content_b = "MarketingStatusID\tMarketingStatusDescription\n1\tBeta\n1\tAlpha\n"

    base_zip_files = {
        "Products.txt": "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng",
        "Submissions.txt": "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01",
        "MarketingStatus.txt": "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1",
    }

    def run_with_lookup_content(lookup_content: str) -> str:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as z:
            for fname, content in base_zip_files.items():
                z.writestr(fname, content)
            z.writestr("MarketingStatus_Lookup.txt", lookup_content)

        buffer.seek(0)

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.content = buffer.getvalue()
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            source = drugs_fda_source()
            # We specifically want Gold product
            gold_prods = list(source.resources["dim_drug_product"])
            return str(gold_prods[0].marketing_status_description)

    result_a = run_with_lookup_content(content_a)
    result_b = run_with_lookup_content(content_b)

    # Without sorting, Polars 'unique' (keep='first') would typically return:
    # result_a -> "Alpha" (first in file A)
    # result_b -> "Beta" (first in file B)
    # But we WANT determinism: result_a == result_b

    # We assert that they are equal. If code is not sorting, this might fail (or pass accidentally if Polars optimizes).
    # But strictly, we want "Alpha" (lexicographically first) if we implement sort.
    assert result_a == "Alpha"
    assert result_b == "Alpha"


def test_marketing_status_determinism() -> None:
    """
    Complex Case: MarketingStatus contains multiple statuses for the same product.
    We should deterministically pick one (e.g., sorted by ID).
    """
    # Case A: ID 1 before ID 2
    content_a = "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1\n000001\t001\t2"
    # Case B: ID 2 before ID 1
    content_b = "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t2\n000001\t001\t1"

    base_zip_files = {
        "Products.txt": "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng",
        "Submissions.txt": "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01",
        "MarketingStatus_Lookup.txt": "MarketingStatusID\tMarketingStatusDescription\n1\tOne\n2\tTwo",
    }

    def run_with_marketing_content(mkt_content: str) -> int:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as z:
            for fname, content in base_zip_files.items():
                z.writestr(fname, content)
            z.writestr("MarketingStatus.txt", mkt_content)

        buffer.seek(0)

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.content = buffer.getvalue()
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            source = drugs_fda_source()
            gold_prods = list(source.resources["dim_drug_product"])
            val = gold_prods[0].marketing_status_id
            assert val is not None
            return int(val)

    result_a = run_with_marketing_content(content_a)
    result_b = run_with_marketing_content(content_b)

    # We want ID 1 (smaller) to be picked if we sort by ID.
    assert result_a == 1
    assert result_b == 1
