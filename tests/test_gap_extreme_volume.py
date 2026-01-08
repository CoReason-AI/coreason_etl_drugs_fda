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


def test_massive_fanout_search_vector_resilience() -> None:
    """
    Test resilience when a product has an extreme number of TE codes (e.g., 5,000).
    The pipeline logic for `search_vector` concatenates these.
    We want to ensure:
    1. It doesn't crash.
    2. It handles the concatenation (might result in truncated or huge string, but no crash).
    3. Performance is reasonable (tested via implicit timeout).
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # 1 Product
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # 5,000 TE Codes for the same product
        # Current logic: Left Join TE on [ApplNo, ProductNo].
        # Wait, Gold logic implementation of TE Join:
        # df_te_sub = df_te.select(["appl_no", "product_no", "te_code"]).unique(subset=["appl_no", "product_no"])
        # It takes UNIQUE subset. So it picks ONE TE code per product.
        # This prevents fanout.
        # So 5,000 rows in TE.txt for same product -> collapse to 1 row.
        # This test verifies that the collapse is efficient and works.

        te_rows = ["ApplNo\tProductNo\tTECode"]
        for i in range(5000):
            te_rows.append(f"000001\t001\tTE{i}")

        z.writestr("TE.txt", "\n".join(te_rows))

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

        # Verify it picked one TE code (deterministically the first or one of them)
        # Polars unique might pick any, usually first if stable.
        # With 5000 input rows, we expect 1 output row.
        assert row.te_code is not None
        assert row.te_code.startswith("TE")

        # Search vector should contain that one TE code, not 5000 of them.
        assert len(row.search_vector) < 1000  # Should be small if deduplicated


def test_massive_active_ingredients_list() -> None:
    """
    Test a product with 1,000 distinct active ingredients in the `ActiveIngredient` string.
    Logic splits by ';'.
    This creates a list of 1,000 strings.
    Verify `search_vector` (which joins them) handles this massive string generation.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Create 1000 ingredients joined by ;
        ingredients = ";".join([f"Ing{i}" for i in range(1000)])

        z.writestr(
            "Products.txt",
            f"ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\t{ingredients}",
        )
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["fda_drugs_silver_products"])
        gold_prods = list(source.resources["fda_drugs_gold_drug_product"])

        assert len(silver_prods) == 1
        row = silver_prods[0]

        # Verify list length
        assert len(row.active_ingredients_list) == 1000
        assert row.active_ingredients_list[0] == "ING0"

        # Verify Gold Search Vector
        # It joins them with " ".
        # Length approx 1000 * 4 chars + spaces ~ 5000 chars.
        gold_row = gold_prods[0]
        assert len(gold_row.search_vector) > 4000
        assert "ING999" in gold_row.search_vector
