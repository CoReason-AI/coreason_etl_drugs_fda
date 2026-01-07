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


def test_gold_search_vector_edge_cases() -> None:
    """Test search_vector generation when columns are missing in source."""
    # Case 1: Missing 'drug_name' in Products.txt (Common, as it might be named differently or missing)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products without DrugName
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIngA",
        )
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["FDA@DRUGS_gold_drug_product"])
        row = gold_prods[0]
        # Should be just "INGA"
        assert row.search_vector == "INGA"

    # Case 2: Missing 'active_ingredient' (Should normally not happen but good for robustness)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tDrugName\n000001\t001\tF\tS\tMyDrug",
        )
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
    buffer.seek(0)
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["FDA@DRUGS_gold_drug_product"])
        row = gold_prods[0]
        # Should be "MYDRUG" (uppercased)
        assert row.search_vector == "MYDRUG"
        assert row.active_ingredients_list == []


def test_gold_search_vector_missing_sponsor_te() -> None:
    """Test search vector logic when SponsorName and TECode columns are missing from joins."""
    # Applications.txt WITHOUT SponsorName
    # TE.txt WITHOUT TECode (or missing TE file)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\tDrugName\n000001\t001\tF\tS\tIngA\tMyDrug",
        )
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
        # Applications exists but missing SponsorName column?
        z.writestr("Applications.txt", "ApplNo\tOtherCol\n000001\tVal")
        # TE missing

    buffer.seek(0)
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["FDA@DRUGS_gold_drug_product"])
        row = gold_prods[0]

        # Search vector: MyDrug + IngA + "" + "" -> "MYDRUG INGA"
        assert row.search_vector == "MYDRUG INGA"
