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


def test_massive_string_resilience() -> None:
    """
    Test resilience against massive string inputs (e.g., 50k characters).
    Ensures that buffer limits or strict parsing doesn't crash.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Create a massive string (50k 'A's)
        massive_ingredient = "A" * 50000
        products = f"ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\t{massive_ingredient}"
        z.writestr("Products.txt", products)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["FDA@DRUGS_silver_products"])

        assert len(silver_prods) == 1
        row = silver_prods[0]

        # Check that the massive string was read correctly (length check)
        # Note: clean_ingredients splits by ';', so we expect one element
        assert len(row.active_ingredients_list) == 1
        assert len(row.active_ingredients_list[0]) == 50000


def test_loose_quoting_handling() -> None:
    """
    Test that fields containing quotes (double or single) are read literally
    and do NOT cause row parsing errors, verifying `quote_char=None`.
    Input: "Drug \"Name\"" -> Should be read as "Drug \"Name\""
    If quote_char was '"', this might be parsed as "Drug Name" or error.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products has fields with quotes
        # ApplNo 000001
        # Form: 'Tablet "Fast"'
        # Strength: "10'mg"
        products = 'ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTablet "Fast"\t10\'mg\tIng'
        z.writestr("Products.txt", products)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["FDA@DRUGS_silver_products"])

        assert len(silver_prods) == 1
        row = silver_prods[0]

        # Verify quotes are preserved literally
        # Form is title-cased: 'Tablet "Fast"' -> 'Tablet "Fast"'
        assert row.form == 'Tablet "Fast"'
        assert row.strength == "10'mg"


def test_malformed_exclusivity_dates() -> None:
    """
    Test that invalid dates in Exclusivity.txt do not crash the pipeline.
    They should be parsed as Null/None and effectively ignored for protection calculation.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        products = "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng"
        z.writestr("Products.txt", products)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # Exclusivity with garbage date
        z.writestr("Exclusivity.txt", "ApplNo\tProductNo\tExclusivityDate\n000001\t001\tNOT-A-DATE")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["FDA@DRUGS_gold_drug_product"])

        assert len(gold_prods) == 1
        row = gold_prods[0]

        # is_protected logic: Max(ExclusivityDate) > Today
        # If date is invalid -> Null. Max(Null) -> Null.
        # Null > Today -> False (or error?)
        # Logic in source.py:
        # df_exclusivity = fix_dates(...)
        # group_by... max()
        # when(col > today).then(True).otherwise(False)
        # Polars: Null > Date is usually Null (False-like in when/then/otherwise if not explicitly handled?)
        # Actually in Polars: (Null > Val) is Null.
        # when(Null).then(True).otherwise(False) -> False.
        # So it should default to False (Not Protected).

        assert row.is_protected is False
