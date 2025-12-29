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

def test_missing_submissions_file() -> None:
    """
    Edge Case: Submissions.txt is missing from the ZIP.
    Expectation: The 'silver_products' resource should NOT be yielded because
    it strictly depends on Submissions for approval dates.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Include Form/Strength to satisfy Pydantic
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\n001\t001\tF\tS")
    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        # Check available resources
        resource_names = list(source.resources.keys())

        # 'silver_products' should be missing
        assert "silver_products" not in resource_names
        # 'raw_fda__products' should be present
        assert "raw_fda__products" in resource_names
        # 'dim_drug_product' (Gold) depends on Products present.
        # Logic says: if "Products.txt" in files_present: yield Gold.
        # But Gold calls _create_silver_dataframe which calls _extract_approval_dates.
        # If Submissions missing, _extract_approval_dates returns {}.
        # _create_silver_dataframe handles missing Submissions?
        # Let's check source.py logic:
        # if "Products.txt" in files_present and "Submissions.txt" in files_present: -> yield Silver
        # if "Products.txt" in files_present: -> yield Gold
        # So Gold IS yielded.
        assert "dim_drug_product" in resource_names

        # Verify Gold content - should have null approval dates
        gold_res = source.resources["dim_drug_product"]
        rows = list(gold_res)
        assert len(rows) == 1
        assert rows[0].original_approval_date is None


def test_empty_string_ingredients() -> None:
    """
    Complex Case: ActiveIngredient is an empty string "".
    Expectation: clean_ingredients splits "" -> [""] (list containing empty string),
    or maybe [""] is filtered?
    The current logic: .str.split(";") -> [""]
    .list.eval(pl.element().str.strip_chars()) -> [""]
    So result is [""]
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tActiveIngredient\tForm\tStrength\n001\t001\t\tF\tS")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n001\tORIG\t2020-01-01")
    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_res = source.resources["silver_products"]
        rows = list(silver_res)

        # Verify ingredients list
        # Polars split on empty string returns [""] (a list with one empty string)
        # unless missing_utf8_is_empty_string logic interferes, but TSV likely parses as "" or None.
        # If TSV has \t\t, it's None or "" depending on parser.
        # If it's "", result is [""]?
        # Let's see what happens.
        assert rows[0].active_ingredients_list == [""] or rows[0].active_ingredients_list == []

        # If it is None, code fills with [].
        # We wrote \t\t so it's likely None or "".


def test_malformed_legacy_date() -> None:
    """
    Edge Case: Date string is close to legacy format but differs in case or punctuation.
    "approved prior to Jan 1, 1982" (lowercase 'a')
    Expectation: Not detected as legacy, fails parse, becomes None.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Include Form/Strength
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\n001\t001\tF\tS")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n001\tORIG\tapproved prior to Jan 1, 1982")
    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_res = source.resources["silver_products"]
        rows = list(silver_res)

        # Check date is None (failed parse) and NOT 1982-01-01
        assert rows[0].original_approval_date is None
        assert rows[0].is_historic_record is False


def test_minimal_gold_record_search_vector() -> None:
    """
    Complex Case: Product has NO aux data (No Sponsor, No TE, No Marketing, No Ingredients).
    Expectation: Search Vector is built safely without crashing, likely just IDs or empty string components.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Include Form/Strength
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\n001\t001\tF\tS")
        # Submissions needed for Silver to trigger?
        # source.py: if Products + Submissions -> Silver.
        # But Gold trigger is just Products.
        # But Gold calls _create_silver_dataframe which calls extract_approval_dates which needs Submissions.
        # If Submissions missing, extract returns empty dict.
        # So we don't need Submissions for Gold to technically run, per test_missing_submissions_file.
    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_res = source.resources["dim_drug_product"]
        rows = list(gold_res)
        row = rows[0]

        # Search vector should be empty string (stripped) or just spaces stripped
        # Logic: DrugName("") + Ingredients("") + Sponsor("") + TE("")
        # Result: ""
        assert row.search_vector == ""
        assert row.is_generic is False
        assert row.is_protected is False


def test_duplicate_products_logic() -> None:
    """
    Edge Case: Products.txt contains duplicate rows for the same ApplNo/ProductNo.
    Expectation:
    - Silver layer generates same coreason_id.
    - If yielded to dlt with write_disposition='merge', dlt handles it.
    - But here we yield 2 rows from the resource iterator.
    - This test just verifies we get 2 rows out of the generator.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        content = (
            "ApplNo\tProductNo\tForm\tStrength\n"
            "001\t001\tF\tS\n"
            "001\t001\tF\tS"
        )
        z.writestr("Products.txt", content)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n001\tORIG\t2020-01-01")
    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_res = source.resources["silver_products"]
        rows = list(silver_res)

        # Should get 2 rows (Silver doesn't deduplicate Products explicitly, relies on source)
        assert len(rows) == 2
        # They should have identical coreason_id
        assert rows[0].coreason_id == rows[1].coreason_id
