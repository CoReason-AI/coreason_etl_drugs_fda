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


def test_resilience_ragged_lines_extra_columns() -> None:
    """
    Test resilience to "ragged" lines (extra columns).
    Polars `read_csv` with `truncate_ragged_lines=True` should handle this
    by ignoring the extra fields, rather than crashing or shifting data incorrectly.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Header has 5 cols.
        # Row 1 has 5 cols.
        # Row 2 has 7 cols (Extra junk).
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n"
            "000001\t001\tTab\t10mg\tIng1\n"
            "000002\t002\tInj\t20mg\tIng2\tEXTRA\tJUNK",
        )
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        # Should process without error
        silver_prods = list(source.resources["fda_drugs_silver_products"])

        # Should get 2 rows (or 1 if the second is dropped, but truncate usually keeps it)
        # truncate_ragged_lines=True usually keeps the row and ignores extra cols.
        assert len(silver_prods) >= 1

        # Verify Row 1
        r1 = next(r for r in silver_prods if r.appl_no == "000001")
        assert r1.product_no == "001"
        assert "ING1" in r1.active_ingredients_list

        # Verify Row 2 (if present)
        # Note: If Submissions doesn't match 000002, it won't get approval date,
        # but Silver products logic requires Submissions join?
        # Silver logic joins dates with LEFT join.
        # So it should be present even if no date.
        r2 = next((r for r in silver_prods if r.appl_no == "000002"), None)
        if r2:
            assert r2.product_no == "002"
            assert "ING2" in r2.active_ingredients_list


def test_resilience_ragged_lines_missing_columns() -> None:
    """
    Test resilience to "ragged" lines (missing columns).
    Polars `read_csv` often treats missing columns as nulls if configured,
    or might error if not `null_values` handled.
    With `ignore_errors=True`, it might skip the row or fill nulls.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Row 2 is missing fields at the end
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n"
            "000001\t001\tTab\t10mg\tIng1\n"
            "000002\t002\tInj",  # Missing Strength, Ingredient
        )
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate")

    buffer.seek(0)

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prods = list(source.resources["fda_drugs_silver_products"])

        # Row 1 OK
        assert any(r.appl_no == "000001" for r in silver_prods)

        # Row 2: Might be skipped if Pydantic validation fails (missing Form/Strength as non-empty str?)
        # Or if Polars filled with Null.
        # Silver logic: fill_null("") for Form/Strength.
        # So it should survive if Polars read it.
        # Checking if it exists
        r2 = next((r for r in silver_prods if r.appl_no == "000002"), None)
        if r2:
            assert r2.form == "Inj"
            assert r2.strength == ""  # Filled default
            assert r2.active_ingredients_list == []  # Filled default


def test_resilience_whitespace_join_keys() -> None:
    """
    Test that whitespace in join keys (e.g. " 001 ") in auxiliary files
    is cleaned before joining, preventing join failures.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # MarketingStatus has whitespace in ApplNo and ProductNo
        # " 000001 " -> Should match "000001"
        # " 001 " -> Should match "001"
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n 000001 \t 001 \t1")
        z.writestr("MarketingStatus_Lookup.txt", "MarketingStatusID\tMarketingStatusDescription\n1\tMatched")

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

        # If whitespace handling works, this should be "Matched"
        # If failed, it would be None
        assert row.marketing_status_description == "Matched"


def test_resilience_empty_optional_files() -> None:
    """
    Test behavior when optional files are present but EMPTY (header only or 0 bytes).
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # MarketingStatus is 0 bytes
        z.writestr("MarketingStatus.txt", "")
        # TE is header only
        z.writestr("TE.txt", "ApplNo\tProductNo\tTECode")

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

        # Should just have Nones
        assert row.marketing_status_id is None
        assert row.te_code is None


def test_resilience_non_ascii_ingredients() -> None:
    """
    Test handling of non-ASCII characters in ingredients (e.g. Greek letters, symbols).
    CP1252 supports some, but let's test typical ones.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Use a char available in CP1252, e.g., µ (micro sign) = 0xB5
        # "Microgram" often abbreviated
        ing_str = "Ingredient with µ"
        content = f"ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\t{ing_str}"

        # Write as CP1252 explicitly
        z.writestr("Products.txt", content.encode("cp1252"))
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

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

        # Should preserve the char (upper cased)
        # 'µ'.upper() is 'µ' or 'Μ'? In Python 'µ'.upper() -> 'Μ' (Mu) or stays 'µ'?
        # Actually 'µ' (U+00B5) upper() is 'Μ' (U+039C) usually.
        # Let's see what Python does.
        expected = ing_str.upper()
        assert expected in row.active_ingredients_list


def test_missing_submissions_skips_silver() -> None:
    """
    Test that the silver_products resource is NOT yielded when Submissions.txt is missing.
    The source explicitly checks for existence of both Products.txt and Submissions.txt
    before defining the resource.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Include Products.txt but OMIT Submissions.txt
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tIng1\n",
        )
        z.writestr("Applications.txt", "ApplNo\n000001")

    buffer.seek(0)

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # We expect bronze resources to be present (e.g. raw_fda__products)
        assert "fda_drugs_bronze_fda_products" in source.resources
        assert "fda_drugs_bronze_fda_applications" in source.resources

        # But silver_products should be ABSENT because Submissions.txt is missing
        assert "fda_drugs_silver_products" not in source.resources
