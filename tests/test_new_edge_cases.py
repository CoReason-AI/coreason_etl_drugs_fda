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

import polars as pl
import pytest
from pydantic import ValidationError

from coreason_etl_drugs_fda.silver import ProductSilver
from coreason_etl_drugs_fda.source import drugs_fda_source
from coreason_etl_drugs_fda.transform import clean_ingredients


def test_search_vector_full_complexity() -> None:
    """
    Test search_vector generation with:
    - Unicode characters in DrugName and Sponsor.
    - Multiple ingredients.
    - Missing TE code (null).
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # DrugName: "Trâdemark®"
        # ActiveIngredient: "IngA; IngB"
        # Must encode as CP1252 because source reads as CP1252
        products = (
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\tDrugName\n000001\t001\tF\tS\tIngA; IngB\tTrâdemark®"
        )
        z.writestr("Products.txt", products.encode("cp1252"))

        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # Sponsor: "Spönsör"
        apps = "ApplNo\tSponsorName\tApplType\n000001\tSpönsör\tN"
        z.writestr("Applications.txt", apps.encode("cp1252"))
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

        # Expected: "TRÂDEMARK® INGA INGB SPÖNSÖR" (Uppercased)
        # Note: upper() on special chars depends on locale/python version but usually works for standard unicode.
        # "Trâdemark®".upper() -> "TRÂDEMARK®"
        # "Spönsör".upper() -> "SPÖNSÖR"
        target = "TRÂDEMARK® INGA INGB SPÖNSÖR"
        assert row["search_vector"] == target


def test_exclusivity_boundary_today() -> None:
    """
    Test Exclusivity Logic Boundary:
    BRD: True if current_date < Max(ExclusivityDate)
    If Max(ExclusivityDate) == today, then False (Expired today).
    """
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng",
        )
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
        # Exclusivity expires TODAY
        z.writestr("Exclusivity.txt", f"ApplNo\tProductNo\tExclusivityDate\n000001\t001\t{today_str}")

    buffer.seek(0)
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["FDA@DRUGS_gold_drug_product"])
        row = gold_prods[0]

        # Should be NOT protected because date < max_date is False (date == max_date)
        # Logic: today < max_date
        assert row["is_protected"] is False


def test_active_ingredients_formatting_edge_cases() -> None:
    """
    Test cleaning of ingredients with:
    1. Empty string -> [""]? Or []?
       If "ActiveIngredient" is empty string "", split(";") -> [""]
    2. Only semi-colons -> ";;" -> ["", "", ""] -> cleaned to ["", "", ""]?
       transform.py: .list.eval(pl.element().str.strip_chars())
       Empty string stripped is empty string.
    """
    df = pl.DataFrame({"active_ingredient": ["", ";;", " ; "]})
    res = clean_ingredients(df)

    # Row 1: "" -> [""] (Polars split behavior on empty string usually returns [""] or [])
    # pl.lit("").str.split(";") -> [""]
    assert res["active_ingredients_list"][0].to_list() == []

    # Row 2: ";;" -> ["", "", ""]
    assert res["active_ingredients_list"][1].to_list() == []

    # Row 3: " ; " -> split -> [" ", " "] -> strip -> ["", ""]
    assert res["active_ingredients_list"][2].to_list() == []


def test_source_id_validation() -> None:
    """
    Test source_id validation in Pydantic model.
    Must be exactly 9 digits.
    """
    import uuid

    uid = uuid.uuid4()

    # Valid
    ProductSilver(
        coreason_id=uid,
        source_id="123456789",
        appl_no="123456",
        product_no="789",
        form="F",
        strength="S",
        active_ingredients_list=[],
        original_approval_date=None,
        hash_md5="hash",
    )  # Should pass if types match (coreason_id needs UUID)

    # Invalid Length (8 digits)
    with pytest.raises(ValidationError):
        ProductSilver(
            coreason_id=uid,
            source_id="12345678",
            appl_no="123456",
            product_no="001",
            form="F",
            strength="S",
            active_ingredients_list=[],
            original_approval_date=None,
            hash_md5="hash",
        )

    # Invalid Characters
    with pytest.raises(ValidationError):
        ProductSilver(
            coreason_id=uid,
            source_id="12345678A",
            appl_no="123456",
            product_no="001",
            form="F",
            strength="S",
            active_ingredients_list=[],
            original_approval_date=None,
            hash_md5="hash",
        )


def test_duplicate_orig_submissions_selection() -> None:
    """
    Test that when multiple 'ORIG' submissions exist for a single ApplNo,
    the pipeline selects the EARLIEST date.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr(
            "Products.txt",
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tTab\t10mg\tIng1\n",
        )
        # Three ORIG submissions:
        # 1. 2022-01-01 (Latest)
        # 2. 2020-01-01 (Earliest) -> Target
        # 3. 2021-01-01 (Middle)
        # Order in file shouldn't matter if we sort correctly.
        z.writestr(
            "Submissions.txt",
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n"
            "000001\tORIG\t2022-01-01\n"
            "000001\tORIG\t2020-01-01\n"
            "000001\tORIG\t2021-01-01\n",
        )

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

        # Should be 2020-01-01
        assert row["original_approval_date"] == date(2020, 1, 1)
