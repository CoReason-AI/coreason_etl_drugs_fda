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


def test_marketing_status_lookup_fanout() -> None:
    """
    Test that duplicate keys in MarketingStatus_Lookup.txt do not cause row duplication (fan-out)
    in the final Gold table. The logic should deduplicate the lookup table before joining.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # 1 Product
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
        # Links to Status ID 1
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1")

        # Lookup has DUPLICATE entry for ID 1
        # Row 1: Prescription
        # Row 2: Over-the-counter (Conflict)
        # The pipeline should pick ONE (likely the first or arbitrary) but NOT produce 2 rows.
        z.writestr(
            "MarketingStatus_Lookup.txt",
            "MarketingStatusID\tMarketingStatusDescription\n1\tPrescription\n1\tDuplicateEntry",
        )

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["fda_drugs_gold_products"])

        # MUST still be exactly 1 row
        assert len(gold_prods) == 1

        row = gold_prods[0]
        # Verify it successfully joined one of them
        assert row["marketing_status_description"] in ["Prescription", "DuplicateEntry"]


def test_marketing_status_lookup_dirty_ids() -> None:
    """
    Test that malformed (non-integer) IDs in the Lookup file are handled gracefully.
    The pipeline casts to Int64 with strict=False, so they should become null and not match.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
        # Links to Status ID 1
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1")

        # Lookup has dirty ID "ABC" and "1.0"
        # "1" (valid) -> Matches
        # "ABC" (invalid) -> Null -> Ignored
        z.writestr(
            "MarketingStatus_Lookup.txt",
            "MarketingStatusID\tMarketingStatusDescription\n1\tValid\nABC\tInvalid\n1.0\tFloat",
        )

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["fda_drugs_gold_products"])

        assert len(gold_prods) == 1
        row = gold_prods[0]

        # Should match the valid "1"
        assert row["marketing_status_description"] == "Valid"


def test_complex_integration() -> None:
    """
    Comprehensive integration test verifying a fully populated Gold Record
    derived from ALL source files with some data nuances.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Product: ANDA (Generic), padded needs, multi-ingredient
        z.writestr(
            "Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n70001\t1\tTABLET\t10MG\tIngA; IngB"
        )

        # Submissions: Multiple ORIG, random order
        z.writestr(
            "Submissions.txt",
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n070001\tORIG\t2015-06-01\n070001\tORIG\t2010-01-01",
        )

        # Applications: Sponsor Info, Type A (ANDA)
        z.writestr("Applications.txt", "ApplNo\tSponsorName\tApplType\n070001\tGenericCorp\tA")

        # Marketing Status: ID 2 (OTC)
        z.writestr("MarketingStatus.txt", "ApplNo\tProductNo\tMarketingStatusID\n070001\t001\t2")

        # Lookup: ID 2 -> OTC
        z.writestr("MarketingStatus_Lookup.txt", "MarketingStatusID\tMarketingStatusDescription\n2\tOver-the-Counter")

        # TE: Code AB
        z.writestr("TE.txt", "ApplNo\tProductNo\tTECode\n070001\t001\tAB")

        # Exclusivity: Expired
        z.writestr(
            "Exclusivity.txt", "ApplNo\tProductNo\tExclusivityCode\tExclusivityDate\n070001\t001\tGEN\t2000-01-01"
        )

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["fda_drugs_gold_products"])

        assert len(gold_prods) == 1
        row = gold_prods[0]

        # Verify IDs (Padded)
        assert row["appl_no"] == "070001"
        assert row["product_no"] == "001"

        # Verify Ingredients (Split & Cleaned)
        assert row["active_ingredients_list"] == ["INGA", "INGB"]

        # Verify Date (Earliest ORIG)
        assert row["original_approval_date"] == date(2010, 1, 1)

        # Verify Sponsor & Type
        assert row["sponsor_name"] == "GenericCorp"
        assert row["is_generic"] is True

        # Verify Marketing Status Enriched
        assert row["marketing_status_id"] == 2
        assert row["marketing_status_description"] == "Over-the-Counter"

        # Verify TE
        assert row["te_code"] == "AB"

        # Verify Protection (Expired)
        assert row["is_protected"] is False


def test_submission_date_sorting_legacy_vs_iso() -> None:
    """
    Complex Case: Verify correct sorting when Submissions contains both ISO dates
    and the legacy "Approved prior to..." string.
    Expected: "Approved prior to Jan 1, 1982" (1982-01-01) is strictly older than "1985-01-01".
    If sorting is purely lexical on string, "1..." < "A...", so 1985 wins (INCORRECT).
    If sorting is chronological, 1982 wins (CORRECT).
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tActiveIngredient\tForm\tStrength\n000001\t001\tIng\tF\tS")

        # Two submissions for same ApplNo:
        # 1. 1985-01-01
        # 2. Approved prior to Jan 1, 1982
        # We want the earliest.
        content = (
            "ApplNo\tSubmissionType\tSubmissionStatusDate\n"
            "000001\tORIG\t1985-01-01\n"
            "000001\tORIG\tApproved prior to Jan 1, 1982"
        )
        z.writestr("Submissions.txt", content)

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["fda_drugs_gold_products"])

        assert len(gold_prods) == 1
        row = gold_prods[0]

        # Should be 1982-01-01
        assert row["original_approval_date"] == date(1982, 1, 1)


def test_te_code_fanout_prevention() -> None:
    """
    Complex Case: Verify that duplicate TE codes for the same Product do not cause row explosion.
    The pipeline should pick one unique TE code or deduplicate.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tActiveIngredient\tForm\tStrength\n000001\t001\tIng\tF\tS")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

        # TE File has duplicate rows or multiple codes
        # If it has different codes, the current logic picks one (arbitrary due to unique keep='first'?).
        # If it has same code, it should definitely not fan out.
        z.writestr("TE.txt", "ApplNo\tProductNo\tTECode\n000001\t001\tAB\n000001\t001\tXY")

    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["fda_drugs_gold_products"])

        # Should NOT fan out to 2 rows
        assert len(gold_prods) == 1
