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

import pytest
from coreason_etl_drugs_fda.source import (
    _create_silver_dataframe,
    _extract_approval_dates,
    _read_file_from_zip,
    drugs_fda_source,
)
from dlt.extract.exceptions import ResourceExtractionError
from pydantic import ValidationError


@pytest.fixture  # type: ignore[misc]
def mock_zip_content() -> bytes:
    """Creates a mock ZIP file in memory containing sample TSV files."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Create Products.txt
        # ApplNo 000004 has match in Submissions.
        # ApplNo 000005 has NO match.
        products_content = (
            "ApplNo\tProductNo\tForm\tStrength\tReferenceDrug\tDrugName\tActiveIngredient\tReferenceStandard\n"
            "000004\t004\tSOLUTION/DROPS;OPHTHALMIC\t1%\t0\tPAREDRINE\tHYDROXYAMPHETAMINE HYDROBROMIDE\t0\n"
            "   000005   \t005\tTABLET   \t5MG   \t0\tTESTDRUG\tTESTINGREDIENT\t0"
        )
        z.writestr("Products.txt", products_content)

        # Create Submissions.txt
        # 000004: ORIG, AP, 1982-01-01
        submissions_content = (
            "ApplNo\tSubmissionClassCodeID\tSubmissionType\tSubmissionNo\tSubmissionStatus\tSubmissionStatusDate\tReviewPriorityID\n"
            "000004\t7\tORIG\t1\tAP\t1982-01-01\t0\n"
            "000006\t7\tSUPPL\t2\tAP\t2023-01-01\t0"
        )
        z.writestr("Submissions.txt", submissions_content)

    buffer.seek(0)
    return buffer.getvalue()


def test_drugs_fda_source_extraction(mock_zip_content: bytes) -> None:
    """
    Test that the source correctly extracts, parses, and cleans data from the ZIP.
    Also verifies the 'silver_products' resource.
    """
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_zip_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Initialize the source
        source = drugs_fda_source()

        # Check resources
        resources = source.resources
        assert "raw_fda__products" in resources
        assert "raw_fda__submissions" in resources
        assert "silver_products" in resources

        # 1. Verify Raw Products
        raw_prod = list(resources["raw_fda__products"])
        assert len(raw_prod) == 2
        assert raw_prod[0]["appl_no"] == "000004"
        # Raw layer keeps original name (snake_cased) but not transformed yet?
        # Transform logic renames it. Raw layer is direct from read.
        # Products.txt has "ActiveIngredient", clean_dataframe makes it "active_ingredient"
        assert raw_prod[0]["active_ingredient"] == "HYDROXYAMPHETAMINE HYDROBROMIDE"

        # 2. Verify Silver Products
        silver_prod = list(resources["silver_products"])
        assert len(silver_prod) == 2

        row1 = silver_prod[0]
        # Check Padded IDs
        assert row1.appl_no == "000004"
        assert row1.product_no == "004"
        # Check Date Join
        assert row1.original_approval_date == date(1982, 1, 1)
        # Check Active Ingredient List
        assert row1.active_ingredients_list == ["HYDROXYAMPHETAMINE HYDROBROMIDE"]
        # Check UUID
        assert row1.coreason_id is not None
        assert row1.source_id == "000004004"
        assert row1.hash_md5 is not None

        row2 = silver_prod[1]
        assert row2.appl_no == "000005"
        # Check No Date Join
        assert row2.original_approval_date is None


def test_silver_products_legacy_date(mock_zip_content: bytes) -> None:
    """Test legacy date string handling in silver_products."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        products = "ApplNo\tProductNo\tActiveIngredient\tForm\tStrength\n000007\t001\tIng\tF\tS"
        z.writestr("Products.txt", products)
        # Submissions with legacy string
        submissions = "ApplNo\tSubmissionType\tSubmissionStatusDate\n000007\tORIG\tApproved prior to Jan 1, 1982"
        z.writestr("Submissions.txt", submissions)
    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        silver_prod = list(source.resources["silver_products"])
        row = silver_prod[0]

        assert row.original_approval_date == date(1982, 1, 1)
        assert row.is_historic_record is True


def test_read_file_from_zip_missing() -> None:
    """Test _read_file_from_zip with non-existent file."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("exists.txt", "col\nval")

    # This generator should yield nothing
    gen = _read_file_from_zip(buffer.getvalue(), "missing.txt")
    assert list(gen) == []


def test_extract_approval_dates_missing_file() -> None:
    """Test _extract_approval_dates when Submissions.txt is missing."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "col\nval")

    dates = _extract_approval_dates(buffer.getvalue())
    assert dates == {}


def test_extract_approval_dates_missing_columns() -> None:
    """Test _extract_approval_dates with malformed Submissions.txt."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Missing SubmissionType or SubmissionStatusDate
        z.writestr("Submissions.txt", "ApplNo\tWrongCol\n123\tval")

    dates = _extract_approval_dates(buffer.getvalue())
    assert dates == {}


def test_silver_products_empty_dates() -> None:
    """Test silver_products_resource when no approval dates are found (empty dates_df)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        products = "ApplNo\tProductNo\tActiveIngredient\tForm\tStrength\n000008\t001\tIng\tF\tS"
        z.writestr("Products.txt", products)
        # Submissions has no ORIG
        submissions = "ApplNo\tSubmissionType\tSubmissionStatusDate\n000008\tSUPPL\t2023-01-01"
        z.writestr("Submissions.txt", submissions)
    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        # Should yield silver products, but with null dates
        silver_prod = list(source.resources["silver_products"])
        assert len(silver_prod) == 1
        assert silver_prod[0].original_approval_date is None


def test_silver_products_validation_error() -> None:
    """Test that invalid data raises a Pydantic ValidationError."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Invalid ApplNo (5 digits instead of 6) and missing required fields
        # Pydantic regex: ^\d{6}$
        # We'll provide a 5 digit one.
        # Also, transform logic pads it, so we need to provide one that FAILS after padding?
        # Logic: df.with_columns(pl.col("appl_no").cast(pl.String).str.pad_start(6, "0"))
        # So "123" becomes "000123" which is valid.
        # We need something that is NOT digits. "ABC".
        # But wait, transform logic doesn't check for digits before padding.
        # "ABC" -> "000ABC" (if length 3).
        # Regex ^\d{6}$ will fail on "000ABC".
        products = "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\nABC\t001\tForm\tStr\tIng"
        z.writestr("Products.txt", products)
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\nABC\tORIG\t2023-01-01")

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # dlt wraps exceptions in ResourceExtractionError
        with pytest.raises(ResourceExtractionError) as excinfo:
            list(source.resources["silver_products"])

        # Verify it was a ValidationError
        assert isinstance(excinfo.value.__cause__, ValidationError)


def test_gold_products_logic() -> None:
    """Test Gold layer joins and logic (is_generic, is_protected)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Products
        # 000001: NDA, Protected
        # 000002: ANDA, Not Protected
        products = (
            "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng1\n000002\t001\tF\tS\tIng2"
        )
        z.writestr("Products.txt", products)

        # Submissions (needed for Silver base)
        submissions = "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01\n000002\tORIG\t2020-01-01"
        z.writestr("Submissions.txt", submissions)

        # Applications (Sponsor, ApplType)
        # 000001 -> N (NDA), SponsorA
        # 000002 -> A (ANDA), SponsorB
        apps = "ApplNo\tSponsorName\tApplType\n000001\tSponsorA\tN\n000002\tSponsorB\tA"
        z.writestr("Applications.txt", apps)

        # Exclusivity
        # 000001 -> Future date
        # 000002 -> Past date
        # We need to simulate dates relative to "today" used in code.
        # But we mocked requests, we didn't mock date.today().
        # So we use far future (3000) and far past (2000).
        excl = "ApplNo\tProductNo\tExclusivityDate\n000001\t001\t3000-01-01\n000002\t001\t2000-01-01"
        z.writestr("Exclusivity.txt", excl)

        # MarketingStatus (Just to check join)
        marketing = "ApplNo\tProductNo\tMarketingStatusID\n000001\t001\t1"
        z.writestr("MarketingStatus.txt", marketing)

        # TE (Just to check join)
        te = "ApplNo\tProductNo\tTECode\n000002\t001\tAB"
        z.writestr("TE.txt", te)

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])
        assert len(gold_prods) == 2

        # Row 1: NDA, Protected, Has Marketing
        row1 = next(p for p in gold_prods if p.appl_no == "000001")
        assert row1.sponsor_name == "SponsorA"
        assert row1.is_generic is False  # ApplType N
        assert row1.is_protected is True  # Excl Date 3000 > Today
        assert row1.marketing_status_id == 1
        assert row1.te_code is None  # Missing in TE
        # search_vector: DrugName + ActiveIngredient + SponsorName + TECode
        # Products.txt didn't provide DrugName, so ""
        # Ing1 + SponsorA + ""
        # Note: join puts spaces. "" + Ing1 + SponsorA + "" -> "Ing1 SponsorA" (stripped)
        # Note: ActiveIngredient is uppercased in transformation!
        # Search vector is also uppercased now
        assert "ING1" in row1.search_vector
        assert "SPONSORA" in row1.search_vector

        # Row 2: ANDA, Not Protected, Has TE
        row2 = next(p for p in gold_prods if p.appl_no == "000002")
        assert row2.sponsor_name == "SponsorB"
        assert row2.is_generic is True  # ApplType A
        assert row2.is_protected is False  # Excl Date 2000 < Today
        assert row2.te_code == "AB"
        assert row2.marketing_status_id is None  # Missing in Marketing
        # Ing2 + SponsorB + AB
        assert "ING2" in row2.search_vector
        assert "SPONSORB" in row2.search_vector
        assert "AB" in row2.search_vector


def test_gold_products_missing_aux_files() -> None:
    """Test Gold layer works (with nulls) even if auxiliary files are missing."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])
        assert len(gold_prods) == 1
        row = gold_prods[0]

        assert row.sponsor_name is None
        assert row.is_generic is False  # Default if missing
        assert row.is_protected is False  # Default if missing
        assert row.marketing_status_id is None
        # search_vector should handle missing cols
        # drug_name missing -> ""
        # active_ingredients -> "ING"
        # sponsor missing -> ""
        # te missing -> ""
        # So "ING"
        assert row.search_vector == "ING"


def test_gold_products_missing_appl_type_column() -> None:
    """Test Gold layer when Applications.txt exists but lacks ApplType column."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Products.txt", "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng")
        z.writestr("Submissions.txt", "ApplNo\tSubmissionType\tSubmissionStatusDate\n000001\tORIG\t2020-01-01")
        # Applications has SponsorName but NO ApplType
        z.writestr("Applications.txt", "ApplNo\tSponsorName\n000001\tSponsorX")

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        gold_prods = list(source.resources["dim_drug_product"])
        row = gold_prods[0]

        assert row.sponsor_name == "SponsorX"
        assert row.is_generic is False  # Default


def test_source_skips_silver_if_missing_files() -> None:
    """Test that silver_products and gold_products resources are skipped if files are missing."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Only Products, no Submissions -> Silver skipped (in current logic line 224 checks both)
        # Wait, Silver requires BOTH Products AND Submissions.
        # Gold requires Products.
        # If I provide ONLY Products, Silver should be skipped, Gold might be present?
        # Let's check logic:
        # if "Products.txt" in files_present and "Submissions.txt" in files_present: -> Silver
        # if "Products.txt" in files_present: -> Gold

        products = "ApplNo\tProductNo\tForm\tStrength\tActiveIngredient\n000001\t001\tF\tS\tIng"
        z.writestr("Products.txt", products)

    buffer.seek(0)
    mock_content = buffer.getvalue()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        resources = source.resources

        assert "raw_fda__products" in resources
        assert "silver_products" not in resources  # Should be skipped
        assert "dim_drug_product" in resources  # Should be present (only depends on Products)

    # Case 2: No Products -> Silver and Gold skipped
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Submissions.txt", "ApplNo\n1")
    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        resources = source.resources
        assert "silver_products" not in resources
        assert "dim_drug_product" not in resources


def test_create_silver_dataframe_missing_products() -> None:
    """Test _create_silver_dataframe returns empty DataFrame if Products.txt is missing."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("Submissions.txt", "ApplNo\n1")

    df = _create_silver_dataframe(buffer.getvalue())
    assert df.is_empty()


def test_gold_products_empty_source_file() -> None:
    """Test Gold layer handles empty Products.txt gracefully."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Empty Products file
        z.writestr("Products.txt", "")

    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()
        # Gold resource is yielded because Products.txt is in zip
        # But iterating it should yield nothing (return early)
        gold_prods = list(source.resources["dim_drug_product"])
        assert len(gold_prods) == 0
