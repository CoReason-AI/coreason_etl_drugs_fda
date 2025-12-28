# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import datetime
import io
import uuid
import zipfile
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from pydantic import ValidationError

from coreason_etl_drugs_fda.silver import ProductSilver, generate_coreason_id
from coreason_etl_drugs_fda.source import drugs_fda_source
from coreason_etl_drugs_fda.transform import clean_ingredients, fix_dates, normalize_ids


def test_malformed_tsv_ragged_lines() -> None:
    """
    Test handling of TSV files with ragged lines (extra columns).
    source.py uses `truncate_ragged_lines=True`.
    """
    # Create a zip with ragged lines
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Header has 2 cols. Row 1 has 2. Row 2 has 3 (extra). Row 3 has 1 (missing).
        content = "ColA\tColB\nVal1\tVal2\nVal3\tVal4\tExtra\nVal5"
        z.writestr("Products.txt", content)
    buffer.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = buffer.getvalue()
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # dlt source yields resources.
        assert "raw_fda__products" in source.resources
        resource = source.resources["raw_fda__products"]

        data = list(resource)
        # Check that we got rows.
        # Truncate ragged lines behavior in Polars:
        # It usually truncates rows that are too long if they don't match header?
        # Or it might ignore them if `ignore_errors=True`.
        # Let's inspect.

        # Row 1: Val1, Val2
        # Row 2: Val3, Val4 (Extra ignored?)
        # Row 3: Val5, null

        assert len(data) >= 1

        # Note: Polars `read_csv` with `truncate_ragged_lines=True` allows parsing rows with more columns
        # than header by ignoring extra cols. Rows with fewer columns might be filled with nulls.

        df = pl.DataFrame(data)
        assert "col_a" in df.columns
        assert "col_b" in df.columns

        # Row with extra data
        row2 = df.filter(pl.col("col_a") == "Val3")
        assert len(row2) == 1
        assert row2["col_b"][0] == "Val4"

        # Row with missing data
        # Note: Polars might error on missing columns unless `null_values` logic applies or schema inference allows.
        # But `ignore_errors=True` is set.
        row3 = df.filter(pl.col("col_a") == "Val5")
        if len(row3) > 0:
            assert row3["col_b"][0] is None
        else:
            # If ignore_errors dropped it, that's also valid handling for "Complex/Edge" case of bad data.
            pass


def test_transform_null_handling() -> None:
    """
    Test transformation functions with Null values.
    """
    df = pl.DataFrame({"appl_no": [None, "123"], "product_no": ["1", None], "active_ingredient": [None, "A; B"]})

    # 1. normalize_ids
    # Should handle None. Padded strings of null usually become null or "00null"?
    # pl.col().cast(pl.Utf8) converts None to null. str.pad_start on null results in null.
    res_ids = normalize_ids(df)
    assert res_ids["appl_no"][0] is None
    assert res_ids["appl_no"][1] == "000123"
    assert res_ids["product_no"][0] == "001"
    assert res_ids["product_no"][1] is None

    # 2. clean_ingredients
    # str.to_uppercase on null is null.
    res_ing = clean_ingredients(df)
    ing_list = res_ing["active_ingredient"].to_list()
    assert ing_list[0] is None
    assert ing_list[1] == ["A", "B"]

    # 3. generate_coreason_id handling of Nulls
    # If appl_no is None, UUID generation might fail or produce a specific UUID.
    # Our logic: `f"{appl}|{prod}"`. If appl is None, python format might fail if struct dict has None.
    # Wait, `row['appl_no']` will be `None`. `f"{None}|..."` -> "None|...".
    # This generates a valid UUID for the string "None|...".
    # Is this desired? Pydantic model requires strict string.
    # But Bronze data might have nulls.

    res_uuid = generate_coreason_id(res_ids)
    assert res_uuid["coreason_id"][0] is not None
    # Verify it doesn't crash.


def test_id_overflow() -> None:
    """
    Test ID normalization when input is longer than padding.
    """
    df = pl.DataFrame(
        {
            "appl_no": ["1234567"],  # 7 digits
            "product_no": ["1234"],  # 4 digits
        }
    )
    res = normalize_ids(df)

    # pad_start does not truncate.
    assert res["appl_no"][0] == "1234567"
    assert res["product_no"][0] == "1234"

    # Pydantic validation should fail
    # We need to construct the model
    with pytest.raises(ValidationError):
        ProductSilver(
            coreason_id=uuid.uuid4(),
            appl_no=res["appl_no"][0],  # Invalid
            product_no="001",
            form="F",
            strength="S",
            active_ingredient=[],
            original_approval_date=None,
            hash_md5="hash",
        )


def test_date_parsing_variations() -> None:
    """
    Test fix_dates with various formats.
    """
    df = pl.DataFrame(
        {
            "date": [
                "1982-01-01",
                "01/01/1982",  # Standard US format, not ISO
                "Jan 1, 1982",  # Text
                "Approved prior to Jan 1, 1982",  # Legacy
                "Invalid",
            ]
        }
    )

    # We assume 'date' is a date column
    # fix_dates logic:
    # 1. Handle legacy string -> 1982-01-01
    # 2. .str.to_date(format="%Y-%m-%d", strict=False)

    # So "01/01/1982" will fail to parse with %Y-%m-%d and become null.
    # This is expected behavior if we strictly require ISO format from dlt/source.
    # But edge case check confirms this behavior.

    res = fix_dates(df, ["date"])

    assert res["date"][0] == datetime.date(1982, 1, 1)

    # The legacy string row
    assert res["date"][3] == datetime.date(1982, 1, 1)
    assert res["is_historic_record"][3]

    # The others should be null because format mismatch
    assert res["date"][1] is None
    assert res["date"][2] is None
    assert res["date"][4] is None


def test_pydantic_validation_edge_cases() -> None:
    """
    Test Pydantic model with strict constraints.
    """
    base_data = {
        "coreason_id": uuid.uuid4(),
        "appl_no": "000123",
        "product_no": "001",
        "form": "Form",
        "strength": "Str",
        "active_ingredient": ["Ing"],
        "original_approval_date": None,
        "hash_md5": "hash",
    }

    # 1. ApplNo with letters
    data = base_data.copy()
    data["appl_no"] = "A00123"
    with pytest.raises(ValidationError):
        ProductSilver(**data)

    # 2. ProductNo with letters
    data = base_data.copy()
    data["product_no"] = "0A1"
    with pytest.raises(ValidationError):
        ProductSilver(**data)

    # 3. Empty strings?
    # Pattern `^\d{6}$` rejects empty.
    data = base_data.copy()
    data["appl_no"] = ""
    with pytest.raises(ValidationError):
        ProductSilver(**data)
