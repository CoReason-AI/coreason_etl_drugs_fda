# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import uuid

import polars as pl
import pytest
from pydantic import ValidationError

from coreason_etl_drugs_fda.silver import NAMESPACE_FDA, ProductSilver, generate_coreason_id, generate_row_hash


def test_generate_coreason_id() -> None:
    df = pl.DataFrame({"appl_no": ["001234", "005678"], "product_no": ["001", "002"]})

    result = generate_coreason_id(df)

    assert "coreason_id" in result.columns

    # Check determinism
    id1 = result["coreason_id"][0]
    expected_name = "001234|001"
    expected_uuid = str(uuid.uuid5(NAMESPACE_FDA, expected_name))
    assert id1 == expected_uuid

    # Check uniqueness
    id2 = result["coreason_id"][1]
    assert id1 != id2


def test_product_silver_model() -> None:
    # Valid data
    data = {
        "coreason_id": uuid.uuid4(),
        "source_id": "001234001",
        "appl_no": "001234",
        "product_no": "001",
        "form": "Tablet",
        "strength": "10mg",
        "active_ingredients_list": ["INGREDIENT A"],
        "original_approval_date": "2023-01-01",
        "hash_md5": "abc123hash",
    }
    model = ProductSilver(**data)
    assert model.appl_no == "001234"

    # Invalid appl_no (length)
    data_invalid_appl = data.copy()
    data_invalid_appl["appl_no"] = "123"  # Too short
    with pytest.raises(ValidationError):
        ProductSilver(**data_invalid_appl)

    # Invalid product_no (length)
    data_invalid_prod = data.copy()
    data_invalid_prod["product_no"] = "1"  # Too short
    with pytest.raises(ValidationError):
        ProductSilver(**data_invalid_prod)


def test_generate_row_hash() -> None:
    df = pl.DataFrame({"col1": ["a", "b"], "col2": [1, 2]})

    result = generate_row_hash(df)
    assert "hash_md5" in result.columns
    assert result["hash_md5"][0] != result["hash_md5"][1]

    # Determinism check
    result2 = generate_row_hash(df)
    assert result["hash_md5"][0] == result2["hash_md5"][0]
