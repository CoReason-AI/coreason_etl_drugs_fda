# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import polars as pl

from coreason_etl_drugs_fda.silver import generate_row_hash


def test_hash_column_order_instability() -> None:
    """
    Demonstrate that generate_row_hash is sensitive to column order.
    If we fix the code, this test should assert equality.
    """
    data = {"col_a": ["1"], "col_b": ["2"]}
    df1 = pl.DataFrame(data)
    # df2 has same data, different order
    df2 = df1.select(["col_b", "col_a"])

    # Generate hashes
    res1 = generate_row_hash(df1)
    res2 = generate_row_hash(df2)

    hash1 = res1["hash_md5"][0]
    hash2 = res2["hash_md5"][0]

    # Ideally, for data integrity, row content "A=1, B=2" is same row as "B=2, A=1".
    # If the hash is used for CDC/Deduplication, it should be stable against column reordering.
    # Currently (before fix), we expect them to be DIFFERENT because concat_str uses order.
    # After fix, they should be EQUAL.

    # We will assert equality, expecting failure first (TDD), then fix it.
    assert hash1 == hash2
