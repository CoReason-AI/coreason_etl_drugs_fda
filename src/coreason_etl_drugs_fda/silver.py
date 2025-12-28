# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import hashlib
import uuid
from datetime import date
from typing import Any, Dict, List, Optional

import polars as pl
from pydantic import BaseModel, Field

# Define a stable namespace for FDA Drugs
# Generated using uuid.uuid5(uuid.NAMESPACE_DNS, "fda.coreason.ai")
NAMESPACE_FDA = uuid.UUID("9a527060-639d-5a63-a612-9c1673322488")


class ProductSilver(BaseModel):  # type: ignore[misc]
    """
    Silver layer schema for Drug Products.
    """

    coreason_id: uuid.UUID
    appl_no: str = Field(..., pattern=r"^\d{6}$")
    product_no: str = Field(..., pattern=r"^\d{3}$")
    form: str
    strength: str
    active_ingredient: List[str]
    original_approval_date: Optional[date]
    is_historic_record: bool = False
    hash_md5: str


def generate_coreason_id(df: pl.DataFrame) -> pl.DataFrame:
    """
    Generates coreason_id using UUIDv5(NAMESPACE_FDA, f"{ApplNo}|{ProductNo}").
    Expects appl_no and product_no to be already normalized (padded strings).
    """
    # We use map_elements (apply) because UUID generation isn't natively vectorized in Polars
    # easily for UUIDv5 with custom namespace unless we construct the bytes.
    # Logic: uuid5(namespace, name)

    # We can create a struct column and apply the function

    def _create_uuid(struct: Dict[str, Any]) -> str:
        appl = struct["appl_no"]
        prod = struct["product_no"]
        name = f"{appl}|{prod}"
        return str(uuid.uuid5(NAMESPACE_FDA, name))

    df = df.with_columns(
        pl.struct(["appl_no", "product_no"])
        .map_elements(_create_uuid, return_dtype=pl.Utf8)
        .cast(pl.Utf8)  # Ensure it's string (Utf8 is safer for transport)
        .alias("coreason_id")
    )
    return df


def generate_row_hash(df: pl.DataFrame) -> pl.DataFrame:
    """
    Generates an MD5 hash of the row content for change detection.
    This is a simplified implementation hashing the concatenation of all columns as string.
    """
    # Concatenate all column values as string and hash
    # We must cast all columns to Utf8 first, especially lists.

    exprs = []
    for col_name in df.columns:
        dtype = df.schema[col_name]
        if isinstance(dtype, pl.List):
            # Convert list to string representation: join elements with ;
            # Ensure elements are strings before joining
            expr = pl.col(col_name).list.eval(pl.element().cast(pl.Utf8)).list.join(";")
        else:
            expr = pl.col(col_name).cast(pl.Utf8)

        # Fill nulls with empty string to ensure concatenation doesn't result in null
        expr = expr.fill_null("")
        exprs.append(expr)

    df = df.with_columns(
        pl.concat_str(exprs, separator="|")
        .map_elements(lambda x: hashlib.md5(x.encode()).hexdigest(), return_dtype=pl.Utf8)
        .alias("hash_md5")
    )
    return df
