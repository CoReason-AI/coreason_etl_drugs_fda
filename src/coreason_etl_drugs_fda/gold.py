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
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class ProductGold(BaseModel):  # type: ignore[misc]
    """
    Gold layer schema for Drug Products (One Big Table).
    """

    coreason_id: uuid.UUID
    source_id: str = Field(..., pattern=r"^\d{9}$")
    appl_no: str = Field(..., pattern=r"^\d{6}$")
    product_no: str = Field(..., pattern=r"^\d{3}$")
    form: str
    strength: str
    active_ingredients_list: List[str]
    original_approval_date: Optional[date]

    # Gold Added Fields
    sponsor_name: Optional[str] = None
    appl_type: Optional[str] = None
    marketing_status_id: Optional[int] = None
    marketing_status_description: Optional[str] = None
    te_code: Optional[str] = None
    is_generic: bool
    is_protected: bool
    search_vector: str

    # Hash (from Silver, but maybe Gold hash needed?)
    # BRD doesn't specify Gold hash, but Silver has hash_md5.
    # Let's keep hash_md5 from Silver or recalculate.
    # Usually Gold is for consumption, so strict tracking might not be needed if not specified.
    # But for "Change Detection", maybe?
    # I'll exclude it for now unless needed.
