# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

"""
Fast, extensible Python ETL to maintain a local Drugs FDA mirror
"""

import os
import sys

from loguru import logger

__version__ = "0.1.0"
__author__ = "Gowtham A Rao"
__email__ = "gowtham.rao@coreason.ai"

# Configure logging
logger.remove()
logger.add(sys.stderr, level=os.getenv("LOG_LEVEL", "INFO"))

__all__ = []
