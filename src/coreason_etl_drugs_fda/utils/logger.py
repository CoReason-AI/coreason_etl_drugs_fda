# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import os
import sys
from pathlib import Path

from loguru import logger

# Define the log directory
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"

# Remove default handler
logger.remove()

# Sink 1: Stdout
logger.add(
    sys.stderr,
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    ),
)

# Sink 2: File
logger.add(
    str(LOG_FILE),
    rotation="500 MB",
    retention="10 days",
    serialize=True,
    enqueue=True,
    level=os.getenv("LOG_LEVEL", "INFO"),
)

__all__ = ["logger"]
