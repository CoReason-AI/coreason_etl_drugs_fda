# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_drugs_fda

import tomllib
from pathlib import Path
import coreason_etl_drugs_fda


def test_version():
    """Test that the package version is correct."""
    assert coreason_etl_drugs_fda.__version__ == "0.2.0"


def test_pyproject_version_matches_package():
    """Test that the pyproject.toml version matches the package version."""
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject_data = tomllib.load(f)

    poetry_version = pyproject_data["tool"]["poetry"]["version"]
    project_version = pyproject_data["project"]["version"]

    assert poetry_version == coreason_etl_drugs_fda.__version__
    assert project_version == coreason_etl_drugs_fda.__version__
