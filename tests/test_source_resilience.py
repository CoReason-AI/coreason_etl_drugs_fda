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
from unittest.mock import MagicMock, patch
from zipfile import BadZipFile

import pytest

from coreason_etl_drugs_fda.source import drugs_fda_source


def test_source_not_a_zip() -> None:
    """
    Test that the source raises an error (or handles it) when the download is not a ZIP.
    requests.get returns content, zipfile.ZipFile tries to open it.
    It should raise BadZipFile.
    """
    mock_content = b"This is not a zip file"

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # drugs_fda_source is a dlt source function.
        # Exceptions raised inside the source function body during initialization
        # (before yielding resources) are often wrapped by dlt in SourceDataExtractionError
        # or similar, OR they might bubble up if they happen before dlt machinery takes over.

        # But wait, looking at the traceback, it IS BadZipFile.
        # Try iterating the source to trigger execution if dlt makes it lazy?
        # But existing test `test_source_resilience.py` failed ON definition.

        # Let's catch Exception and check type name to be safe against dlt wrapping or import mismatches.

        with pytest.raises(ValueError, match="Downloaded content is not a ZIP"):
            drugs_fda_source()


def test_source_empty_zip() -> None:
    """
    Test a valid ZIP that is empty (no files).
    """
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as _:
        pass  # Empty
    buffer.seek(0)

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        source = drugs_fda_source()

        # Should return a source with NO resources (or empty list of resources)
        # Because we iterate `for filename in files_present:`
        # files_present will be empty.

        assert len(source.resources) == 0


def test_source_http_error() -> None:
    """
    Test that HTTP errors are raised.
    """
    import requests

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=404)
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            drugs_fda_source()

def test_source_corrupted_zip() -> None:
    """
    Test a file that starts with PK but is corrupted.
    Should raise BadZipFile (and log error).
    """
    import zipfile
    mock_content = b"PK\x03\x04" + b"trash" * 10

    with patch("coreason_etl_drugs_fda.source.cffi_requests.get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with pytest.raises(zipfile.BadZipFile):
            drugs_fda_source()
