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
import requests

from coreason_etl_drugs_fda.source import drugs_fda_source


def test_source_not_a_zip() -> None:
    """
    Test that the source raises an error (or handles it) when the download is not a ZIP.
    requests.get returns content, zipfile.ZipFile tries to open it.
    It should raise BadZipFile.
    """
    mock_content = b"This is not a zip file"

    # Patch dlt.sources.helpers.requests.get
    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = mock_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        try:
            # We iterate to force execution if it's a generator, but drugs_fda_source returns a DltSource object immediately,
            # however, the body of the function runs immediately in the current implementation?
            # No, dlt sources are decorated functions.
            # But the logic inside `drugs_fda_source` (fetching the zip) runs BEFORE yielding resources.
            # So calling `drugs_fda_source()` triggers the download.
            drugs_fda_source()
        except Exception as e:
            # Check if it is BadZipFile
            assert "BadZipFile" in type(e).__name__ or isinstance(e, BadZipFile)
            return

        pytest.fail("Did not raise BadZipFile")


def test_source_empty_zip() -> None:
    """
    Test a valid ZIP that is empty (no files).
    """
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as _:
        pass  # Empty
    buffer.seek(0)

    # Patch dlt.sources.helpers.requests.get
    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
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
    # Patch dlt.sources.helpers.requests.get
    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            drugs_fda_source()
