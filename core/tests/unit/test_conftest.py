from unittest.mock import mock_open, patch

import pytest


def test_get_test_csv_fixture(get_test_csv):
    """
    Explicitly test the get_test_csv fixture logic to cover lines 14-20 in conftest.py
    """
    # 1. Test Successful Read
    with patch("builtins.open", mock_open(read_data="mock_content")), patch("pathlib.Path.exists", return_value=True):
        content = get_test_csv("dummy.csv")
        assert content == "mock_content"

    # 2. Test File Not Found
    with (
        patch("pathlib.Path.exists", return_value=False),
        pytest.raises(FileNotFoundError, match="Test data file not found"),
    ):
        get_test_csv("non_existent.csv")
