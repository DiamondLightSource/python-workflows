"""
Tests the functionality of validate.py with several different recipes which should raise different errors
"""

import pytest
import mock
import workflows
from workflows.validate import validate_recipe, main
import json
import sys

# Python 2 and 3 compatibility
if sys.version_info[0] == 3:
    builtins_open = "builtins.open"
else:
    builtins_open = "__builtins__.open"


def test_no_file():
    """Test that validate returns an error when it returns with no file"""
    with pytest.raises(Exception):
        validate_recipe()


def test_healthy_recipe():
    """Test that no errors are raised when given a healthy recipe"""
    healthy_recipe = """
        {
          "1": {
            "queue": "simpleservice.submission",
            "parameters": {
              "commands": [
                "echo This is a command32"
              ],
              "workingdir": "/dls/tmp/riw56156/zocalo",
              "output_file": "out.txt"
            }
          },
          "start": [
            [1, []]
          ]
        }
    """

    # Run validate with mock open, expect no exceptions
    with mock.patch(
        builtins_open, mock.mock_open(read_data=healthy_recipe), create=True
    ) as mock_file:
        validate_recipe("/path/to/open")


def test_bad_json():
    """Test that no errors are raised when given a bad json recipe"""
    bad_json = """
        {
          "1": {
            "queue": "simpleservice.submission"
            "parameters": {
              "commands": [
                "echo This is a command32"
              ],
              "workingdir": "/dls/tmp/riw56156/zocalo",
              "output_file": "out.txt"
            }
          },
          "start": [
            [1, []]
          ]
        }
    """

    # Run validate with mock open, expect JSON error
    with pytest.raises(json.decoder.JSONDecodeError):
        with mock.patch(
            builtins_open, mock.mock_open(read_data=bad_json), create=True
        ) as mock_file:
            validate_recipe("/path/to/open")


def test_bad_recipe():
    """Test that no errors are raised when given a bad zocalo recipe"""
    bad_recipe = """
        {
          "1": {
            "queue": "simpleservice.submission",
            "parameters": {
              "commands": [
                "echo This is a command32"
              ],
              "workingdir": "/dls/tmp/riw56156/zocalo",
              "output_file": "out.txt"
            }
          }
        }
    """

    # Run validate with mock open, expect JSON error
    with pytest.raises(workflows.Error):
        with mock.patch(
            builtins_open, mock.mock_open(read_data=bad_recipe), create=True
        ) as mock_file:
            validate_recipe("/path/to/open")


# Create a mock of the validate call
@mock.patch("workflows.validate.validate_recipe")
def test_main(mock_requests):
    """Test that the main function call works with one inputs"""
    # Create fake arguments to test on
    test_args = ["validate.py", "file1"]
    with mock.patch.object(sys, "argv", test_args):
        main()
    mock_requests.assert_called_once()
    mock_requests.assert_called_with("file1")


def test_main():
    """Test that the main function exits with no inputs (prompted by argparse)"""
    # Create fake arguments to test on
    test_args = ["validate.py"]
    with pytest.raises(SystemExit):
        with mock.patch.object(sys, "argv", test_args):
            main()


# Create a mock of the validate call
@mock.patch("workflows.validate.validate_recipe")
def test_main_multiple(mock_requests):
    """Test that the main function call works with a number of inputs"""
    # Create fake arguments to test on
    test_args = ["validate.py", "file1", "file2", "file3"]
    with mock.patch.object(sys, "argv", test_args):
        main()
    mock_requests.has_calls(
        [mock.call("file1"), mock.call("file2"), mock.call("file3")]
    )


@mock.patch("workflows.validate.validate_recipe")
def test_exit_on_bad_recipe(mock_requests):
    """Test that main exits when validate_recipe finds an error"""
    mock_requests.get.side_effect = Exception
    # Create fake arguments to test on
    test_args = ["validate.py"]
    with pytest.raises(SystemExit):
        with mock.patch.object(sys, "argv", test_args):
            main()
