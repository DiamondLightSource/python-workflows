"""
Tests the functionality of validate.py with several different recipes which should raise different errors
"""

import pytest
import mock
import workflows
from workflows.validate import validate_recipe, main
import sys

# Python 2 and 3 compatibility
if sys.version_info[0] == 3:
    builtins_open = "builtins.open"
else:
    builtins_open = "__builtins__.open"


def test_validate_returns_type_error_when_called_without_parameters():
    with pytest.raises(TypeError):
        validate_recipe()


def test_no_errors_when_validating_healthy_recipe():
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


def test_value_error_when_validating_bad_json():
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

    # Run validate with mock open, expect JSON error (only available from python 3.5)
    with pytest.raises(ValueError):
        with mock.patch(
            builtins_open, mock.mock_open(read_data=bad_json), create=True
        ) as mock_file:
            validate_recipe("/path/to/open")


def test_workflows_error_when_validating_incorrect_workflows_recipe():
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
def test_command_line_validation_one_argument(mock_requests):
    # Create fake arguments to test on
    test_args = ["validate.py", "file1"]
    with mock.patch.object(sys, "argv", test_args):
        main()
    mock_requests.assert_called_once_with("file1")


def test_exit_when_command_line_validation_given_no_arguments():
    # Create fake arguments to test on
    test_args = ["validate.py"]
    with pytest.raises(SystemExit):
        with mock.patch.object(sys, "argv", test_args):
            main()


# Create a mock of the validate call
@mock.patch("workflows.validate.validate_recipe")
def test_command_line_validation_multiple_arguments(mock_requests):
    # Create fake arguments to test on
    test_args = ["validate.py", "file1", "file2", "file3"]
    with mock.patch.object(sys, "argv", test_args):
        main()
    mock_requests.assert_has_calls(
        [mock.call("file1"), mock.call("file2"), mock.call("file3")]
    )
    assert mock_requests.call_count == 3


@mock.patch("workflows.validate.validate_recipe")
def test_system_exit_when_error_raised_from_command_line_validation(mock_requests):
    mock_requests.get.side_effect = Exception
    # Create fake arguments to test on
    test_args = ["validate.py"]
    with pytest.raises(SystemExit):
        with mock.patch.object(sys, "argv", test_args):
            main()
