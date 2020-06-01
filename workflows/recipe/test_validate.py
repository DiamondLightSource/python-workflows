"""
Tests the functionality of validate.py with several different recipes which should raise different errors
"""

import pytest
import mock
import sys

import workflows
from workflows.recipe.validate import validate_recipe, main


def test_validate_returns_type_error_when_called_without_parameters():
    with pytest.raises(TypeError):
        validate_recipe()


def test_no_errors_when_validating_healthy_recipe(tmpdir):
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
    recipe_file = tmpdir.join("recipe.json")
    recipe_file.write(healthy_recipe)

    validate_recipe(recipe_file.strpath)


def test_value_error_when_validating_bad_json(tmpdir):
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
    recipe_file = tmpdir.join("recipe.json")
    recipe_file.write(bad_json)

    # Run validate with mock open, expect JSON error (only available from python 3.5)
    with pytest.raises(ValueError):
        validate_recipe(recipe_file.strpath)


def test_workflows_error_when_validating_incorrect_workflows_recipe(tmpdir):
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
    recipe_file = tmpdir.join("recipe.json")
    recipe_file.write(bad_recipe)

    # Run validate with mock open, expect JSON error
    with pytest.raises(workflows.Error):
        validate_recipe(recipe_file.strpath)


# Create a mock of the validate call
@mock.patch("workflows.recipe.validate.validate_recipe")
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
@mock.patch("workflows.recipe.validate.validate_recipe")
def test_command_line_validation_multiple_arguments(mock_requests):
    # Create fake arguments to test on
    test_args = ["validate.py", "file1", "file2", "file3"]
    with mock.patch.object(sys, "argv", test_args):
        main()
    mock_requests.assert_has_calls(
        [mock.call("file1"), mock.call("file2"), mock.call("file3")]
    )
    assert mock_requests.call_count == 3


@mock.patch("workflows.recipe.validate.validate_recipe")
def test_system_exit_when_error_raised_from_command_line_validation(mock_requests):
    mock_requests.get.side_effect = Exception
    # Create fake arguments to test on
    test_args = ["validate.py"]
    with pytest.raises(SystemExit):
        with mock.patch.object(sys, "argv", test_args):
            main()
