"""
Runs the json validator on a json file and raises an exception if there is an error
Intended to be used as a hook for repositories which are developing recipes

Example of how this could be used in a .pre-commit-config.yaml file:
```
# Validate json recipes
- repo: https://github.com/DiamondLightSource/python-workflows
  hooks:
  - name: Recipe validation
    id: recipe-validation
    language: python
    files: ^recipes/
    entry: workflows.validate-recipe
```
"""

import argparse
import logging
import sys

import workflows.recipe
import workflows


def validate_recipe(json_filename):
    """Reads a json file, tries to turn it into a recipe and then validates it.
    Exits on exception with non-zero error"""

    # Read in the file
    try:
        with open(json_filename) as f:
            recipe_text = f.read()
    except Exception:
        logging.exception("Could not read recipe from {}".format(json_filename))
        raise

    # Turn it into a recipe and validate
    try:
        # Create a recipe object and validate it
        workflows.recipe.Recipe(recipe_text).validate()
    except ValueError as e:
        # json.JSONDecodeError only available from python 3.5.x
        logging.error("JSON error in recipe {}:\n{}".format(json_filename, e))
        raise e
    except workflows.Error as e:
        logging.error("Problem in recipe {}:\n{}".format(json_filename, e))
        raise e
    except Exception as e:
        logging.error("Problem in recipe {}: {}".format(json_filename, e))
        raise e


def main():
    """Run the program from entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "files", nargs="+", help="file or list of files to be validated"
    )
    args = parser.parse_args()

    # Validate every file provided, keep list of fails
    failed_files = []
    for input_file in args.files:
        try:
            validate_recipe(input_file)
        except Exception:
            failed_files.append(input_file)

    # Let the user know which files had errors (summary of previous output)
    # Otherwise exit silently
    if failed_files:
        print("Errors found in the following recipes: {}".format(failed_files))
        sys.exit(1)


if __name__ == "__main__":

    main()
