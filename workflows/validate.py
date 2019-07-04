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
    entry: workflows-recipe-validate
```
"""

import logging
import workflows.recipe
import workflows
import sys
import json


def validate(json_filename: str):
    """Reads a json file, tries to turn it into a recipe and then validates it.
    Exits on exception with non-zero error"""

    # Read in the file
    try:
        with open(json_filename) as f:
            recipe_text = f.read()
    except Exception:
        logging.exception(f"Could not recipe from {json_filename}")
        sys.exit(1)

    # Turn it into a recipe and validate
    try:
        recipe = workflows.recipe.Recipe(recipe_text).validate()
    except json.decoder.JSONDecodeError as e:
        logging.error(f"JSON error in recipe {json_filename}, please address this")
        logging.error(f"{e.msg} at line {e.lineno} col {e.colno}")
        sys.exit(1)
    except workflows.Error as e:
        logging.error(f"JSON error in recipe {json_filename}, please address this")
        logging.error(f"{e}")
        sys.exit(1)
    except Exception:
        logging.exception(f"Problem in recipe {json_filename}, please address this")
        sys.exit(1)


def main():
    """Run the program from entry point"""
    validate(sys.argv[1])


if __name__ == "__main__":

    validate(sys.argv[1])
