from __future__ import absolute_import, division
import mock
import pytest
import workflows
from workflows.recipe.wrapper import RecipeWrapper

def generate_recipe_message():
  '''Helper function for tests.'''
  message = {
     'recipe': {},
     'recipe-pointer': 1,
     'recipe-path': [],
     'environment': { 'ID': mock.sentinel.GUID,
                      'source': mock.sentinel.source,
                      'timestamp': mock.sentinel.timestamp,
                    },
     'payload': mock.sentinel.payload,
     }
  return message


def test_recipe_wrapper_extracts_message_information():
  m = generate_recipe_message()

  rw = RecipeWrapper(m)

  assert rw.recipe == m['recipe']
  assert rw.recipe_pointer == m['recipe-pointer']
  assert rw.recipe_path == m['recipe-path']
  assert rw.environment == m['environment']

