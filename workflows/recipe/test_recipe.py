from __future__ import absolute_import, division
import pytest
import workflows
import workflows.recipe

def generate_recipes():
  '''Generate two recipe objects for testing.'''
  class A(workflows.recipe.Recipe):
    recipe = {
        1: { 'service': 'A service',
             'queue': 'some.queue',
             'output': [ 2 ],
           },
        2: { 'service': 'B service',
             'queue': 'another.queue',
           },
        'start': [
           (1, {}),
        ]
      }

  class B(workflows.recipe.Recipe):
    recipe = {
        1: { 'service': 'A service',
             'queue': 'some.queue',
             'output': 2,
           },
        2: { 'service': 'C service',
             'queue': 'third.queue',
           },
        'start': [
           (1, {}),
        ]
      }

  return A(), B()

def test_can_generate_recipe_objects():
  '''Test generation of recipes.'''
  A, B = generate_recipes()

  # Check that both recipies are valid
  A.validate()
  B.validate()

def test_validate_tests_for_empty_recipe():
  '''Validating a recipe that has not been defined must throw an error.'''
  A, _ = generate_recipes()
  A.recipe = None
  with pytest.raises(workflows.WorkflowsError):
    A.validate()

def test_validate_tests_for_invalid_nodes():
  '''Check that the recipe contains only numeric nodes and a non-empty 'start' node.'''
  A, _ = generate_recipes()
  A.recipe['xnode'] = None
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'xnode' in excinfo.value.message

  A, _ = generate_recipes()
  del(A.recipe['start'])
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'start' in excinfo.value.message

  A.recipe['start'] = []
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'start' in excinfo.value.message

  A.recipe['start'] = [ ('something',) ]
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'start' in excinfo.value.message

  A.recipe['start'] = [ (1, 'something'), 'banana' ]
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'start' in excinfo.value.message

  A.recipe['start'] = [ (1, 'something'), ('start', 'banana') ]
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'start' in excinfo.value.message

def test_validate_tests_for_invalid_links():
  '''Check that the nodes in recipes have valid output/error links to other nodes.'''

  # Part 1: Check outgoing links from start node:
  A, _ = generate_recipes()
  A.recipe['start'] = [ ('asdf', None) ]
  with pytest.raises(workflows.WorkflowsError):
    A.validate()

  A.recipe['start'] = [ (1, None), (2, None) ]
  A.validate()

  A.recipe['start'] = [ (1, None), (99, None) ]
  with pytest.raises(workflows.WorkflowsError):
    A.validate()

  # Part 2 & 3: Check outgoing links from other nodes (output and error)
  for outgoing in ('output', 'error'):
    A, _ = generate_recipes()
    A.recipe[1][outgoing] = 'asdf'
    with pytest.raises(workflows.WorkflowsError):
      A.validate()

    A.recipe[1][outgoing] = 99
    with pytest.raises(workflows.WorkflowsError):
      A.validate()

    A.recipe[1][outgoing] = [2, 99]
    with pytest.raises(workflows.WorkflowsError):
      A.validate()

    A.recipe[1][outgoing] = [2, 'banana']
    with pytest.raises(workflows.WorkflowsError):
      A.validate()

    A.recipe[1][outgoing] = { 'all': 2, 'some': [ 2, 2 ] }
    A.validate()

    A.recipe[1][outgoing] = { 'all': 2, 'some': [ 2, 99 ] }
    with pytest.raises(workflows.WorkflowsError):
      A.validate()

  # Part 4: Check for unreferenced nodes
  A, _ = generate_recipes()
  A.recipe[99] = {}
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert '99' in excinfo.value.message

def test_validate_tests_for_cycles():
  '''Check that validation detects cycles in recipes.  Recipes must be acyclical.'''
  A, _ = generate_recipes()
  A.recipe[2]['output'] = 1
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'cycle' in excinfo.value.message

  A, _ = generate_recipes()
  A.recipe[2]['output'] = 2
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'cycle' in excinfo.value.message

  A, _ = generate_recipes()
  A.recipe[2]['output'] = [1, 2]
  with pytest.raises(workflows.WorkflowsError) as excinfo:
    A.validate()
  assert 'cycle' in excinfo.value.message

