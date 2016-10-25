import workflows.recipe

def generate_recipes():
  '''Generate two recipe objects for testing.'''
  class A(workflows.recipe.Recipe):
    recipe = { }

  class B(workflows.recipe.Recipe):
    recipe = { }

  return A(), B()

def test_can_generate_recipe_objects():
  '''Test generation of recipes.'''
  A, B = generate_recipes()

  # Check that both recipies are valid
  A.validate()
  B.validate()

