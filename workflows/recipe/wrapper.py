from __future__ import absolute_import, division

class RecipeWrapper(object):
  '''A wrapper object which contains a recipe and a number of functions to make
     life easier for recipe users.
  '''

  def __init__(self, message):
    '''Create a RecipeWrapper object from a wrapped message.'''
    self.recipe = message['recipe']
    self.recipe_pointer = message['recipe-pointer'] # TODO: failsafe way?
    self.recipe_path = message.get('recipe-path', [])
    self.environment = message.get('environment', {})

  
