from __future__ import absolute_import, division

import copy
import workflows.recipe

class RecipeWrapper(object):
  '''A wrapper object which contains a recipe and a number of functions to make
     life easier for recipe users.
  '''

  def __init__(self, message, transport=None, log=None):
    '''Create a RecipeWrapper object from a wrapped message.
       References to the transport layer are required to send directly to
       connected downstream processes. Optionally a logger is wrappend and
       extended to include global recipe information in log messages for
       tracking.
    '''
    self.recipe = workflows.recipe.Recipe(message['recipe'])
    self.recipe_pointer = message['recipe-pointer'] # TODO: failsafe way?
    self.recipe_step = self.recipe[self.recipe_pointer]
    self.recipe_path = message.get('recipe-path', [])
    self.environment = message.get('environment', {})
    self.transport = transport
    self.log = log

  def send_to(self, channel, message, header=None, **kwargs):
    if not self.transport:
      raise NotImplementedError('This RecipeWrapper object does not contain ' \
                                'a reference to a transport object.')

    if not self.recipe_step:
      raise NotImplementedError('This RecipeWrapper object does not contain ' \
                                'a recipe with a selected step.')

    if channel not in self.recipe_step:
      raise ValueError('The current recipe step does not have an output ' \
                       'channel named %s.' % channel)

    if not header:
      header = {}
    header['workflows-recipe'] = True

    def generate_full_recipe_message(destination):
      return {
          'environment': self.environment,
          'payload': message,
          'recipe': self.recipe.recipe,
          'recipe-path': self.recipe_path + [ self.recipe_pointer ],
          'recipe-pointer': destination,
             }

    destinations = self.recipe_step[channel]
    if not isinstance(destinations, list):
      destinations = [ destinations ]
    for destination in destinations:
      dest_kwargs = kwargs.copy()
      if 'transport-delay' in self.recipe[destination] and 'delay' not in kwargs:
        dest_kwargs['delay'] = self.recipe[destination]['transport-delay']
      if self.recipe[destination].get('queue'):
        self.transport.send(
            self.recipe[destination]['queue'],
            generate_full_recipe_message(destination),
            header=header, **dest_kwargs)
      if self.recipe[destination].get('topic'):
        self.transport.broadcast(
            self.recipe[destination]['topic'],
            generate_full_recipe_message(destination),
            header=header, **dest_kwargs)
