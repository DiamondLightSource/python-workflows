from __future__ import absolute_import, division

import copy
import workflows.recipe

class RecipeWrapper(object):
  '''A wrapper object which contains a recipe and a number of functions to make
     life easier for recipe users.
  '''

  def __init__(self, message=None, transport=None, log=None, recipe=None):
    '''Create a RecipeWrapper object from a wrapped message.
       References to the transport layer are required to send directly to
       connected downstream processes. Optionally a logger is wrappend and
       extended to include global recipe information in log messages for
       tracking.
    '''
    if message:
      self.recipe = workflows.recipe.Recipe(message['recipe'])
      self.recipe_pointer = message['recipe-pointer']
      self.recipe_step = self.recipe[self.recipe_pointer]
      self.recipe_path = message.get('recipe-path', [])
      self.environment = message.get('environment', {})
    elif recipe:
      self.recipe = workflows.recipe.Recipe(recipe)
      self.recipe_pointer = None
      self.recipe_step = None
      self.recipe_path = []
      self.environment = {}
    else:
      raise ValueError('A message or recipe is required to create ' \
                       'a RecipeWrapper object.')
    self.transport = transport
    self.log = log

  def send_to(self, channel, message, header=None, **kwargs):
    if not self.transport:
      raise ValueError('This RecipeWrapper object does not contain ' \
                       'a reference to a transport object.')

    if not self.recipe_step:
      raise ValueError('This RecipeWrapper object does not contain ' \
                       'a recipe with a selected step.')

    if channel not in self.recipe_step:
      raise ValueError('The current recipe step does not have an output ' \
                       'channel named %s.' % channel)


    destinations = self.recipe_step[channel]
    if not isinstance(destinations, list):
      destinations = [ destinations ]
    for destination in destinations:
      self._send_to_destination(destination, header, message, kwargs)

  def start(self, header=None, **kwargs):
    '''Trigger the start of a recipe, sending the defined payloads to the
       recipients set in the recipe. Any parameters to this function are
       passed to the transport send/broadcast methods.
       If the wrapped recipe has already been started then a ValueError will
       be raised.
    '''
    if not self.transport:
      raise ValueError('This RecipeWrapper object does not contain ' \
                       'a reference to a transport object.')

    if self.recipe_step:
      raise ValueError('This recipe has already been started.')

    for destination, payload in self.recipe['start']:
      self._send_to_destination(destination, header, payload, kwargs)

  def checkpoint(self, message, header=None, **kwargs):
    '''Send a message to the current recipe destination. This can be used to
       keep a state for longer processing tasks.
    '''
    if not self.transport:
      raise ValueError('This RecipeWrapper object does not contain ' \
                       'a reference to a transport object.')

    if not self.recipe_step:
      raise ValueError('This RecipeWrapper object does not contain ' \
                       'a recipe with a selected step.')

    self._send_to_destination(self.recipe_pointer, header, message, kwargs, \
                              add_path_step=False)

  def _generate_full_recipe_message(self, destination, message, add_path_step):
    '''Factory function to generate independent message objects for
       downstream recipients with different destinations.'''
    if add_path_step and self.recipe_pointer:
      recipe_path = self.recipe_path + [ self.recipe_pointer ]
    else:
      recipe_path = self.recipe_path

    return {
        'environment': self.environment,
        'payload': message,
        'recipe': self.recipe.recipe,
        'recipe-path': recipe_path,
        'recipe-pointer': destination,
    }

  def _send_to_destination(self, destination, header, payload, \
                           transport_kwargs, add_path_step=True):
    '''Helper function to send a message to a specific recipe destination.'''
    if header:
      header = header.copy()
      header['workflows-recipe'] = True
    else:
      header = { 'workflows-recipe': True }

    dest_kwargs = transport_kwargs.copy()
    if 'transport-delay' in self.recipe[destination] and \
       'delay' not in transport_kwargs:
      dest_kwargs['delay'] = self.recipe[destination]['transport-delay']
    if self.recipe[destination].get('queue'):
      self.transport.send(
          self.recipe[destination]['queue'],
          self._generate_full_recipe_message(destination, payload, add_path_step),
          header=header, **dest_kwargs)
    if self.recipe[destination].get('topic'):
      self.transport.broadcast(
          self.recipe[destination]['topic'],
          self._generate_full_recipe_message(destination, payload, add_path_step),
          header=header, **dest_kwargs)
