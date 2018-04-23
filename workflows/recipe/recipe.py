from __future__ import absolute_import, division, print_function

import copy
import json
import string

import workflows

try: # Python3 compatibility
  basestring = basestring
except NameError:
  basestring = (str, bytes)

class Recipe(object):
  '''Object containing a processing recipe that can be passed to services.
     A recipe describes how all involved services are connected together, how
     data should be passed and how errors should be handled.'''

  recipe = {}
  '''The processing recipe is encoded in this dictionary.'''
  # TODO: Describe format

  def __init__(self, recipe=None):
    '''Constructor allows passing in a recipe dictionary.'''
    if isinstance(recipe, basestring):
      self.recipe = self.deserialize(recipe)
    elif recipe:
      self.recipe = self._sanitize(recipe)

  def deserialize(self, string):
    '''Convert a recipe that has been stored as serialized json string to a
       data structure.'''
    return self._sanitize(json.loads(string))

  @staticmethod
  def _sanitize(recipe):
    '''Clean up a recipe that may have been stored as serialized json string.
       Convert any numerical pointers that are stored as strings to integers.'''
    recipe = recipe.copy()
    for k in list(recipe):
      if k not in ('start', 'error') and int(k) and k != int(k):
        recipe[int(k)] = recipe[k]
        del(recipe[k])
    for k in list(recipe):
      if 'output' in recipe[k] and not isinstance(recipe[k]['output'], (list, dict)):
        recipe[k]['output'] = [ recipe[k]['output'] ]
      # dicts should be normalized, too
    if 'start' in recipe:
      recipe['start'] = [ tuple(x) for x in recipe['start'] ]
    return recipe

  def serialize(self):
    '''Write out the current recipe as serialized json string.'''
    return json.dumps(self.recipe)

  def pretty(self):
    '''Write out the current recipe as serialized json string with pretty formatting.'''
    return json.dumps(self.recipe, indent=2)

  def __getitem__(self, item):
    '''Allow direct dictionary access to recipe elements.'''
    return self.recipe.__getitem__(item)

  def __contains__(self, item):
    '''Testing for presence of recipe elements.'''
    return item in self.recipe

  def __eq__(self, other):
    '''Overload equality operator (!=) to allow comparing recipe objects
       with one another and with their string representations.'''
    if isinstance(other, Recipe):
      return self.recipe == other.recipe
    if isinstance(other, dict):
      return self.recipe == self._sanitize(other)
    return self.recipe == self.deserialize(other)

  def __ne__(self, other):
    '''Overload inequality operator (!=) to allow comparing recipe objects
       with one another and with their string representations.'''
    result = self.__eq__(other)
    if result is NotImplemented:
      return result
    return not result

  def __hash__(self):
    '''Recipe objects are mutable and therefore should not be hashable.'''
    return None

  def validate(self):
    '''Check whether the encoded recipe is valid. It must describe a directed
       acyclical graph, all connections must be defined, etc.'''
    if not self.recipe:
      raise workflows.Error('Invalid recipe: No recipe defined')

    # Without a 'start' node nothing would happen
    if 'start' not in self.recipe:
      raise workflows.Error('Invalid recipe: "start" node missing')
    if not self.recipe['start']:
      raise workflows.Error('Invalid recipe: "start" node empty')
    if not all(isinstance(x, (list, tuple)) and len(x) == 2
               for x in self.recipe['start']):
      raise workflows.Error('Invalid recipe: "start" node invalid')
    if any(x[0] == 'start' for x in self.recipe['start']):
      raise workflows.Error('Invalid recipe: "start" node points to itself')

    # Check that 'error' node points to regular nodes only
    if 'error' in self.recipe and \
        isinstance(self.recipe['error'], (list, tuple, basestring)):
      if 'start' in self.recipe['error']:
        raise workflows.Error('Invalid recipe: "error" node points to "start" node')
      if 'error' in self.recipe['error']:
        raise workflows.Error('Invalid recipe: "error" node points to itself')

    # All other nodes must be numeric
    nodes = list(filter(lambda x: not isinstance(x, int)
                             and x not in ('start', 'error'),
                        self.recipe))
    if nodes:
      raise workflows.Error('Invalid recipe: Node "%s" is not numeric' % nodes[0])

    # Detect cycles
    touched_nodes = set(['start', 'error'])
    def flatten_links(struct):
      '''Take an output/error link object, list or dictionary and return flat list of linked nodes.'''
      if struct is None: return []
      if isinstance(struct, int): return [ struct ]
      if isinstance(struct, list):
        if not all(isinstance(x, int) for x in struct):
          raise workflows.Error('Invalid recipe: Invalid link in recipe (%s)' % str(struct))
        return struct
      if isinstance(struct, dict):
        joined_list = []
        for sub_list in struct.values():
          joined_list += flatten_links(sub_list)
        return joined_list
      raise workflows.Error('Invalid recipe: Invalid link in recipe (%s)' % str(struct))
    def find_cycles(path):
      '''Depth-First-Search helper function to identify cycles.'''
      if path[-1] not in self.recipe:
        raise workflows.Error('Invalid recipe: Node "%s" is referenced via "%s" but missing' % (str(path[-1]), str(path[:-1])))
      touched_nodes.add(path[-1])
      node = self.recipe[path[-1]]
      for outgoing in ('output', 'error'):
        if outgoing in node:
          references = flatten_links(node[outgoing])
          for n in references:
            if n in path:
              raise workflows.Error('Invalid recipe: Recipe contains cycle (%s -> %s)' % (str(path), str(n)))
            find_cycles(path + [n])
    for link in self.recipe['start']:
      find_cycles(['start', link[0]])
    if 'error' in self.recipe:
      if isinstance(self.recipe['error'], (list, tuple)):
        for link in self.recipe['error']:
          find_cycles(['error', link])
      else:
        find_cycles(['error', self.recipe['error']])

    # Test recipe for unreferenced nodes
    for node in self.recipe:
      if node not in touched_nodes:
        raise workflows.Error('Invalid recipe: Recipe contains unreferenced node "%s"' % str(node))

  def apply_parameters(self, parameters):
    '''Recursively apply dictionary entries in 'parameters' to {item}s in recipe
       structure, leaving undefined {item}s as they are. A special case is a
       {$REPLACE:item}, which replaces the string with a copy of the referenced
       parameter item.

       Examples:

       parameters = { 'x':'5' }
       apply_parameters( { '{x}': '{y}' }, parameters )
          => { '5': '{y}' }

       parameters = { 'y':'5' }
       apply_parameters( { '{x}': '{y}' }, parameters )
          => { '{x}': '5' }

       parameters = { 'x':'3', 'y':'5' }
       apply_parameters( { '{x}': '{y}' }, parameters )
          => { '3': '5' }

       parameters = { 'l': [ 1, 2 ] }
       apply_parameters( { 'x': '{$REPLACE:l}' }, parameters )
          => { 'x': [ 1, 2 ] }
    '''

    class SafeString(object):
      def __init__(self, s):
        self.string = s
      def __repr__(self):
        return '{' + self.string + '}'
      def __str__(self):
        return '{' + self.string + '}'
      def __getitem__(self, item):
        return SafeString(self.string + '[' + item + ']')

    class SafeDict(dict):
      '''A dictionary that returns undefined keys as {keyname}.
         This can be used to selectively replace variables in datastructures.'''
      def __missing__(self, key):
        return SafeString(key)

    # By default the python formatter class is used to resolve {item} references
    formatter = string.Formatter()

    # Special format strings "{$REPLACE:(...)}" use this data structure
    # formatter to return the referenced data structure rather than a formatted
    # string.
    ds_formatter = string.Formatter()
    def ds_format_field(value, spec):
      ds_format_field.last = value
      return ''
    ds_formatter.format_field = ds_format_field

    params = SafeDict(parameters)

    def _recursive_apply(item):
      '''Helper function to recursively apply replacements.'''
      if isinstance(item, basestring):
        if item.startswith('{$REPLACE') and item.endswith('}'):
          try:
            ds_formatter.vformat("{" + item[10:-1] + "}", (), parameters)
          except KeyError:
            return None
          return copy.deepcopy(ds_formatter.format_field.last)
        else:
          return formatter.vformat(item, (), params)
      if isinstance(item, dict):
        return { _recursive_apply(key): _recursive_apply(value) for
                 key, value in item.items() }
      if isinstance(item, tuple):
        return tuple(_recursive_apply(list(item)))
      if isinstance(item, list):
        return [ _recursive_apply(x) for x in item ]
      return item

    self.recipe = _recursive_apply(self.recipe)

  def merge(self, other):
    '''Merge two recipes together, returning a single recipe containing all
       nodes.
       Note: This does NOT yet return a minimal recipe.
       :param other: A Recipe object that should be merged with the current
                     Recipe object.
       :return: A new Recipe object containing information from both recipes.
    '''

    # Merging empty values returns a copy of the original
    if not other:
      return Recipe(self.recipe)

    # When a string is passed, merge with a constructed recipe object
    if isinstance(other, basestring):
      return self.merge(Recipe(other))

    # Merging empty recipes returns a copy of the original
    if not other.recipe:
      return Recipe(self.recipe)

    # If own recipe empty, use other recipe
    if not self.recipe:
      return Recipe(other.recipe)

    # Assuming both recipes are valid
    self.validate()
    other.validate()

    # Start from current recipe
    new_recipe = self.recipe

    # Find the maximum index of the current recipe
    max_index = max(1, *filter(lambda x:isinstance(x, int), self.recipe.keys()))
    next_index = max_index + 1

    # Set up a translation table for indices and copy all entries
    translation = {}
    for key, value in other.recipe.items():
      if isinstance(key, int):
        if key not in translation:
          translation[key] = next_index
          next_index = next_index + 1
        new_recipe[translation[key]] = value

    # Rewrite all copied entries to point to new keys
    def translate(x):
      if isinstance(x, list):
        return list(map(translate, x))
      elif isinstance(x, tuple):
        return tuple(map(translate, x))
      elif isinstance(x, dict):
        return { k: translate(v) for k, v in x.items() }
      else:
        return translation[x]
    for idx in translation.values():
      if 'output' in new_recipe[idx]:
        new_recipe[idx]['output'] = translate(new_recipe[idx]['output'])
      if 'error' in new_recipe[idx]:
        new_recipe[idx]['error'] = translate(new_recipe[idx]['error'])

    # Join 'start' nodes
    for (idx, param) in other.recipe['start']:
      new_recipe['start'].append((translate(idx), param))

    # Join 'error' nodes
    if 'error' in other.recipe:
      if 'error' not in new_recipe:
        new_recipe['error'] = translate(other.recipe['error'])
      else:
        if isinstance(new_recipe['error'], (list, tuple)):
          new_recipe['error'] = list(new_recipe['error'])
        else:
          new_recipe['error'] = list([new_recipe['error']])
        if isinstance(other.recipe['error'], (list, tuple)):
          new_recipe['error'].extend(translate(other.recipe['error']))
        else:
          new_recipe['error'].append(translate(other.recipe['error']))

#   # Minimize DAG
#   queuehash, topichash = {}, {}
#   for k, v in new_recipe.items():
#     if isinstance(v, dict):
#       if 'queue' in v:
#         queuehash[v['queue']] = queuehash.get(v['queue'], [])
#         queuehash[v['queue']].append(k)
#       if 'topic' in v:
#         topichash[v['topic']] = topichash.get(v['topic'], [])
#         topichash[v['topic']].append(k)
#
#   print queuehash
#   print topichash

    return Recipe(new_recipe)
