import workflows

class Recipe(object):
  '''Object containing a processing recipe that can be passed to services.
     A recipe describes how all involved services are connected together, how
     data should be passed and how errors should be handled.'''

  recipe = {}
  '''The processing recipe is encoded in this dictionary.'''
  # TODO: Describe format

  def validate(self):
    '''Check whether the encoded recipe is valid. It must describe a directed
       acyclical graph, all connections must be defined, etc.'''
    if not self.recipe:
      raise workflows.WorkflowsError('Invalid recipe: No recipe defined')

    # Without a 'start' node nothing would happen
    if 'start' not in self.recipe:
      raise workflows.WorkflowsError('Invalid recipe: "start" node missing')
    if not self.recipe['start']:
      raise workflows.WorkflowsError('Invalid recipe: "start" node empty')
    if not all(isinstance(x, tuple) and len(x) == 2 for x in self.recipe['start']):
      raise workflows.WorkflowsError('Invalid recipe: "start" node invalid')
    if any(x[0] == 'start' for x in self.recipe['start']):
      raise workflows.WorkflowsError('Invalid recipe: "start" node points to itself')

    # All other nodes must be numeric
    nodes = filter(lambda x: not isinstance(x, int) and x != 'start', self.recipe)
    if nodes:
      raise workflows.WorkflowsError('Invalid recipe: Node "%s" is not numeric' % nodes[0])

    # Detect cycles
    touched_nodes = set(['start'])
    def flatten_links(struct):
      '''Take an output/error link object, list or dictionary and return flat list of linked nodes.'''
      if struct is None: return []
      if isinstance(struct, int): return [ struct ]
      if isinstance(struct, list):
        if not all(isinstance(x, int) for x in struct):
          raise workflows.WorkflowsError('Invalid recipe: Invalid link in recipe (%s)' % str(struct))
        return struct
      if isinstance(struct, dict):
        joined_list = []
        for sub_list in struct.values():
          joined_list += flatten_links(sub_list)
        return joined_list
      raise workflows.WorkflowsError('Invalid recipe: Invalid link in recipe (%s)' % str(struct))
    def find_cycles(path):
      '''Depth-First-Search helper function to identify cycles.'''
      if path[-1] not in self.recipe:
        raise workflows.WorkflowsError('Invalid recipe: Node "%s" is referenced via "%s" but missing' % (str(path[-1]), str(path[:-1])))
      touched_nodes.add(path[-1])
      node = self.recipe[path[-1]]
      for outgoing in ('output', 'error'):
        if outgoing in node:
          references = flatten_links(node[outgoing])
          for n in references:
            if n in path:
              raise workflows.WorkflowsError('Invalid recipe: Recipe contains cycle (%s -> %s)' % (str(path), str(n)))
            find_cycles(path + [n])
    for link in self.recipe['start']:
      find_cycles(['start', link[0]])

    # Test recipe for unreferenced nodes
    for node in self.recipe:
      if node not in touched_nodes:
        raise workflows.WorkflowsError('Invalid recipe: Recipe contains unreferenced node "%s"' % str(node))
