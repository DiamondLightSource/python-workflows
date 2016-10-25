class Recipe(object):
  '''Object containing a processing recipe that can be passed to services.
     A recipe describes how all involved services are connected together, how
     data should be passed and how errors should be handled.'''

  def validate(self):
    '''Check whether the encoded recipe is valid. It must describe a directed
       acyclical graph, all connections must be defined, etc.'''
    return True
