from __future__ import absolute_import, division

def load_plugins(paths):
  '''Import all python files (except test_*) in directories. This is required
     for registration of services and transport plugins.
     :param paths: A path or list of paths containing files to import.
  '''
  import imp, pkgutil
  try: # Python3 compatibility
    basestring = basestring
  except NameError:
    basestring = (str, bytes)
  if isinstance(paths, basestring):
    paths = list(paths)
  for _, name, _ in pkgutil.iter_modules(paths):
    if not name.startswith('test_'):
      fid, pathname, desc = imp.find_module(name, paths)
      imp.load_module(name, fid, pathname, desc)
      if fid:
        fid.close()

def version():
  '''Returns the version number of the installed workflows package.'''
  import pkg_resources # part of setuptools
  return pkg_resources.require("workflows")[0].version

class WorkflowsError(Exception):
  '''Common class for exceptions deliberately raised by workflows package.'''

class DisconnectedError(WorkflowsError):
  '''Indicates the connection could not be established or has been lost.'''

class AuthenticationError(WorkflowsError):
  '''Indicates the connection could not be established due to incorrect credentials.'''

def add_plugin_register_to_class(*bases):
  # This function is based off the six library
  # https://github.com/benjaminp/six
  #
  # Copyright (c) 2010-2017 Benjamin Peterson
  #
  # Permission is hereby granted, free of charge, to any person obtaining a copy of
  # this software and associated documentation files (the "Software"), to deal in
  # the Software without restriction, including without limitation the rights to
  # use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
  # the Software, and to permit persons to whom the Software is furnished to do so,
  # subject to the following conditions:
  #
  # The above copyright notice and this permission notice shall be included in all
  # copies or substantial portions of the Software.
  #
  # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
  # FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
  # COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
  # IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  # CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

  """Create a base class with a plugin registry metaclass."""
  # This requires a bit of explanation: the basic idea is to make a dummy
  # metaclass for one level of class instantiation that replaces itself with
  # the actual metaclass.

  class PluginRegister(type):
    '''Define metaclass function to keep a list of all subclasses. This enables
       looking up subclasses by name.'''
    def __init__(cls, name, base, attrs):
      '''Add new subclass to list of all known subclasses.'''
      if not hasattr(cls, 'plugin_register'):
        cls.plugin_register = {}
      else:
        cls.plugin_register[name] = cls

  class Metametaclass(PluginRegister):
    '''Dummy metaclass.'''
    def __new__(cls, name, this_bases, d):
      '''Return the actual metaclass.'''
      return PluginRegister(name, bases, d)

  return type.__new__(Metametaclass, 'temporary_class', (), {})
