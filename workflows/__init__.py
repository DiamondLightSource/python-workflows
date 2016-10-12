from __future__ import absolute_import, division

def load_plugins(paths):
  '''Import all python files (except test_*) in directories. This is required
     for registration of services and transport plugins.
     :param paths: A path or list of paths containing files to import.
  '''
  import imp, pkgutil
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
