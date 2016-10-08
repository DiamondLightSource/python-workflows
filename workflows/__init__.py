from __future__ import absolute_import, division

def version():
  import pkg_resources # part of setuptools
  return pkg_resources.require("workflows")[0].version

class WorkflowsError(Exception):
  '''Common class for exceptions deliberately raised by workflows package.'''
