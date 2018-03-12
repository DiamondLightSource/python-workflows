from __future__ import absolute_import, division, print_function

__version__ = '0.63'

def version():
  '''Returns the version number of the installed workflows package.'''
  return __version__

class WorkflowsError(Exception):
  '''Common class for exceptions deliberately raised by workflows package.'''

class DisconnectedError(WorkflowsError):
  '''Indicates the connection could not be established or has been lost.'''

class AuthenticationError(WorkflowsError):
  '''Indicates the connection could not be established due to incorrect credentials.'''
