from __future__ import absolute_import, division, print_function

import pkg_resources

default_transport = 'StompTransport'

def lookup(transport):
  '''Get a transport layer class based on its name.'''
  return get_known_transports().get(transport, \
         get_known_transports()[default_transport])

def add_command_line_options(parser):
  '''Add command line options for all available transport layer classes.'''
  for transport in get_known_transports().values():
    transport().add_command_line_options(parser)

def get_known_transports():
  '''Return a dictionary of all known transport mechanisms.'''
  if not hasattr(get_known_transports, 'cache'):
    setattr(get_known_transports, 'cache', {
      e.name: e.load()
      for e in pkg_resources.iter_entry_points('workflows.transport')
    })
  return get_known_transports.cache.copy()
