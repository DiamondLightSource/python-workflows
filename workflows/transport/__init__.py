from __future__ import absolute_import, division
import workflows
from workflows.transport.common_transport import CommonTransport

default_transport = 'StompTransport'

def lookup(transport):
  '''Get a transport layer class based on its name.'''
  return CommonTransport.plugin_register.get(transport, \
         CommonTransport.plugin_register[default_transport])

def add_command_line_options(parser):
  '''Add command line options for all available transport layer classes.'''
  for transport in CommonTransport.plugin_register.values():
    transport().add_command_line_options(parser)

def get_known_transports():
  '''Return a dictionary of all known transport mechanisms.'''
  return CommonTransport.plugin_register

workflows.load_plugins(__path__)
