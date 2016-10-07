from __future__ import division, absolute_import
from workflows.transport.common_transport import CommonTransport

default_transport = 'StompTransport'

def lookup(transport):
  '''Get a transport layer class based on its name.'''
  return CommonTransport.register.get(transport, \
         CommonTransport.register[default_transport])

def add_command_line_options(parser):
  '''Add command line options for all available transport layer classes.'''
  for transport in CommonTransport.register.itervalues():
    transport().add_command_line_options(parser)

def get_known_transports():
  '''Return a dictionary of all known transport mechanisms.'''
  return CommonTransport.register

CommonTransport.load(__path__)
