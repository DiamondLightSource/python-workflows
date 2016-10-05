from __future__ import division
import workflows.transport

all_transports = {
  'stomp': lambda: workflows.transport.stomp.Transport
}
default_transport = 'stomp'

def lookup(transport):
  '''Get a transport layer class based on its name.'''
  return all_transports.get(transport, all_transports[default_transport])()

def add_command_line_options(parser):
  '''Add command line options for all available transport layer classes.'''
  for transport in all_transports.itervalues():
    transport().add_command_line_options(parser)

class CommonTransport():
  '''A common transport class, containing e.g. the logic to connect clients
     to message subscriptions and transactions, so that these can be cleanly
     terminated when the client goes away.'''
