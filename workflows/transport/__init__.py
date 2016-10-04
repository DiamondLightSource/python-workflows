from __future__ import division

import workflows.transport.stomp

def lookup(transport):
  if transport is not None and \
    transport.lower() == "stomp":
      return workflows.transport.stomp.Transport
  # fallback to default
  return workflows.transport.stomp.Transport

def add_command_line_options(parser):
  workflows.transport.stomp.Transport().add_command_line_options(parser)
