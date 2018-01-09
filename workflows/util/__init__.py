from __future__ import absolute_import, division, print_function

import os
import socket

def generate_unique_host_id():
  '''Generate a unique ID, that is somewhat guaranteed to be unique among all
     instances running at the same time.'''
  host = '.'.join(reversed(socket.gethostname().split('.')))
  pid = os.getpid()
  return "%s.%d" % (host, pid)
