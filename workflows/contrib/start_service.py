#
# Starts a workflows service
#

from __future__ import absolute_import
import sys

def run(cmdline_args):
  import workflows
  import workflows.transport
  import workflows.frontend
  from optparse import OptionParser, SUPPRESS_HELP

  parser = OptionParser(
    usage='start_service [options]',
    version=workflows.version()
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-s", "--service", dest="service", metavar="SVC",
      default=None, help="Name of the service to start, default '%default'")
  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
      default="stomp", help="Transport mechanism, default '%default'")
  workflows.transport.add_command_line_options(parser)
  (options, args) = parser.parse_args(cmdline_args)

  workflows.frontend.Frontend(
      service=options.service,
      transport=options.transport,
    ).run()

if __name__ == '__main__':
  run(sys.argv[1:])
