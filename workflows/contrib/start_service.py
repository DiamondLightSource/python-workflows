from __future__ import absolute_import, division
import sys
import workflows
import workflows.services
import workflows.transport
import workflows.frontend
from optparse import OptionParser, SUPPRESS_HELP

#
# Starts a workflows service
#

def run(cmdline_args, program_name = 'start_service',
        version=workflows.version()):
  '''Example command line interface to start services.
     :param cmdline_args: List of command line arguments to pass to parser
     :param program_name: Name of the command line tool to display in help
     :param version: Version number to print when run with '--version'
  '''
  parser = OptionParser(
    usage=program_name + ' [options]',
    version=version
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-s", "--service", dest="service", metavar="SVC",
      default=None, help="Name of the service to start. Known services: %s" % \
        ", ".join(workflows.services.get_known_services()))
  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
      default="StompTransport",
      help="Transport mechanism. Known mechanisms: %s (default: %%default)" % \
        ", ".join(workflows.transport.get_known_transports()))
  workflows.transport.add_command_line_options(parser)

  (options, args) = parser.parse_args(cmdline_args)

  workflows.frontend.Frontend(
      service=options.service,
      transport=options.transport,
    ).run()

if __name__ == '__main__':
  run(sys.argv[1:])

