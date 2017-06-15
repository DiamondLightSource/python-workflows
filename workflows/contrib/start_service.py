from __future__ import absolute_import, division
from optparse import OptionParser, SUPPRESS_HELP
import workflows
import workflows.frontend
import workflows.services
import workflows.transport

class ServiceStarter(object):
  '''A helper class to start a workflows service from the command line.
     A number of hooks are provided so that this class can be subclassed and
     used in a number of scenarios.'''

  @staticmethod
  def on_parser_preparation(parser):
    '''Plugin hook to manipulate the OptionParser object before command line
       parsing. If a value is returned here it will replace the OptionParser
       object.'''

  @staticmethod
  def on_parsing(options, args):
    '''Plugin hook to manipulate the command line parsing results.
       A tuple of values can be returned, which will replace (options, args).
       '''

  @staticmethod
  def on_transport_preparation(transport):
    '''Plugin hook to intercept/manipulate the Transport object before passing
       it to the Frontend object.'''

  @staticmethod
  def before_frontend_construction(kwargs):
    '''Plugin hook to manipulate the Frontend object constructor arguments. If
       a value is returned here it will replace the keyword arguments
       dictionary passed to the constructor.'''

  @staticmethod
  def on_frontend_preparation(frontend):
    '''Plugin hook to manipulate the Frontend object before starting it. If a
       value is returned here it will replace the Frontend object.'''

  def run(self, cmdline_args=None, program_name='start_service',
        version=workflows.version(), **kwargs):
    '''Example command line interface to start services.
       :param cmdline_args: List of command line arguments to pass to parser
       :param program_name: Name of the command line tool to display in help
       :param version: Version number to print when run with '--version'
    '''

    # Enumerate all known services
    known_services = workflows.services.get_known_services()

    # Set up parser
    parser = OptionParser(
      usage=program_name + ' [options]' if program_name else None,
      version=version
    )
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option("-s", "--service", dest="service", metavar="SVC",
      default=None, help="Name of the service to start. Known services: " + \
        ", ".join(known_services))
    parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
      default="StompTransport",
      help="Transport mechanism. Known mechanisms: " + \
           ", ".join(workflows.transport.get_known_transports()) + \
           " (default: %%default)")
    workflows.transport.add_command_line_options(parser)

    # Call on_parser_preparation hook
    parser = self.on_parser_preparation(parser) or parser

    # Parse command line options
    (options, args) = parser.parse_args(cmdline_args)

    # Call on_parsing hook
    (options, args) = self.on_parsing(options, args) or (options, args)

    # Create Transport object
    transport = workflows.transport.lookup(options.transport)()

    # Call on_transport_preparation hook
    transport = self.on_transport_preparation(transport) or transport

    # When service name is specified, check if service exists or can be derived
    if options.service and options.service not in known_services:
      matching = [ s for s in known_services if s.startswith(options.service) ]
      if not matching:
        matching = [ s for s in known_services
                     if s.lower().startswith(options.service.lower()) ]
      if matching and len(matching) == 1:
        options.service = matching[0]

    kwargs.update({ 'service': options.service,
                    'transport': transport })

    # Call before_frontend_construction hook
    kwargs = self.before_frontend_construction(kwargs) or kwargs

    # Create Frontend object
    frontend = workflows.frontend.Frontend(**kwargs)

    # Call on_frontend_preparation hook
    frontend = self.on_frontend_preparation(frontend) or frontend

    # Start Frontend
    try:
      frontend.run()
    except KeyboardInterrupt:
      print("\nShutdown via Ctrl+C")

if __name__ == '__main__':  # pragma: no cover
  ServiceStarter().run()
