import workflows
from workflows.transport.common_transport import CommonTransport

class PikaTransport(CommonTransport):

    @classmethod
    def load_configuration_file(cls, filename):
        pass

    @classmethod
    def add_command_line_options(cls, parser):
        pass

    @classmethod
    def add_command_line_options_argparse(cls, argparser):
        pass

    @classmethod
    def add_command_line_options_optparse(cls, optparser):
        pass
