from __future__ import absolute_import, division
import mock
import pytest
import workflows.contrib.start_service

def test_get_command_line_help(capsys):
  '''Running the start_service script with --help should display command line help and exit.'''
  with pytest.raises(SystemExit):
    workflows.contrib.start_service.ServiceStarter().run(['--help'], program_name='sentinelvalue')
  out, err = capsys.readouterr()
  assert 'Usage: sentinelvalue' in out

@mock.patch('workflows.contrib.start_service.OptionParser')
@mock.patch('workflows.contrib.start_service.workflows.transport.lookup')
@mock.patch('workflows.contrib.start_service.workflows.frontend')
@mock.patch('workflows.contrib.start_service.workflows.services')
def test_script_initialises_transport_and_starts_frontend(mock_services, mock_frontend, mock_tlookup, mock_parser):
  '''Check that the start_service script sets up the transport mechanism and the frontend properly.
     Correct service should be selected and the frontend started.'''
  mock_options = mock.Mock()
  mock_options.service = 'someservice'
  mock_options.transport = mock.sentinel.transport
  mock_parser.return_value.parse_args.return_value = (mock_options, mock.Mock())
  mock_services.get_known_services.return_value = { 'SomeService': None }

  workflows.contrib.start_service.ServiceStarter().run(cmdline_args=['-s', 'some'], version=mock.sentinel.version)

  mock_tlookup.assert_called_once_with(mock.sentinel.transport)
  mock_parser.assert_called_once_with(usage=mock.ANY, version=mock.sentinel.version)
  mock_frontend.Frontend.assert_called_once_with(service='SomeService', transport=mock_tlookup.return_value.return_value)
  mock_frontend.Frontend.return_value.run.assert_called_once_with()
