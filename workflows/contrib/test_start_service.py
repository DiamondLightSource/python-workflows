from __future__ import absolute_import, division
import pytest
import workflows.contrib.start_service

def test_get_command_line_help(capsys):
  with pytest.raises(SystemExit):
    workflows.contrib.start_service.run(['--help'])
  out, err = capsys.readouterr()
  assert 'Usage: start_service' in out
