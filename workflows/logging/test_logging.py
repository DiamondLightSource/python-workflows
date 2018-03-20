from __future__ import absolute_import, division, print_function

import logging
import os

import mock
import workflows.logging

def test_obtain_exception_source():
  '''Check that exception information can be obtained.'''

  def crashfunction():
    '''A function that crashes in a predictable manner.'''
    everything = 42
    nothing = 0
    print(everything / nothing)

  try:
    crashfunction()
    assert False, "Test went past exception."
  except Exception:
    fp, fn, line_number, name, line = workflows.logging.get_exception_source()

    assert fp == __file__
    assert os.path.basename(fp) == fn
    assert name == 'crashfunction'
    assert line_number == 16 # this will break if line number in this file changes
    assert line == "print(everything / nothing)"

def test_callback_handler_works_within_logging_framework():
  '''Check that the callback handler can be used by Python logging
     and works as expected.'''
  cbmock = mock.Mock()
  logmsg = 'Test message for callback'
  log = logging.getLogger('workflows.tests.logging.callback')
  log.setLevel(logging.INFO)

  cbh = workflows.logging.CallbackHandler(cbmock)
  cbh.handleError = mock.Mock()
  log.addHandler(cbh)
  log.info(logmsg)

  cbmock.assert_called_once()
  assert cbmock.call_args == ((mock.ANY,), {})
  logrec = cbmock.call_args[0][0]
  assert isinstance(logrec, logging.LogRecord)
  assert logrec.name      == 'workflows.tests.logging.callback'
  assert logrec.levelname == 'INFO'
  assert logrec.message   == logmsg
  assert logrec.funcName.startswith('test_')
  assert not cbh.handleError.called

  # Now check that the callback handler can handle errors in the
  # callback function.
  logmsg = 'Test message for error in logging'
  cbmock.side_effect=AttributeError('Some failure')

  log.info(logmsg)

  assert cbmock.call_count == 2
  assert cbmock.call_args == ((mock.ANY,), {})
  logrec = cbmock.call_args[0][0]
  assert isinstance(logrec, logging.LogRecord)
  assert logrec.message == logmsg
  cbh.handleError.assert_called_once()
  assert cbh.handleError.call_args == cbmock.call_args
