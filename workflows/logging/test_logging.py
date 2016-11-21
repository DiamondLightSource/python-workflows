from __future__ import absolute_import, division
import logging
import mock
import workflows.logging

def test_callback_handler_works_within_logging_framework():
  '''Check that the callback handler can be used by Python logging
     and works as expected.'''
  cbmock = mock.Mock()
  logmsg = 'Test message for callback'
  log = logging.getLogger('workflows.tests.logging.callback')
  log.setLevel(logging.INFO)

  cbh = workflows.logging.CallbackHandler(cbmock)
  log.addHandler(workflows.logging.CallbackHandler(cbmock))
  log.info(logmsg)

  cbmock.assert_called_once()
  assert cbmock.call_args == ((mock.ANY,), {})
  logrec = cbmock.call_args[0][0]
  assert isinstance(logrec, logging.LogRecord)
  assert logrec.name      == 'workflows.tests.logging.callback'
  assert logrec.levelname == 'INFO'
  assert logrec.message   == logmsg
  assert logrec.funcName.startswith('test_')
