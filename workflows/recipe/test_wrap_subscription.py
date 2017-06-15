from __future__ import absolute_import, division
import mock
import pytest
import workflows
import workflows.recipe

def check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock, allow_non_recipe, log=None):
  '''Test callback function of a recipe wrapper.'''

  # This message does not contain an encoded recipe. It should be passed through directly.
  header = { 'random-header': mock.sentinel.ID }
  message = mock.Mock()
  recipient.reset_mock()
  transport.reset_mock()
  callback(header, message)
  if allow_non_recipe:
    recipient.assert_called_once_with(None, header, message)
    transport.nack.assert_not_called()
  else:
    transport.nack.assert_called_once_with(header)
    recipient.assert_not_called()

  # This message does not contain an encoded recipe. It should be passed through directly.
  header = { 'workflows-recipe': "False" }
  recipient.reset_mock()
  transport.reset_mock()
  callback(header, message)
  if allow_non_recipe:
    recipient.assert_called_once_with(None, header, message)
    transport.nack.assert_not_called()
  else:
    transport.nack.assert_called_once_with(header)
    recipient.assert_not_called()

  # This message contains an encoded recipe. It should be interpreted and the payload passed
  # through with a helper object for simple recipe-conformant replies.
  header = { 'workflows-recipe': "True" }
  message = {
     'recipe': mock.sentinel.recipe,
     'recipe-pointer': mock.sentinel.recipe_pointer,
     'recipe-path': [],
     'environment': {
                 'ID': mock.sentinel.GUID,
                 'source': mock.sentinel.source,
                 'timestamp': mock.sentinel.timestamp,
               },
     'payload': mock.sentinel.payload,
     }
  recipient.reset_mock()
  transport.reset_mock()
  callback(header, message)

  recipient.assert_called_once_with(rw_mock.return_value, header, message['payload'])
  rw_mock.assert_called_once_with(message=message, transport=transport, log=log)
  transport.nack.assert_not_called()

@mock.patch('workflows.recipe.RecipeWrapper', autospec=True)
def test_wrapping_a_subscription(rw_mock):
  '''Test queue subscription with recipe wrapper.'''
  transport, recipient = mock.Mock(), mock.Mock()

  workflows.recipe.wrap_subscribe(transport, mock.sentinel.channel, recipient,
      mock.sentinel.irrelevant_extra_arg, keyword=mock.sentinel.keyword_arg)

  # Channel and any extra arguments must be passed on to transport layer.
  # Callback function will obviously change.
  transport.subscribe.assert_called_once_with(mock.sentinel.channel, mock.ANY,
      mock.sentinel.irrelevant_extra_arg, keyword=mock.sentinel.keyword_arg)
  callback = transport.subscribe.call_args[0][1]
  assert callback != recipient

  # Part II: Message handling via unwrapper
  check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock, False)

@mock.patch('workflows.recipe.RecipeWrapper', autospec=True)
def test_wrapping_a_subscription_allowing_non_recipe_messages(rw_mock):
  '''Test queue subscription with recipe wrapper allowing non-recipe messages to pass through.'''
  transport, recipient = mock.Mock(), mock.Mock()

  workflows.recipe.wrap_subscribe(transport, mock.sentinel.channel, recipient,
      mock.sentinel.irrelevant_extra_arg, keyword=mock.sentinel.keyword_arg,
      allow_non_recipe_messages=True)

  transport.subscribe.assert_called_once()
  callback = transport.subscribe.call_args[0][1]

  # Part II: Message handling via unwrapper
  check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock, True)

@mock.patch('workflows.recipe.RecipeWrapper', autospec=True)
def test_wrapping_a_subscription_with_logger(rw_mock):
  '''Test queue subscription with recipe wrapper, passing a python logger.'''
  transport, recipient, logger = mock.Mock(), mock.Mock(), mock.Mock()

  workflows.recipe.wrap_subscribe(transport, mock.sentinel.channel, recipient,
      log=logger)

  # Channel and any extra arguments must be passed on to transport layer.
  # Callback function will obviously change.
  transport.subscribe.assert_called_once_with(mock.sentinel.channel, mock.ANY)
  callback = transport.subscribe.call_args[0][1]
  assert callback != recipient

  # Part II: Message handling via unwrapper
  check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock, False, log=logger)

@mock.patch('workflows.recipe.RecipeWrapper', autospec=True)
def test_wrapping_a_broadcast_subscription(rw_mock):
  '''Test topic subscription with recipe wrapper.'''
  transport, recipient = mock.Mock(), mock.Mock()

  workflows.recipe.wrap_subscribe_broadcast(transport, mock.sentinel.channel, recipient,
      mock.sentinel.irrelevant_extra_arg, keyword=mock.sentinel.keyword_arg)

  # Channel and any extra arguments must be passed on to transport layer.
  # Callback function will obviously change.
  transport.subscribe_broadcast.assert_called_once_with(mock.sentinel.channel, mock.ANY,
      mock.sentinel.irrelevant_extra_arg, keyword=mock.sentinel.keyword_arg)
  callback = transport.subscribe_broadcast.call_args[0][1]
  assert callback != recipient

  # Part II: Message handling via unwrapper
  check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock, False)
