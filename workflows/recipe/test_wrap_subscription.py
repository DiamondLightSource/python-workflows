from __future__ import absolute_import, division
import mock
import pytest
import workflows
import workflows.recipe

def check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock):
  '''Test callback function of a recipe wrapper.'''

  # This message does not contain an encoded recipe. It should be passed through directly.
  header = { 'random-header': mock.sentinel.ID }
  message = mock.Mock()
  recipient.reset_mock()
  callback(header, message)
  recipient.assert_called_once_with(None, header, message)

  # This message contains an encoded recipe. It should be interpreted and the payload passed
  # through with a helper object for simple recipe-conformant replies.
  header = { 'workflows-recipe': True }
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
  callback(header, message)

  recipient.assert_called_once_with(rw_mock.return_value, header, message['payload'])
  rw_mock.assert_called_once_with(message)

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
  check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock)

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
  check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock)
