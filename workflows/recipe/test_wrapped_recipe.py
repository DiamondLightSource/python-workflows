from __future__ import absolute_import, division
import mock
import pytest
import workflows
from workflows.recipe import Recipe
from workflows.recipe.wrapper import RecipeWrapper

def generate_recipe_message():
  '''Helper function for tests.'''
  message = {
     'recipe': {
       1: { 'service': 'A service',
            'queue': 'service.one',
            'output': [ 2 ],
            'multi-output': [ 2, 3, 4 ],
            'error': 2,
          },
       2: { 'service': 'service 2',
            'queue': 'queue.two',
          },
       3: { 'service': 'service 3',
            'topic': 'topic.three',
            'transport-delay': 300,
          },
       4: { 'service': 'service 4',
            'queue': 'queue.four',
            'transport-delay': 300,
          },
       'start': [
          (1, {}),
       ],
       'error': [ 2 ],
     },
     'recipe-pointer': 1,
     'recipe-path': [],
     'environment': { 'ID': mock.sentinel.GUID,
                      'source': mock.sentinel.source,
                      'timestamp': mock.sentinel.timestamp,
                    },
     'payload': mock.sentinel.payload,
    }
  return message


def test_recipe_wrapper_extracts_message_information():
  m = generate_recipe_message()

  rw = RecipeWrapper(m)

  assert rw.recipe == Recipe(m['recipe'])
  assert rw.recipe_pointer == m['recipe-pointer']
  assert rw.recipe_step == m['recipe'][m['recipe-pointer']]
  assert rw.recipe_path == m['recipe-path']
  assert rw.environment == m['environment']

def test_recipe_embedded_message_sending():
  m = generate_recipe_message()
  t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
  def downstream_recipe(dest, payload):
    ds_message = generate_recipe_message()
    ds_message['recipe-pointer'] = dest
    ds_message['recipe-path'] = [ 1 ]
    ds_message['payload'] = payload
    return ds_message

  rw = RecipeWrapper(m, transport=t)
  assert rw.transport == t
  assert t.method_calls == []
  t.reset_mock() # magic call may have been recorded

  with pytest.raises(ValueError):
    rw.send_to('unknown', mock.sentinel.message_text)

  rw.send_to('output', mock.sentinel.message_text)

  expected = [ mock.call.send(
      m['recipe'][2]['queue'],
      downstream_recipe(2, mock.sentinel.message_text),
      header={'workflows-recipe': True},
  ) ]
  assert t.mock_calls == expected
  t.reset_mock()

  rw.send_to('multi-output', mock.sentinel.another_message_text, transaction=mock.sentinel.txn)

  expected = []
  expected.append(mock.call.send(
      m['recipe'][2]['queue'],
      downstream_recipe(2, mock.sentinel.another_message_text),
      header={'workflows-recipe': True},
      transaction=mock.sentinel.txn,
  ))
  expected.append(mock.call.broadcast(
      m['recipe'][3]['topic'],
      downstream_recipe(3, mock.sentinel.another_message_text),
      header={'workflows-recipe': True},
      transaction=mock.sentinel.txn,
      delay=300,
  ))
  expected.append(mock.call.send(
      m['recipe'][4]['queue'],
      downstream_recipe(4, mock.sentinel.another_message_text),
      header={'workflows-recipe': True},
      transaction=mock.sentinel.txn,
      delay=300,
  ))
  assert t.mock_calls == expected
