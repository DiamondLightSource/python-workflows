from __future__ import absolute_import, division

def _wrap_subscription(transport_layer, subscription_call, channel, callback,
    *args, **kwargs):
  '''Internal method to create an intercepting function for incoming messages
     to interpret recipes. This function is then used to subscribe to a channel
     on the transport layer.
       :param transport_layer: Reference to underlying transport object.
       :param subscription_call: Reference to the subscribing function of the
                                 transport layer.
       :param channel:  Channel name to subscribe to.
       :param callback: Real function to be called when messages are received.
                        The callback will pass three arguments,
                        a RecipeWrapper object (details below), the header as
                        a dictionary structure, and the message.
  '''

  def unwrap_recipe(header, message):
    if header.get('workflows-recipe', False) and 'payload' in message:
      return callback(1, header, message['payload'])
    return callback(None, header, message)

  subscription_call(channel, unwrap_recipe, *args, **kwargs)

def subscribe(transport_layer, channel, callback, *args, **kwargs):
  '''Listen to a queue on the transport layer, similar to the subscribe call in
     transport/common_transport.py. Intercept all incoming messages and parse
     for recipe information.
     See common_transport.subscribe for possible additional keyword arguments.
       :param transport_layer: Reference to underlying transport object.
       :param channel:  Queue name to subscribe to.
       :param callback: Function to be called when messages are received.
                        The callback will pass three arguments,
                        a RecipeWrapper object (details below), the header as
                        a dictionary structure, and the message.
  '''

  _wrap_subscription(transport_layer, transport_layer.subscribe,
                     channel, callback, *args, **kwargs)

def subscribe_broadcast(transport_layer, channel, callback, *args, **kwargs):
  '''Listen to a topic on the transport layer, similar to the
     subscribe_broadcast call in transport/common_transport.py. Intercept all
     incoming messages and parse for recipe information.
     See common_transport.subscribe_broadcast for possible arguments.
       :param transport_layer: Reference to underlying transport object.
       :param channel:  Topic name to subscribe to.
       :param callback: Function to be called when messages are received.
                        The callback will pass three arguments,
                        a RecipeWrapper object (details below), the header as
                        a dictionary structure, and the message.
  '''

  _wrap_subscription(transport_layer, transport_layer.subscribe_broadcast,
                     channel, callback, *args, **kwargs)

#class RecipeWrapper(object):
  # TODO: tbd
