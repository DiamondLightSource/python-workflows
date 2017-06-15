from __future__ import absolute_import, division
from workflows.recipe.recipe import Recipe
from workflows.recipe.wrapper import RecipeWrapper

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

       :allow_non_recipe_messages: Pass on incoming messages that do not
                        include recipe information. In this case the first
                        argument to the callback function will be 'None'.
       :log:            A python logger object that is passed to the
                        RecipeWrapper and may provide additional,
                        message-specific context to log messages.
  '''

  allow_non_recipe_messages = kwargs.pop('allow_non_recipe_messages', False)
  log = kwargs.pop('log', None)

  def unwrap_recipe(header, message):
    '''This is a helper function unpacking incoming messages when they are
       in a recipe format. Other messages are passed through unmodified.
       :param header:  A dictionary of message headers. If the header contains
                       an entry 'workflows-recipe' then the message is parsed
                       and the embedded recipe information is passed on in a
                       RecipeWrapper object to the target function.
       :param message: Incoming deserialized message object.
    '''
    if header.get('workflows-recipe') in (True, 'True', 'true', 1):
      return callback(RecipeWrapper(message=message,
                                    transport=transport_layer,
                                    log=log),
                      header, message.get('payload'))
    if allow_non_recipe_messages:
      return callback(None, header, message)

#   self.log.warn('Discarding non-recipe message:\n' + \
#                 "First 1000 characters of header:\n%s\n" + \
#                 "First 1000 characters of message:\n%s",
#                 str(header)[:1000], str(message)[:1000])
    transport_layer.nack(header)

  subscription_call(channel, unwrap_recipe, *args, **kwargs)

def wrap_subscribe(transport_layer, channel, callback, *args, **kwargs):
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

def wrap_subscribe_broadcast(transport_layer, channel, callback, *args, **kwargs):
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
