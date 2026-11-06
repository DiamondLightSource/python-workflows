## Recipes

A [service](../services/README.MD) listens on a [channel](../transport/README.MD), acts on incoming messages, and produces some output.
The place where this output is sent to could be hardcoded, ie. written into the source code of the service.
This makes the service structure rigid, and connecting one service to the output of another requires coordinating the source code of both services.
Naturally, this approach does not scale well.

Recipes are a more dynamical approach to encode the connection between services.
The assumption is that services are connected in a [directed acyclic graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph):

[![a directed acyclic graph](https://upload.wikimedia.org/wikipedia/commons/c/c6/Topological_Ordering.svg)](https://commons.wikimedia.org/wiki/File%3ATopological_Ordering.svg)

A DAG consists of nodes and directed edges.
Nodes correspond to services, and the edges correspond to connections between the services.
There must be no loops between<sup>1</sup> the services, so a later node can not send output to an earlier node.

Nodes may have more than one input.
This would be the case is a service listens on one channel and accepts data coming in from a number of different services.

Nodes can have no output, one output, or many outputs.
A node with no output represents the end of a processing chain. Once this step is completed nothing follows.
When there are multiple outputs these may be equivalent, so identical data is sent to multiple downstream services.
There can also be different named output streams, which may be producing different data, or conditional output depending on the input to the service.
The available output streams are service-specific, as of course is the content of outgoing messages and the circumstances in which each output stream sends messages.

#### Dictionary representation

In a recipe the following convention is used:
Each node (service) is represented as a numeric integer.
One special node, named ```start```, points to the first node and includes the data that is to be sent to the first node.

A recipe can then be represented as a dictionary:
```
{
  1: { (..), 'output': 2, (..) },
  2: { (..), 'output': 3, (..) },
  3: { (..) },
   (..)
  'start': [ (1, 'some data'), (2, { 'this can also be': 'a data structure' }) ]
}
```
This recipe describes 3 services, which are connected as follows:
* Service 1 will be sent the string 'some data'.
* The output of service 1 will be sent to service 2.
* Service 2 will, independently and in a separate message, receive the dictionary shown above.
* The output of service 2 will be sent to service 3. Since service 2 receives two messages this may mean that, depending on the service implementation, two output messages are generated here.
* The output of service 3, if the service generates any, is to be discarded.

#### Node (service) definitions

## Recipe-wrapping messages

Messages including recipes have the following format:
* The message header includes a field ```workflows-recipe```, which is set to ```True```.
* The message content is a dictionary which has to contain the following entries:
  * ```environment```: A dictionary containing recipe-wide key-value pairs. This can be empty.
  * ```payload```: Any data structure, but usually a dictionary. This is the message content for the targeted processing node.
  * ```recipe```: A dictionary representation of the processing recipe.
  * ```recipe-path```: An ordered list of numerical node identifiers, representing the path through the recipe that resulted in the current message. The list is ordered historically, from the oldest to the most recent node. The ```start``` node is not included in the list. The list can be empty.
  * ```recipe-pointer```: The numerical identifier of the node that this message is directed to.



#### Footnotes
1. There must not be any loops between services. A service may, however, appear multiple times in the graph. Local loops are also allowed: these are loops from one service directly to itself. Each service decides when to send data on to the next service. Sending data to oneself, in effect delaying the processing workflow, is equivalent to simply staying at the same node in the recipe model, and therefore permitted.
