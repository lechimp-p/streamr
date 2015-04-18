[![Build Status](https://travis-ci.org/lechimp-p/streamr.svg?branch=master)](https://travis-ci.org/lechimp-p/streamr)
[![Code Health](https://landscape.io/github/lechimp-p/streamr/master/landscape.svg?style=flat)](https://landscape.io/github/lechimp-p/streamr/master)

# streamr

**A stream abstraction for Python.**

streamr lets you express your data processing as a pipeline of stream processors.
With streamr you can program the flow of your data through different parts of
your overall process rather than telling the interpreter when to shuffle which
data where. streamr lets you compose complex data processing pipelines from 
simple and reusable components. 

## Stream Processors

A stream processor is the basic abstraction of this library. It can take data 
from upstream, do some stuff with it and send data downstream. It also can
request some parameter for initialisation and produce a result. Thus, schematically
it looks like this:

```
                   params for init 
                          |
                          v
                  -----------------
                  |               |
                  |    stream     |
      upstream ==>|               |==> downstream
                  |   processor   |
                  |               |
                  -----------------
                          |
                          v 
               result after processing
```

The upstream <-> downstream axis is how the data flows through the processor, 
while the init <-> result axis represents the lifecycle of a processor.

The inputs and outputs of stream processors are typed to catch errors during
construction of processes. Currently the type system can work with python types,
tuple types, list types, function types and type variables. A processor thus
could be pictured like this with type annotations:

```
         RegexObject
             v
           -----
    str => |   | => [int] 
           -----
             v
            int 
``` 

This for example pictures a processor that is inited with a regular expression,
takes strings from upstream and sends lists of ints downstream. As result it
produces another int. A processor of this type could e.g. search with the regex
in the string from upstream and return every position the regex matches. The
result could be the total amount of matches during the lifetime of the processor.
A textual representation for that processor would be `RegexObject -> int % str 
-> [int]`.

## Combinators

Stream processor can be combined to new stream processors by two basic 
combinators. The combinations can be treated uniformly with the basic processors.

### Sequential Combination

Sequential combination `a >> b` connects the downstream of one processor to the 
upstream of another processors, where the types of both need to match. The new 
processor is inited with a tuple of params and results in a tuple as well:
`(i -> r % a -> b) >> (i' -> r' % b -> c) = ((i,i') -> (r,r') % a -> c)`.

```
    i             i'         (i,i')
    v             v            v
 a =O=> b  >>  b =O=> c  =  a =O=> c 
    v             v            v
    r             r'         (r,r')
```

### Parallel Combination

Parallel combination `a * b` creates a stream processor where the upstream and
downstreams is made of tuples of the types of the initial processors. The init
and result is treated analogous to sequential combination:
`(i -> r % a -> b) * (i' -> r' % c -> d) = ((i,i') -> (r,r') % (a,b) -> (c,d)`.

```
    i             i'             (i,i')
    v             v                v
 a =O=> b  *  c  =O=> d  =  (a,b) =O=> (c,d) 
    v             v                v
    r             r'             (r,r')
```

## Some Building Blocks

### Producers and Consumers

* `from_list([1,2,3])` takes no values from upstream and sends the list it
  got downstream one after another (it is of type `() -> () % () -> int`). 
  `from_list` could also be used like from_list(item_type = str)`, which
  expects to get a list [str] as init param: `[a] -> () % () -> a`. 
* `const("foo")` constantly sends "foo" downstream. Like `from_list` it could
  also be used in a flavour where the value is retrieved from init:
  `const(value_type = "str")` is of type `a -> () % () -> a`. In both flavours
  it supports an `amount`-parameter to send a maximum amount of values 
  downstream.
* `to_list()` is a consumer that appends values from upstream to a list `() -> [a]
  % a -> ()`. As `to_list(my_list)` it appends to a given list `() -> () % a -> ()`.
  In both flavours it supports the max_amount flag to specify how many values
  the consumer should consume before it stops. 

### Decorators

* `@pipe(type_in, type_out)` is used to decorate a function `f(await, send)` to
  become a pipe `() -> () % type_in -> type_out`. The function `f` can retreive
  values from upstream via `await` and `send` values downstream.
* `@transformation(type_in, type_out)` decorates functions `f(a,b)` that turn a
  value a of type type_in to a value b of type_out to become a pipe `() -> () % 
  type_in -> type_out`

### Miscellaneous  

* `pass_if(type_io, predicate)` filters an upstream of type_io in such a way, 
  that only values that comply to predicate will be passed downstream. Could
  also be used as a decoraror `@passif(type_io)`.
* `tee()` turns one value in to values like `a -> (a,a)` distributing the same
  value to two downstream processors.
* `nop()` does nothing at all with values from upstream before passing them 
   downstream. It comes in handy when composing complex stream processors.
* `maps(pipe)` transforms a pipe `i -> r % a -> b` to a pipe `i -> [r] % 
  [a] -> [b]`, thus maps the pipe over lists from upstream.

## Running Processes

A stream process is runnable if it has the form `a -> b % () -> ()`. The ()-type
aka unit denotes, that nothing interesting is out- or inputted, thus processors
of the above form take initial params and produce a result but do not send data 
downstream or take data from upstream. We can run such a processor by providing
it with its desired params and get back a result:

```python
p = from_list(item_type = int) >> to_list()
assert p.run([1,2,3]) == [1,2,3]
```

### Subprocesses

A processor of the form `a -> b % () -> ()` could be turned into a stream processor 
with no init and result with subprocess:`subprocess(a -> b % () -> ()) = (() -> () 
% a -> b)`.

```
                 a             ()
                 v              v
 subprocess( () =O=> () ) =  a =O=> b
                 v              v
                 b             ()
```

This can be used to embed runnable stream processors into other higher level pipelines.

## Implementing Processors

For the implementation of new processors, four base classes are provided, where
three of those classes capture typical types of stream processors and the fourth
base class gives access to the full stream processor abstractions.

### Producers

Producers are stream processors of type `i -> () % () -> b`. The can be implemented
with the `Producer` base class:

```python

from streamr import Producer, Stop, MayResume, to_list
import sys

class AllIntsLargerThan(Producer):
    """
    This is a producer that produces all ints starting at an int supplied
    as init parameter. 
    """
    def __init__(self):
        # Specify the init type and the type of the produced values.
        super(AllIntsLargerThan, self).__init__(int, int)

    # A setup method can be implemented for every stream processor. It is
    # called when the processor starts to execute. The return value of the
    # setup will be passed as env to every call to the produce method.
    # Since a stream processor should be composable it should capture changes
    # during processing in an extra environment and not in the state of the
    # object. 
    def setup(self, params, result):
        # * params is a value of init type
        # * result is a function that could be used to set a result for the
        #   processor. As our Producer has no result, we should not use it.

        # Call overwritten setup function to do type checking on the params.
        super(AllIntsLargerThan, self).setup(params, result)
        
        # As our init type is int and we want to produce all integers larger
        # than our init, we use the initial value as mutable environment. 
        return [params[0]]

    # The produce method will be called, when a downstream processor needs more
    # values. It can use the send method, to send values downstream.
    def produce(self, env, send): 
        # Send the next value.
        send(env[0])
        # Set next value to be send.
        env[0] += 1

        if sys.version_info[0] != 3 and env[0] == sys.maxint:
            # No we definetly can't produce any more integers, so we need to
            # signal that we want to stop:
            return Stop

        # We can go one, but we do not need to.
        return MayResume
        
        # We could also return Resume to signal that we need to go on.
        
    # For some processors (e.g. those which use resources) it could be necessary
    # to implement a teardown method. It will be called, after the processor 
    # finished executing, getting the environment previously produce by setup.
    # def teardown(self, env):
    #   pass

# As our processor only needs to exist once, we could as well create an instance
# of it, that could be used anywhere.
all_ints_larger_than = AllIntsLargerThan()

sp = all_ints_larger_than >> to_list(max_amount = 100)
assert sp.run(10) == range(10, 110)

```

### Consumers

A consumer is a stream processor that collects values from upstream, without
producing new values for downstream: `() -> r % a -> ()`.

```python
from streamr import Consumer, MayResume, from_list
from streamr.types import Type

class Prints(Consumer):
    """
    A consumer that prints all values it retreives from upstream and returns
    the total amount of values it has seen as a result.
    The value will be inserted in a string given as init parameter.
    """
    def __init__(self):
        # As with the producer, we need to specify the types of the processor.
        # As we do not want to restrict on the upstream type, we use a type
        # variable:
        _any = Type.get()
        # init type, result tyoe and consumed type.
        super(Prints, self).__init__(str, int, _any)

    def setup(self, params, result):
        # We did not see any values recently.
        result(0)
        # We need to capture the string and a mutable amount in our environment.
        return (params[0], [0])
    
    # This method will be called when we should consume a new value from 
    # upstream.
    def consume(self, env, await, result):
        val = await()
        # We saw a new value...
        env[1][0] += 1
        result(env[1][0])
        print(env[0] % val)
        return MayResume

prints = lambda x: Prints()

sp = from_list(range(0,10)) >> prints()
assert sp.run("Got value: %s") == 10
```

### Pipes

Pipes are stream processors that produce no result, take values from upstream
and send values downstream: `i -> () % a -> b`.

```python
from streamr import Pipe, MayResume, from_list, to_list
from streamr.types import Type

class Echo(Pipe):
    """
    A pipe that echos values it retreives from upstream for a certain times,
    given as init.
    """
    def __init__(self):
        # We need to pass an init type, an upstream type and a downstream type
        _any = Type.get()
        super(Echo, self).__init__(int, _any, _any)

    def setup(self, params, result):
        return params[0]

    def transform(self, env, await, send):
        val = await()
        for _ in range(0, env):
            send(val)
        return MayResume

echo = lambda: Echo()

sp = from_list([1,2]) >> echo() >> to_list() 
assert sp.run(2) == [1,1,2,2]
``` 

### General Stream Processors

A general stream processor of type `i -> r % a -> b` can be implemented via the
`StreamProcessor` base class.

```python
from streamr import StreamProcessor, MayResume, from_list, to_list

import re

class SearchWithRegex(StreamProcessor):
    """
    Give a regular expression as init value. Take strings from upstream
    and send the number the regex matched on the string downstream. Provide
    the total amount of matches a result.

    Regex -> int % string -> int
    
    This is the processor from the typed example above.
    """
    def __init__(self):
        re_type = type(re.compile(".")) 
        super(SearchWithRegex, self).__init__(re_type, int, str, int)

    def setup(self, params, result):
        result(0)
        return (params[0], [0])

    def step(self, env, stream):
        # Stream is an object that provides the methods await, send and result. 
        string = stream.await()
        amount = len(env[0].findall(string))
        env[1][0] += amount
        stream.result(env[1][0])
        stream.send(amount)
        return MayResume

searchWithRegex = SearchWithRegex()

sp = from_list(["1a33efg", "1", "a"]) >> searchWithRegex >> to_list()
assert sp.run(re.compile("\d")) == (4, [3,1,0])
```

### Contribution

I would be very pleased to get your contributions to this library, either in 
form of general of specific feedback, bug reports or pull requests. I would be 
glad to put some of your stream processors in my library. I'm also interested
in suggestions which direction this library should take (if it is at all 
usefull).

### Outlook

At the moment this library is in kind of a proof of concepts stage. If someone
consideres this library usefull, i could think of some interesting directions
this could take:

* I wanted the interface of the stream processors to be abstract enough to make
  the runtime engines behind the processing switchable. The currently implemented
  simple runtime uses a pull based approach and caches and processes all steps
  sequentially. It should be easy to implement a thread based runtime as well.
  With some more effort it should also be possible to implement a runtime that
  uses multiple processes, potentially on different machines.
* The expression of types is a bit clunky at the moment. With Python3 annotations
  this could be much nicer. There are also some other things that could be
  improved in the typing, e.g. type variables or introduction of sum types.
* There are a lot of processors one could imagine. At the moment there are only
  some very basic processors. That makes a lot open space for improvements...

# Acknowledgments

This library is heavily inspired by the (pipes library)[https://github.com/Gabriel439/Haskell-Pipes-Library]
from Gabriel Gonzalez for Haskell. This most probably also is the reason for
the use of types for the pipeline.

A little discussion with (vortec)[https://github.com/vortec] inspired me to
implement this library.
