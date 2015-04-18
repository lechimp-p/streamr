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

### Pipes

### General Stream Processors
