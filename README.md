[![Build Status](https://travis-ci.org/lechimp-p/streamr.svg?branch=master)](https://travis-ci.org/lechimp-p/streamr)
[![Code Health](https://landscape.io/github/lechimp-p/streamr/master/landscape.svg?style=flat)](https://landscape.io/github/lechimp-p/streamr/master)

# streamr

***A stream abstraction for Python.***

streamr lets you express your data processing as a pipeline of stream processors.
With streamr you can program the flow of your data through different parts of
your overall process rather than telling the interpreter when to shuffle which
data where. streamr lets you compose complex data processing pipelines from 
simple and reusable parts. 

## Stream Processors

A stream processor is the basic abstraction of this library. It can take data 
from upstream, do some stuff with it and send data downstream. It also can
request some parameter for initialisation and produce a result. Thus, schematically
it looks like this:

```
                   params for init 
                          |
                          V
                  -----------------
                  |               |
                  |    stream     |
      upstream ==>|               |==> downstream
                  |  processors   |
                  |               |
                  -----------------
                          |
                          V 
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
             V
           -----
    str => |   | => [int] 
           -----
             V
            int 
``` 

This for example pictures a processor that is inited with a regular expression,
takes strings from upstream and sends lists of ints downstream. As result it
produces another int. A processor of this type could e.g. search with the regex
in the string from upstream and return every position the regex matches. The
result could be the total amount of matches during the lifetime of the processor.


