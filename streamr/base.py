# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

"""
# Rationales

A streaming pipeline is build from three different kind of objects:

* Producers send data downstream
* Consumers receive data from upstream
* Pipes receive data from upstream, transform it and send it downstream.

Depending on their types the object have to be able to tell which type of data
they receive and send. This is to make construction of valid streaming pipelines
fault resistant, thus having to do less error handling when actually processing
data through a pipeline.

Elements of a stream can be combined vertically by using the shift operators 
>> and <<. 

It should be possible to process the same pipeline multiple times and even in
parallel, without the different process interfering with each other.
"""

class StreamPart(object):
    """
    Common base class for the parts of a stream processing pipeline.
    """
    def __rshift__(self, other):
        """
        Compose two parts to get a new part.

        __rshift__ is left biased, this a >> b >> c = (a >> b) >> c
        """
        return compose_stream_parts(self, other)

    def __lshift__ (self, other):
        """
        Compose two part to get a new part.
        """
        return compose_stream_parts(other, self)

    def get_initial_state(self):
        """
        Should return a state object that is used to pass state during one
        execution of the stream part.

        The state object could e.g. be used to hold references to resources.

        If a multithreaded environment is used, this also must be a synchronization
        point between different threads.
        """
        raise NotImplementedError("StreamPart::get_initial_state: implement me!")

    def shutdown_state(self, state):
        """
        Shut perform appropriate shutdown actions for the given state. Will be
        called after one execution of the pipeline with the resulting state.

        If a multithreaded environment is used, this also must be a synchronization
        point between different threads.
        """
        raise NotImplementedError("StreamPart::shutdown_state: implement me!")
    
class Producer(StreamPart):
    """
    A producer is the source for a stream, that is, it produces new data for
    the downstream.
    """
    def type_out(self):
        """
        Get the type of output this producer produces.
        """
        raise NotImplementedError("Producer::type_out: implement me!")

    def produce(self, state):
        """
        Produce new data to be send downstream. The result must solely be a
        result from data in the state object. It must be threadsafe to call
        produce. Produce must return a value with a type according to type_out.
        """
        raise NotImplementedError("Producer::produce: implement me!")

    def can_produce(self, state):
        """
        Must tell whether the producer could produce a new value based on state.
        """
        raise NotImplementedError("Producer::can_produce: implement me!")

    def __str__(self):
        return "(() -> %s)" % self.type_out()

class Continue:
    pass

class MayContinue:
    pass

class Stop:
    pass

class Consumer(StreamPart):
    """
    A consumer is the sink for a stream, that is it consumes data from upstream
    without producing new values.
    """
    def type_in(self):
        """
        Get the type, this consumer consumes.
        """
        raise NotImplementedError("Consumer::type_in: implement me!")

    def consume(self, value, state):
        """
        Consume a new piece of data from upstream. It must be threadsafe to call
        produce. Consume must be able to process values with types according to  
        type_in. Return values will be ignored. 
        """
        raise NotImplementedError("Consumer::consume: implement me!")

    def can_continue(self, state):
        """
        Either returns Continue, which signals consumer needs more data, 
        MayContinue, which signals it could consume more input but as well could
        return a result or Stop which signals it can't consume more data.
        """
        raise NotImplementedError("Consumer::can_continue: implement me!")

    def result(self, state):
        """
        Turn the state into a result.
        """
        raise NotImplementedError("Consumer::result: implement me!")

    def __str__(self):
        return "(%s -> ())" % self.type_in()

class Pipe(Producer, Consumer):
    """
    A pipe is an element between a source and a sink, that is it consumes data
    from upstream an produces new data for downstream.
    """ 
    def __str__(self):
        return "(%s -> %s)" % (self.type_in(), self.type_out()) 

    def transform(self, value, state):
        """
        Transform an input value to an output value. Must adhere to the types
        from type_in and type_out. 
        """
        raise NotImplementedError("Pipe::transform: implement me!")
        

class StreamProcess(object):
    """
    A stream process is a completely defined process between sources and sinks
    with no dangling ends. It could be run.
    """
    def __init__(self, producer, consumer):
        self.producer = producer
        self.consumer = consumer

    def run(self):
        """
        Let this stream process run.

        This is a fairly naive implementation. Since one could use differerent
        execution models for the same stream, it is very likely that this method
        will exchanged by various interpreter-like runners.
        """
        pass

# Objects from the classes need to respect the follwing rules, where abbreaviations
# for the names are used
#
# any >> Pr = error
# Co >> any = error
# Pi >> Pi = Pi
# SP >> any = error
# any >> SP = error
# Pr >> Pi = Pr
# Pi >> Co = Co
# Pr >> Co = SP

def compose_stream_parts(left, right):
    """ 
    Compose two stream parts to get a new one.
    
    Throws TypeErrors when parts can't be combined.
    """
    t_left = type(left)
    t_right = type(right)

    if t_left == Pipe and t_right == Pipe:
        return FusePipes(left, right)
    elif t_left == Pipe and t_right == Consumer:
        return PrependPipe(left, right)
    elif t_left == Producer and t_right == Pipe:
        return AppendPipe(left, right)
    elif t_left == Producer and t_right == Consumer:
        raise NotImplementedError("compose_stream_parts: implement fusion of "
                                  "producer and consumer to stream process.")
    else:
        raise TypeError("Can't compose %s and %s" % (left, right))


class ComposedStreamPart(object):
    """
    Mixin for all composed stream parts.
    """
    def __init__(self, left, right):
        if left.type_out() != right.type_in():
            raise TypeError("Can't compose %s and %s" % (left, right))

        self.left = left
        self.right = right
 
    def get_initial_state(self):
        return (self.left.get_initial_state(), self.right.get_initial_state())
    def shutdown_state(self, state):
        l, r = state
        self.left.shutdown_state(l)
        self.right.shutdown_state(r)   

class FusePipe(Pipe, ComposedStreamPart):
    """
    A pipe build from two other pipes.
    """
    def type_in(self):
        return self.left.type_in()

    def type_out(self):
        return self.right.type_out()

    def transform(self, value, state):
        l, r = state
        tmp = self.left.transform(value, l)
        return self.right.transform(value, r)
    
class AppendPipe(Producer, ComposedStreamPart):
    """
    A producer build from another producer with an appended pipe.
    """
    def type_out(self):
        return self.right.type_out()

    def produce(self, state):
        l, r = state
        tmp = self.left.produce(l)
        return self.right.transform(tmp, r)
    def can_produce(self, state);
        l, r = state
        return self.left.can_produce(l)

class PrependPipe(Consumer, ComposedStreamPart):
    """
    A consumer build from another consumer with a prepended pipe.
    """
    def type_in(self):
        return self.left.type_in()

    def consume(self, value, state):
        l, r = state
        tmp = self.left.transform(value, l)
        self.right.consume(tmp, r)
    def can_continue(self, state):
        l, r = state
        return self.right.can_continue(r) 
    
