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

Producers use generators to produce their values.

Consumers and Producers can use an environment to e.g. hold resources. 
"""

class StreamPart(object):
    """
    Common base class for the parts of a stream processing pipeline.
    """
    def __rshift__(self, other):
        """
        Compose two parts to get a new part.

        __rshift__ is left biased, thus a >> b >> c = (a >> b) >> c
        """
        return compose_stream_parts(self, other)

    def __lshift__ (self, other):
        """
        Compose two part to get a new part.
        """
        return compose_stream_parts(other, self)

    def get_initial_env(self):
        """
        Should return a environment object that is used during one execution of 
        the stream part.

        The environment object could e.g. be used to hold references to resources.

        If a multithreaded environment is used, this also must be a synchronization
        point between different threads.
        """
        raise NotImplementedError("StreamPart::get_initial_env: implement "
                                  "me for class %s!" % type(self))

    def shutdown_env(self, env):
        """
        Shut perform appropriate shutdown actions for the given environment. Will be
        called after one execution of the pipeline with the used environment.

        If a multithreaded environment is used, this also must be a synchronization
        point between different threads.
        """
        raise NotImplementedError("StreamPart::shutdown_env: implement "
                                  "me for class %s!" % type(self))
    
class Producer(StreamPart):
    """
    A producer is the source for a stream, that is, it produces new data for
    the downstream.
    """
    def type_out(self):
        """
        Get the type of output this producer produces.
        """
        raise NotImplementedError("Producer::type_out: implement "
                                  "me for class %s!" % type(self))

    def produce(self, env):
        """
        Generate new data to be send downstream. The result must solely be a
        result from data in the env object. It must be threadsafe to call
        produce. Produce must yield values with a type according to type_out.
        """
        raise NotImplementedError("Producer::produce: implement "
                                  "me for class %s!" % type(self))

    def __str__(self):
        return "(() -> %s)" % self.type_out()

class Consumer(StreamPart):
    """
    A consumer is the sink for a stream, that is it consumes data from upstream
    without producing new values.
    """
    def type_in(self):
        """
        Get the type, this consumer consumes.
        """
        raise NotImplementedError("Consumer::type_in: implement "
                                  "me for class %s!" % type(self))

    def consume(self, env, await):
        """
        Consume data from upstream. It must be threadsafe to call consume. 
        Consume must be able to process values with types according to type_in. 
        Could return a result. 

        await is a function that could be called to get the next value from
        upstream.
        """
        raise NotImplementedError("Consumer::consume: implement "
                                  "me for class %s!" % type(self))

    def __str__(self):
        return "(%s -> ())" % self.type_in()

class Pipe(Producer, Consumer):
    """
    A pipe is an element between a source and a sink, that is it consumes data
    from upstream an produces new data for downstream.
    """ 
    def __str__(self):
        return "(%s -> %s)" % (self.type_in(), self.type_out()) 

    def transform(self, env, await):
        """
        Take data from upstream and generate data for downstream. Must adhere to
        the types from type_in and type_out. Must be a generator.
        
        await is a function that could be called to get the next value from
        upstream.
        """
        raise NotImplementedError("Pipe::transform: implement "
                                  "me for class %s!" % type(self))
        

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
        p_env = self.producer.get_initial_env()
        c_env = self.consumer.get_initial_env()

        producer_gen = self.producer.produce(p_env)

        def await():
            return producer_gen.__next__() 

        try:
            return self.consumer.consume(await, c_env)
        finally:
            self.producer.shutdown_env(p_env)
            self.consumer.shutdown_env(c_env)
            

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
    if isinstance(left, Pipe) and isinstance(right, Pipe):
        return FusePipe(left, right)
    elif isinstance(left, Pipe) and isinstance(right, Consumer):
        return PrependPipe(left, right)
    elif isinstance(left, Producer) and isinstance(right, Pipe):
        return AppendPipe(left, right)
    elif isinstance(left, Producer) and isinstance(right, Consumer):
        return StreamProcess(left, right)
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
 
    def get_initial_env(self):
        return (self.left.get_initial_env(), self.right.get_initial_env())
    def shutdown_env(self, env):
        l, r = env 
        self.left.shutdown_env(l)
        self.right.shutdown_env(r)   

class FusePipe(ComposedStreamPart, Pipe):
    """
    A pipe build from two other pipes.
    """
    def type_in(self):
        return self.left.type_in()

    def type_out(self):
        return self.right.type_out()

    def transform(self, env, await):
        l, r = env 
        producer_gen = self.left.transform(self, l, await)
        def await_left():
            return producer_gen.__next__()
        return self.right.transform(r, await_left)
    
class AppendPipe(ComposedStreamPart, Producer):
    """
    A producer build from another producer with an appended pipe.
    """
    def type_out(self):
        return self.right.type_out()

    def produce(self, env):
        l, r = env 
        producer_gen = self.left.produce(l)
        def await():
            return producer_gen.__next__()
        return self.right.transform(r, await)

class PrependPipe(ComposedStreamPart, Consumer):
    """
    A consumer build from another consumer with a prepended pipe.
    """
    def type_in(self):
        return self.left.type_in()

    def consume(self, env, await):
        l, r = env
        producer_gen = self.left.transform(l, await)
        def await_left():
            return producer_gen.__next__()
        return self.right.consume(r, await_left)
