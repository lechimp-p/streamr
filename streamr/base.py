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

Consumers, Producers and Pipes can use an environment to e.g. hold resources. 
Those resources are guaranteed being shutdown by passing them to a shutdown
method after a stream process is run.

Stream processes are consumer driven, that is, a consumer pulls data from
upstream until he has enough data or there is no more data in the upstream.
"""

from .types import ArrowType, ALL

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

    def __mul__(self, other):
        """
        Stack two stream parts onto each other to process them in
        parallel.
        """
        return stack_stream_parts(self, other)

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

        This can never be variable, since one should always know what a producer
        produces as values.
        """
        raise NotImplementedError("Producer::type_out: implement "
                                  "me for class %s!" % type(self))

    def produce(self, env):
        """
        Generate new data to be send downstream. It must be threadsafe to call
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

    def consume(self, env, upstream):
        """
        Consume data from upstream. It must be threadsafe to call consume. 
        Consume must be able to process values with types according to type_in. 
        Could return a result. 

        upstream is a generator that yields values from upstream.
        """
        raise NotImplementedError("Consumer::consume: implement "
                                  "me for class %s!" % type(self))

    def __str__(self):
        return "(%s -> ())" % self.type_in()

class Pipe(StreamPart):
    """
    A pipe is an element between a source and a sink, that is it consumes data
    from upstream an produces new data for downstream.
    """ 
    def type_in(self):
        """
        Get the type that transformer wants from upstream.
        """
        raise NotImplementedError("Pipe::type_in: implement "
                                  "me for class %s!" % type(self))

    def type_out(self):
        """
        Get the type the transformer sends downstream.
        """
        raise NotImplementedError("Pipe::type_out: implement "
                                  "me for class %s!" % type(self))

    def type_arrow(self):
        """
        Get the type of the transformer as arrow type.
        """
        return ArrowType.get(self.type_in(), self.type_out())

    def __str__(self):
        return "(%s -> %s)" % (self.type_in(), self.type_out()) 

    def transform(self, env, upstream):
        """
        Take data from upstream and generate data for downstream. Must adhere to
        the types from type_in and type_out. Must be a generator.
        
        upstream is a function that could be called to get the next value from
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
        if not consumer.type_in().is_satisfied_by(producer.type_out()):
            raise TypeError("Can't compose '%s' and '%s'" % (producer, consumer))

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

        upstream = self.producer.produce(p_env)

        try:
            return self.consumer.consume(c_env, upstream)
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
        return fuse_pipes(left, right)
    elif isinstance(left, Pipe) and isinstance(right, Consumer):
        return prepend_pipe(left, right)
    elif isinstance(left, Producer) and isinstance(right, Pipe):
        return append_pipe(left, right)
    elif isinstance(left, Producer) and isinstance(right, Consumer):
        return stream_process(left, right)
    else:
        raise TypeError("Can't compose %s and %s" % (left, right))

def fuse_pipes(left, right):
    pipes = [left] if not isinstance(left, FusePipe) else left.parts
    if isinstance(right, FusePipe):
        pipes += right.parts
    else:
        pipes.append(right)

    return FusePipe(*pipes)

def prepend_pipe(left, right):
    if isinstance(right, PrependPipe):
        left = left >> right.parts[0]
        right = right.parts[1]

    return PrependPipe(left, right)

def append_pipe(left, right):
    if isinstance(left, AppendPipe):
        right = left.parts[1] >> right 
        left = left.parts[0]

    return AppendPipe(left, right)

def stream_process(left, right):
    return StreamProcess(left, right)

class ComposedStreamPart(object):
    """
    Mixin for all composed stream parts.

    Handles initialisation and shutdown of the sub parts.
    """
    def __init__(self, *parts):
        assert ALL(isinstance(p, StreamPart) for p in parts)

        self.parts = parts 
 
    def get_initial_env(self):
        return [part.get_initial_env() for part in self.parts]
    def shutdown_env(self, env):
        [v[0].shutdown_env(v[1]) for v in zip(self.parts, env)] 

class FusePipe(ComposedStreamPart, Pipe):
    """
    A pipe build from two other pipes.
    """
    def __init__(self, *pipes):
        assert len(pipes) >= 2

        super(FusePipe, self).__init__(*pipes)

        self.tin = pipes[0].type_in()
        self.tout = FusePipe._infere_type_out(pipes)

    @staticmethod
    def _infere_type_out(pipes):
        cur = pipes[0].type_arrow() 

        for p in pipes[1:]:
            cur = cur.compose_with(p.type_arrow()) 

        return cur.r_type

    def type_in(self):
        return self.tin

    def type_out(self):
        return self.tout

    def transform(self, env, upstream):
        for part, e in zip(self.parts, env):
            upstream = part.transform(e, upstream)
        return upstream
    
class AppendPipe(ComposedStreamPart, Producer):
    """
    A producer build from another producer with an appended pipe.
    """
    def __init__(self, producer, pipe):
        assert isinstance(producer, Producer)
        assert isinstance(pipe, Pipe)

        super(AppendPipe, self).__init__(producer, pipe)

        self.tout = AppendPipe._infere_type_out(producer, pipe)

    @staticmethod
    def _infere_type_out(producer, pipe):
        return pipe.type_arrow()(producer.type_out())

    def type_out(self):
        return self.tout

    def produce(self, env):
        upstream = self.parts[0].produce(env[0])
        return self.parts[1].transform(env[1], upstream)

class PrependPipe(ComposedStreamPart, Consumer):
    """
    A consumer build from another consumer with a prepended pipe.
    """
    def __init__(self, pipe, consumer):
        assert isinstance(pipe, Pipe)
        assert isinstance(consumer, Consumer)

        super(PrependPipe, self).__init__(pipe, consumer)

    def type_in(self):
        return self.parts[0].type_in()

    def consume(self, env, upstream_right):
        upstream_left = self.parts[0].transform(env[0], upstream_right)
        return self.parts[1].consume(env[1], upstream_left)


def stack_stream_parts(top, bottom):
    if isinstance(top, Pipe) and isinstance(bottom, Pipe):
        return stack_pipe(top, bottom)
    
    raise TypeError("Can't stack %s and %s" % (left, right))

def stack_pipe(top, bottom):
    return StackPipe(top, bottom)

class StackPipe(ComposedStreamPart, Pipe):
    """
    Some pipes stacked onto each other.
    """
    def __init__(self, *pipes):
        assert len(pipes) >= 2

        super(StackPipe, self).__init__(*pipes)

        self.tin = reduce(lambda x,y: x * y, (p.type_in() for p in pipes))
        self.tout = reduce(lambda x,y: x * y, (p.type_out() for p in pipes))

    def type_in(self):
        return self.tin
    def type_out(self):
        return self.tout

    def transform(self, env, upstream):
        pass 
