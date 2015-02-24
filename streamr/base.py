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

if "reduce" not in globals():
    from functools import reduce

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

class Resume(object):
    pass

class Stop(object):
    def __init__(self, result):
        self.result = result

class MayResume(object):
    def __init__(self, result):
        self.result = result

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

        The method should not attempt to consume all values from upstream, since
        we need it to support cooperative concurrency. It rather should consume
        only the amount of values it needs for the next meaningful step.

        Afterwards the consumer should either return Resume if it needs to be
        resumed, or Stop if it can't be resumed or MayResume if it may be resumed.

        If a StopIteration exception is thrown during execution of consume and
        the last result of consume was MayResume, the last result is taken.
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

        res = None

        try:
            while True:
                res = self.consumer.consume(c_env, upstream)
                if isinstance(res, Stop):
                    return res.result 
        except StopIteration:
            if res is not None and isinstance(res, (Stop, MayResume)):
                return res.result
            raise RuntimeError("The producer ran out of values, but consumer "
                               "wants to resume.")
        finally:
            self.producer.shutdown_env(p_env)
            self.consumer.shutdown_env(c_env)


###############################################################################
#
# Compose stream parts for sequencing
#
###############################################################################
            
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
    if isinstance(left, MixedStreamPart) or isinstance(right, MixedStreamPart):
        return compose_mixed_stream_parts(left, right)
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
    # Compress the two stacked pipes to one stacked pipe by fusing its
    # parts together.
    if isinstance(left, StackPipe) or isinstance(right, StackPipe):
        return fuse_stack_pipes(left, right)

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
        return { "envs" : [part.get_initial_env() for part in self.parts] }
    def shutdown_env(self, env):
        [v[0].shutdown_env(v[1]) for v in zip(self.parts, env["envs"])] 

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
        for part, e in zip(self.parts, env["envs"]):
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
        upstream = self.parts[0].produce(env["envs"][0])
        return self.parts[1].transform(env["envs"][1], upstream)

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

    def get_initial_env(self):
        env = super(PrependPipe, self).get_initial_env()

        # Since we get a new upstream generator on every call
        # to consume, we need to create a generator that yields
        # values from the current upstream.
        def pull_from_upstream():
            while True:
                yield next(env["upstream"])

        env["upstream_left"] = self.parts[0].transform(["envs"][0], pull_from_upstream())

        return env


    def consume(self, env, upstream):
        # Set the upstream to the current one:
        env["upstream"] = upstream
        return self.parts[1].consume(env["envs"][1], env["upstream_left"])


###############################################################################
#
# Stack stream parts for parallelity 
#
###############################################################################

def stack_stream_parts(top, bottom):
    if isinstance(top, Pipe) and isinstance(bottom, Pipe):
        return stack_pipe(top, bottom)
    if isinstance(top, Producer) and isinstance(bottom, Producer):
        return stack_producer(top, bottom)
    if isinstance(top, Consumer) and isinstance(bottom, Consumer):
        return stack_consumer(top, bottom)
    if isinstance(top, StreamPart) or isinstance(bottom, StreamPart):
        return stack_mixed(top, bottom)
    
    raise TypeError("Can't stack %s and %s" % (top, bottom))

def stack_pipe(top, bottom):
    return StackPipe(top, bottom)

def fuse_stack_pipes(left, right):
    if not right.type_in().is_satisfied_by(left.type_out()):
        raise TypeError("Can't compose '%s' and '%s'" % (left, right))

    fused = [l >> r for l, r in zip(left.parts, right.parts)]
    return StackPipe(*fused)

def stack_producer(top, bottom):
    return StackProducer(top, bottom)

def stack_consumer(top, bottom):
    return StackConsumer(top, bottom)

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
        amount_chans = len(self.parts)
        upstream_chans,_ = _split_upstream([upstream], amount_chans)

        gens = []
        for part, e, us in zip(self.parts, env["envs"], upstream_chans):
            gens.append(part.transform(e, us))

        while True:
            vals = []
            for gen in gens:
                vals.append(next(gen))
            yield tuple(vals) 

class StackProducer(ComposedStreamPart, Producer):
    """
    Some producers stacked onto each other.

    Will stop to produce if the first producer stops to produce values.
    """
    def __init__(self, *producers):
        assert len(producers) >= 2

        super(StackProducer, self).__init__(*producers)

        self.tout = reduce(lambda x,y: x * y, (p.type_out() for p in producers))

    def type_out(self):
        return self.tout

    def produce(self, env):
        gens = []
        for part, e in zip(self.parts, env["envs"]):
            gens.append(part.produce(e))

        while True:
            vals = []
            for gen in gens:
                vals.append(next(gen))
            yield tuple(vals)

class StackConsumer(ComposedStreamPart, Consumer):
    """
    Some consumers stacked onto each other. 
    """
    def __init__(self, *consumers):
        assert len(consumers) >= 2

        super(StackConsumer, self).__init__(*consumers)

        self.tin = reduce(lambda x,y: x * y, (c.type_in() for c in consumers))

    def type_in(self):
        return self.tin

    def get_initial_env(self):
        env = super(StackConsumer, self).get_initial_env()

        # We use a similar trick than in PrependPipe here.
        env["upstream"] = [None]
        amount_chans = len(self.parts)
        usc,_ = _split_upstream(env["upstream"], amount_chans)
        env["upstream_chans"] = usc

        return env

    def consume(self, env, upstream):
        """
        Executes one step of every consumer for each step.
        
        If any consumer wants to stop it raises if any other consumer needs
        to be resumed.
        """
        # Set the upstream to be used to the current upstream.
        env["upstream"][0] = upstream

        res = []
        stop = False
        must_resume = False
        for c, e, us in zip(self.parts, env["envs"], env["upstream_chans"]):  
            r = c.consume(e, us)
            stop = stop or isinstance(r, Stop)
            must_resume = must_resume or isinstance(r, Resume)
            res.append(r.result)

        if stop and must_resume:
            raise RuntimeError("One consumer wants to stop, while another needs to resume.")

        if must_resume:
            return Resume()

        if stop:
            return Stop(tuple(res))

        return MayResume(tuple(res))

def _split_upstream(upstream, amount_chans):
    """
    Get a list of generators where the values from upstream are decomposed
    into amount_chans channels and values could be generated on every channel. 

    The upstream argument should be a list with one element to be able to
    exchange the upstream during execution.

    Returns a list of new upstreams and the list of queues used.
    """
    # Use different queues for the values we get from upstream.
    # It must be possible to yield different amounts of value per
    # channel.
    queues = [[]] * amount_chans

    def upstream_chan(num):
        while True:
            if len(queues[num]) == 0:
                new = next(upstream[0])
                for i in range(0, amount_chans):
                    queues[i].append(new[i])
                
            yield queues[num].pop(0)

    return [upstream_chan(i) for i in range(0, amount_chans)], queues


class MixedStreamPart(StreamPart):
    """
    A stacked stream part that contains producers, pipes and consumers as well.

    Can't be executed as is, just used during construction of stream processes.
    """
    def __init__(self, *parts):
        pr, pi, co = MixedStreamPart._split_parts(parts)
        self.producers = pr
        self.pipes = pi
        self.consumers = co
        self.parts = list(parts)

        self.tin = reduce( lambda x,y: x * y
                         , (p.type_in() for p in 
                           filter( lambda x: not isinstance(x, Producer)
                                 , self.parts)))
        self.tout = reduce( lambda x,y: x * y
                          , (p.type_out() for p in
                            filter( lambda x: not isinstance(x, Consumer)
                                  , self.parts))) 

    @staticmethod
    def _split_parts(parts):
        pr, pi, co = [], [], []

        for part in parts:
            pr.append(None if not isinstance(part, Producer) else part)
            pi.append(None if not isinstance(part, Pipe) else part)
            co.append(None if not isinstance(part, Consumer) else part)

        return pr, pi, co

    def type_in(self):
        return self.tin

    def type_out(self):
        return self.tout

    def get_initial_env(self):
        raise RuntimeError("Do not attempt to run a ClosedEndStackPipe!!")

    def shutdown_env(self):
        raise RuntimeError("Do not attempt to run a ClosedEndStackPipe!!")

def stack_mixed(top, bottom):
    parts = [top] if not isinstance(top, MixedStreamPart) else top.parts
    if isinstance(bottom, MixedStreamPart):
        parts += bottom.parts
    else:
        parts.append(bottom)

    return MixedStreamPart(*parts)

def compose_mixed_stream_parts(left, right):
    pass
