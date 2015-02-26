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

from .types import Type, ArrowType, ALL, sequence, unit

###############################################################################
#
# Interface for an processor in the pipeline.
#
###############################################################################

class StreamProcessor(object):
    """
    A stream processor is the fundamental object to describe a stream process.

    A stream processor must be initialized with a value of some type and generates
    a return value of some other type. It can read values of a type from upstream 
    and send values of another type downstream.

    During initialisation, the stream processor must create an environment, that
    could be used during the processing, e.g. to store resources. A stream 
    processor must be able to handle multiple environments simultaniously. It is
    guaranteed that the environment is passed to the shutdown method after the
    processing.

    Stream processors can be composed sequentially, that is, the downstream of the
    first processor is fed as upstream to the second processor.
    
    Stream processor can also be composed in parallel, which results in a new
    stream processor that uses the products of the types from its components as
    up- and downstream.
    """
    def __init__(self, type_init, type_result, type_in, type_out):
        self.tinit = Type.get(type_init)
        self.tresult = Type.get(type_result)
        self.tin = Type.get(type_in)
        self.tout = Type.get(type_out)

        self.runtime_engine = StreamProcessor.runtime_engine

    def type_init(self):
        return self.tinit
    def type_result(self):
        return self.tresult
    def type_in(self):
        return self.tin
    def type_out(self):
        return self.tout

    def type_arrow(self):
        """
        Arrow type for the processed stream.
        """
        return ArrowType.get(self.type_in(), self.type_out())

    def __str__(self):
        return "[%s -> %s](%s)" % ( self.type_init(), self.type_result()
                                  , self.type_arrow())

    def get_initial_env(self, *params):
        """
        Initialize a new runtime environment for the stream processor.
        """
        assert self.type_init().contains(params)

    def shutdown_env(self, env):
        """
        Shutdown a runtime environment of the processor.
        """
        pass

    def step(self, env, await, send):
        """
        Performs the actual processing. Gets an env that was created by
        get_initial_env, a function to pull values from upstream function to 
        send data downstream.

        Must return Stop, MayResume oder Resume.
        """
        raise NotImplementedError("Implement StreamProcessor.step for"
                                  " %s!" % type(self))
        
    # Engine used for composition
    composition_engine = None

    def __rshift__(self, other):
        """
        Sequential composition.

        For two stream processors with types (init1, result1, a, b) and
        (init2, result2, b, c), produces a new stream processor with the types
        (init1 * init2, result1 * result2, a, c).

        __rshift__ is left biased, thus a >> b >> c = (a >> b) >> c. Nonetheless
        (a >> b) >> c =~ a >> (b >> c) should hold, where =~ means extensional
        equality.
        """
        return StreamProcessor.composition_engine.compose_sequential(self, other)    

    def __lshift__(self, other):
        return StreamProcessor.composition_engine.compose_sequential(other, self)   

    def __mul__(self, other):
        """
        Parallel composition.

        For two stream processors with types (init1, result1, a1, b1) and
        (init2, result2, a2, b2), produces a new stream processor with the types
        (init1 * init2, result1 * result2, a1 * a2, b1 * b2).
        """
        return StreamProcessor.composition_engine.compose_parallel(self, other)   


    # Some judgements about the stream processor

    def is_producer(self):
        """
        This processor does not consume data from upstream.
        """
        return self.type_in() == () and self.type_out() != ()

    def is_consumer(self):
        """
        This processor does not send data downstream.
        """
        return self.type_out() == () and self.type_in() != ()

    def is_pipe(self):
        """
        This processor consumes data from upstream and sends data
        downstream.
        """
        return self.type_in() != () and self.type_out() != ()

    def is_runnable(self):
        """
        This processes stream arrow has type () -> ().
        """
        return self.type_arrow() == ArrowType.get((), ())

    # Engine used for running the process
    runtime_engine = None

    def run(self, *params):
        """
        Run the stream process, using its runtime_engine.

        Params is used to initialize the environment of the processor. Returns
        a result or throws a runtime error.

        Process can only be run if its arrow is () -> ().
        """
        return self.runtime_engine.run(self, params)


###############################################################################
#
# Signals for the step function.
#
###############################################################################

class Resume(object):
    pass

class Stop(object):
    def __init__(self, result):
        self.result = result

class MayResume(object):
    def __init__(self, result):
        self.result = result

class Exhausted(object):
    pass


###############################################################################
#
# Basic classes for composed stream processors.
#
###############################################################################

class ComposedStreamProcessor(StreamProcessor):
    """
    Mixin for all composed stream processors.

    Handles initialisation and shutdown of the sub processors.
    """
    def __init__(self, type_in, type_out, processors):
        assert ALL(isinstance(p, StreamProcessor) for p in processors)

        tinit = Type.get(*[p.type_init() for p in processors])
        tresult = Type.get(*[p.type_result() for p in processors])

        super(ComposedStreamProcessor, self).__init__(tinit, tresult, type_in, type_out)

        self.processors = list(processors)
 
    def get_initial_env(self, *params):
        envs = []
        i = 0
        for p in self.processors:
            if p.type_init() is unit:
                envs.append(p.get_initial_env())
            else:
                assert p.type_init().contains(params[i])
                env.append(p.get_initial_env(params[i]))
                i += 1
                assert i < len(params)

        return { "envs" : envs }

    def shutdown_env(self, env):
        for p, e in zip(self.processors, env["envs"]):
            p.shutdown_env(e)


class SequentialStreamProcessor(ComposedStreamProcessor):
    """
    A processor that processes it subprocessors sequentially by feeding the
    output of a previous processor to the input of the next processor.
    """
    def __init__(self, processors):
        tarr = sequence([p.type_arrow() for p in processors]) 

        super(SequentialStreamProcessor, self).__init__( tarr.type_in()
                                                       , tarr.type_out()
                                                       , processors)

    def get_initial_env(self, *params):
        env = super(SequentialStreamProcessor, self).get_initial_env(*params)
        env["rt"] = self.runtime_engine.get_initial_env_for_seq(self.processors)
        return env
        

    def step(self, env, await, send):
        return self.runtime_engine.step_seq( self.processors
                                           , env["envs"]
                                           , env["rt"]
                                           , await 
                                           , send 
                                           )


class ParallelStreamProcessor(ComposedStreamProcessor):
    """
    A processor that processes it subprocessors in parallel.
    """
    def __init__(self, processors):
        tin = Type.get(*[p.type_in() for p in processors])
        tout = Type.get(*[p.type_out() for p in processors])

        super(ParallelStreamProcessor, self).__init__(tin, tout, processors)

    def get_initial_env(self, *params):
        env = super(ParallelStreamProcessor, self).get_initial_env(*params)
        env["rt"] = self.runtime_engine.get_initial_env_for_par(self.processors)
        return env

    def step(self, env, await, send):
        return self.runtime_engine.step_par( self.processors
                                           , env["envs"]
                                           , env["rt"]
                                           , await
                                           , send
                                           )
                
class EnvAndGen(object):
    def __init__(self, env, gen):
        self.env = env
        self.gen = gen

class Producer(StreamProcessor):
    """
    A producer is the source for a stream, that is, it produces new data for
    the downstream without an upstrean and creates no result.
    """
    def __init__(self, type_init, type_out):
        super(Producer, self).__init__(type_init, (), (), type_out)

    def step(self, env, await, send):
        if not isinstance(env, EnvAndGen):
            env = EnvAndGen(env, self.produce(env))

        send(next(env.gen))
        return MayResume(())

    def produce(self, env):
        """
        Generate new data to be send downstream. It must be threadsafe to call
        produce. Produce must yield values with a type according to type_out.
        """
        raise NotImplementedError("Producer::produce: implement "
                                  "me for class %s!" % type(self))
class Consumer(StreamProcessor):
    """
    A consumer is the sink for a stream, that is it consumes data from upstream
    without producing new values. It also returns a result.
    """
    def __init__(self, type_init, type_result, type_in):
        super(Consumer, self).__init__(type_init, type_result, type_in, ())

    def step(self, env, await, send):
        def _upstream():
            while True:
                val = await()
                yield val

        return self.consume(env, _upstream())

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

class Pipe(StreamProcessor):
    """
    A pipe is an element between a source and a sink, that is it consumes data
    from upstream an produces new data for downstream. It does not produce a
    result.
    """ 
    def __init__(self, type_init, type_in, type_out):
        super(Pipe, self).__init__(type_init, (), type_in, type_out)

    def __str__(self):
        return "(%s -> %s)" % (self.type_in(), self.type_out()) 

    def step(self, env, await, send):
        if not isinstance(env, EnvAndGen):
            def _upstream():
                while True:
                    val = await()
                    yield val

            env = EnvAndGen(env, self.transform(env, _upstream()))

        send(next(env.gen))
        return MayResume(())        

    def transform(self, env, upstream):
        """
        Take data from upstream and generate data for downstream. Must adhere to
        the types from type_in and type_out. Must be a generator.
        
        upstream is a function that could be called to get the next value from
        upstream.
        """
        raise NotImplementedError("Pipe::transform: implement "
                                  "me for class %s!" % type(self))
        
class StreamProcess(StreamProcessor):
    """
    A stream process is a completely defined process between sources and sinks
    with no dangling ends. It could be run.
    """
    def __init__(self, producer, consumer):
        self.producer = producer
        self.consumer = consumer

        tinit = producer.type_init() * consumer.type_init()
        super(StreamProcess, self).__init__( tinit, consumer.type_result()
                                           , (), () )  

    def get_initial_env(self, *params):
        return { "env_producer" : self.producer.get_initial_env()
               , "env_consumer" : self.consumer.get_initial_env()
               , "cache"        : []
               }               
                

    def shutdown_env(self, env):
        self.producer.shutdown_env(env["env_producer"])
        self.consumer.shutdown_env(env["env_consumer"])

    def step(self, env, await, send):
        """
        Let this stream process run.

        This is a fairly naive implementation. Since one could use differerent
        execution models for the same stream, it is very likely that this method
        will exchanged by various interpreter-like runners.
        """
        def internal_send(_val):
            env["cache"].append(val)

        def internal_upstream():
            while True:
                while len(env["cache"]) == 0:
                    res = self.producer.step(env["env_producer"], await, internal_send)
                    if isinstance(res, Stop):
                        raise StopIteration()
                while len(env["cache"]) > 0:
                    yield env["cache"].pop(0) 

        return self.consumer.step(env["env_consumer"], internal_upstream(), send)
