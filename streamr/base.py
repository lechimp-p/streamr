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

    def step(self, env, upstream, downstream):
        """
        Performs the actual processing. Gets an env that was created by
        get_initial_env, a upstream generator object and a function that
        sends data downstream.

        Must return Stop, MayResume oder Resume. If StopIteration is thrown
        inside the body, that is interpreted as if the result from the last
        invocation is the result. If there is no such result, that is interpreted
        as Resume.
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


class Resume(object):
    pass

class Stop(object):
    def __init__(self, result):
        self.result = result

class MayResume(object):
    def __init__(self, result):
        self.result = result


class SimpleCompositionEngine(object):
    """
    Very simple engine to provide composition operations for stream processors.

    Could be switched for a more sophisticated engine, e.g. for performance purpose.
    """
    def compose_sequential(self, left, right):
        not_composable = (  left.is_consumer()
                         or right.is_producer()
                         or left.is_runnable()
                         or right.is_runnable()
                         or not right.type_in().is_satisfied_by(left.type_out()))

        if not_composable:
            raise TypeError("Can't compose %s and %s." % (left, right))

        return SequentialStreamProcessor([left, right])

    def compose_parallel(self, top, bottom):
        return ParallelStreamProcessor([top, bottom])

StreamProcessor.composition_engine = SimpleCompositionEngine()

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
        self.amount_procs = len(self.processors)
        self.amount_result = len(list(filter(lambda x: x.type_result() != (), self.processors)))
 
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
    def __init__(self, processors):
        tarr = sequence([p.type_arrow() for p in processors]) 

        super(SequentialStreamProcessor, self).__init__( tarr.type_in()
                                                       , tarr.type_out()
                                                       , processors)


    def get_initial_env(self, *params):
        env = super(SequentialStreamProcessor, self).get_initial_env(*params)
        env["caches"] = [[]] * self.amount_procs
        env["results"] = [()] * self.amount_procs
        env["exhausted"] = [False] * self.amount_procs
        return env

    def _downstream(self, env, downstream, i):
        """
        Downstream from i'th processor.
        """
        assert i < self.amount_procs
        assert i >= 0

        if i == self.amount_procs - 1:
            return downstream

        def _downstream(val):
           env["caches"][i].append(val)

        return _downstream 

    def _upstream(self, env, upstream, downstream, i):
        """
        Upstream to i'th processor.
        """
        assert i < self.amount_procs
        assert i >= 0

        if i == 0:
            return upstream

        def _upstream():
            us = self._upstream(env, upstream, downstream, i-1)
            ds = self._downstream(env,downstream,i-1)
            while True:
                while len(env["caches"][i-1]) == 0:
                    if env["exhausted"][i-1]:
                        raise StopIteration()
                    p = self.processors[i-1]
                    res = p.step( env["envs"][i-1], us, ds)
                    if not isinstance(res, Resume) and p.type_result() != ():
                        env["results"][i-1] = res.result
                    if isinstance(res, Stop):
                        env["exhausted"][i-1] = True
                while len(env["caches"][i-1]) > 0:
                    yield env["caches"][i-1].pop(0)

        return _upstream() 

    def _rectified_result(self, env):
        if self.amount_result == 0:
            return ()
        res = list(filter(lambda x: x != (), env["results"]))
        if self.amount_result == 1:
            return res[0]
        return tuple(res)
        

    def step(self, env, upstream, downstream):
        res = self.processors[-1].step( env["envs"][-1]
                                      , self._upstream( env, upstream, downstream
                                                      , self.amount_procs - 1)
                                      , self._downstream( env, downstream
                                                        , self.amount_procs - 1)
                                      )       

        if isinstance(res, Resume):
            return Resume()

        env["results"][-1] = res.result

        if isinstance(res, Stop):
            if len(list(filter(lambda x: x != (), env["results"]))) != self.amount_result:
                raise RuntimeError("Last stream processor signals stop,"
                                   " but there are not enough results.")

        r = self._rectified_result(env)
        if isinstance(res, MayResume):
            return MayResume(r)
        return Stop(r)

class ParallelStreamProcessor(ComposedStreamProcessor):
    def __init__(self, processors):
        tin = Type.get(*[p.type_in() for p in processors])
        tout = Type.get(*[p.type_out() for p in processors])

        super(ParallelStreamProcessor, self).__init__(tin, tout, processors)

        self.amount_in = len(list(filter(lambda x: x.type_in() != (), self.processors)))
        self.amount_out = len(list(filter(lambda x: x.type_out() != (), self.processors)))

    def get_initial_env(self, *params):
        env = super(ParallelStreamProcessor, self).get_initial_env(*params)
        env["caches_in"] = [[]] * self.amount_procs
        env["caches_out"] = [[]] * self.amount_procs
        return env

    def _downstream(self, env, i):
        """
        Downstream from i't processor.
        """
        assert i < self.amount_procs
        assert i >= 0

        def _downstream(val):
            env["caches_out"][i].append(val)
        return _downstream

    def _upstream(self, env, upstream, i):
        """
        Upstream to i't processor.
        """
        assert i < self.amount_procs
        assert i >= 0

        assert self.processors[i].type_in() != ()

        while True:
            while len(env["caches_in"][i]) == 0:
                self._fill_cache_in(env, upstream)
            while len(env["caches_in"][i]) > 0:
                yield env["caches_in"][i].pop(0)

    def _fill_cache_in(self, env, upstream):
        assert self.amount_in > 0

        if self.amount_in == 1:
            res = [next(upstream)]
        else:
            res = list(next(upstream))

        for p,i in zip(self.processors, range(0, self.amount_procs)):
            if p.type_in() != ():
                env["caches_in"][i].append(res.pop(0))

    def _flush_cache_out(self, env, downstream):
        if self.amount_out == 0:
            return

        resume = True
        while resume:
            res = [] 
            for p,cache in zip(self.processors, env["caches_out"]):  
                if p.type_out() == ():
                    continue
                if len(cache) == 0:
                    resume = False
                    break
                res.append(cache.pop(0))
            if resume:
                if self.amount_out == 1:
                    downstream(res[0])
                else:
                    downstream(tuple(res)) 

        for r, cache in zip(res, env["caches_out"]):
            cache.insert(0, r)

    def _rectify_res(self, res):
        assert len(res) == self.amount_result

        if self.amount_result == 1:
            return res[0]

        return tuple(res)

    def step(self, env, upstream, downstream):
        res = []
        stop = False
        must_resume = False
        for p,i in zip(self.processors, range(0, self.amount_procs)):
            r = p.step( env["envs"][i]
                      , self._upstream(env, upstream, i)
                      , self._downstream(env, i))
            stop = stop or isinstance(r, Stop)
            must_resume = must_resume or isinstance(r, Resume)
            if not isinstance(r, Resume) and p.type_result() != ():
                res.append(r.result)

        if stop and must_resume:
            raise RuntimeError("One consumer wants to stop, while another needs to resume.")

        self._flush_cache_out(env, downstream) 

        if must_resume:
            return Resume()

        res = self._rectify_res(res)

        if stop:
            return Stop(res)

        return MayResume(res)
        
class SimpleRuntimeEngine(object):
    """
    Very simple runtime engine, that loops process until either Stop is reached
    or StopIteration is thrown.
    """
    def run(self, process, params):
        assert process.is_runnable()
        assert process.type_init().contains(params)

        upstream = (i for i in [])           
        def downstream(val):
            raise RuntimeError("Process should not send a value downstream")
        
        res = None
        env = process.get_initial_env(*params)
        try:
            while True:
                res = process.step(env, upstream, downstream)
                assert isinstance(res, (Stop, MayResume, Resume))
                if isinstance(res, Stop):
                    return res.result
            
        except StopIteration:
            if isinstance(res, (Stop, MayResume)):
                return res.result
            raise RuntimeError("Process did not return result.")
        finally:
            process.shutdown_env(env)

StreamProcessor.runtime_engine = SimpleRuntimeEngine()
                
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

    def step(self, env, upstream, downstream):
        if not isinstance(env, EnvAndGen):
            env = EnvAndGen(env, self.produce(env))

        downstream(next(env.gen))
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

    def step(self, env, upstream, downstream):
        return self.consume(env, upstream)

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

    def step(self, env, upstream, downstream):
        if not isinstance(env, EnvAndGen):
            env = EnvAndGen(env, self.transform(env, upstream))

        downstream(next(env.gen))
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

    def step(self, env, upstream, downstream):
        """
        Let this stream process run.

        This is a fairly naive implementation. Since one could use differerent
        execution models for the same stream, it is very likely that this method
        will exchanged by various interpreter-like runners.
        """
        def internal_downstream(val):
            env["cache"].append(val)

        def internal_upstream():
            while True:
                while len(env["cache"]) == 0:
                    res = self.producer.step(env["env_producer"], upstream, internal_downstream)
                    if isinstance(res, Stop):
                        raise StopIteration()
                while len(env["cache"]) > 0:
                    yield env["cache"].pop(0) 

        return self.consumer.step(env["env_consumer"], internal_upstream(), downstream)


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
    if isinstance(left, MixedStreamProcessor) or isinstance(right, MixedStreamProcessor):
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

class _ComposedStreamProcessor(object):
    """
    Mixin for all composed stream parts.

    Handles initialisation and shutdown of the sub parts.
    """
    def __init__(self, types, *parts):
        assert ALL(isinstance(p, StreamProcessor) for p in parts)

        if types is not None:
            super(_ComposedStreamProcessor, self).__init__(*types)

        self.parts = list(parts)
 
    def get_initial_env(self, *params):
        return { "envs" : [part.get_initial_env() for part in self.parts] }
    def shutdown_env(self, env):
        [v[0].shutdown_env(v[1]) for v in zip(self.parts, env["envs"])] 

class FusePipe(_ComposedStreamProcessor, Pipe):
    """
    A pipe build from two other pipes.
    """
    def __init__(self, *pipes):
        assert len(pipes) >= 2

        tout = FusePipe._infere_type_out(pipes)
        super(FusePipe, self).__init__( ( pipes[0].type_init() * pipes[1].type_init()
                                        , pipes[0].type_in(), tout )
                                      , *pipes)

    @staticmethod
    def _infere_type_out(pipes):
        cur = pipes[0].type_arrow() 

        for p in pipes[1:]:
            cur = cur.compose_with(p.type_arrow()) 

        return cur.r_type

    def transform(self, env, upstream):
        for part, e in zip(self.parts, env["envs"]):
            upstream = part.transform(e, upstream)
        return upstream
    
class AppendPipe(_ComposedStreamProcessor, Producer):
    """
    A producer build from another producer with an appended pipe.
    """
    def __init__(self, producer, pipe):
        assert isinstance(producer, Producer)
        assert isinstance(pipe, Pipe)

        tout = AppendPipe._infere_type_out(producer, pipe)
        super(AppendPipe, self).__init__( ( producer.type_init() * pipe.type_init()
                                          , tout )
                                        , producer, pipe)


    @staticmethod
    def _infere_type_out(producer, pipe):
        return pipe.type_arrow()(producer.type_out())

    def produce(self, env):
        upstream = self.parts[0].produce(env["envs"][0])
        return self.parts[1].transform(env["envs"][1], upstream)

class PrependPipe(_ComposedStreamProcessor, Consumer):
    """
    A consumer build from another consumer with a prepended pipe.
    """
    def __init__(self, pipe, consumer):
        assert isinstance(pipe, Pipe)
        assert isinstance(consumer, Consumer)

        super(PrependPipe, self).__init__( ( pipe.type_init() * consumer.type_init()
                                           , consumer.type_result(), pipe.type_in() )
                                         , pipe, consumer)

    def type_in(self):
        return self.parts[0].type_in()

    def get_initial_env(self, *params):
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
    if isinstance(top, StreamProcessor) or isinstance(bottom, StreamProcessor):
        return stack_mixed(top, bottom)
    
    raise TypeError("Can't stack %s and %s" % (top, bottom))

def stack_pipe(top, bottom):
    pipes = [top] if not isinstance(top, StackPipe) else top.parts
    if isinstance(bottom, StackPipe):
        pipes += bottom.parts
    else:
        pipes.append(bottom)

    return StackPipe(*pipes)

def fuse_stack_pipes(left, right):
    fused = [l >> r for l, r in zip(left.parts, right.parts)]
    return StackPipe(*fused)

def stack_producer(top, bottom):
    producers = [top] if not isinstance(top, StackProducer) else top.parts
    if isinstance(bottom, StackProducer):
        producers += bottom.parts
    else:
        producers.append(bottom)
    return StackProducer(*producers)

def stack_consumer(top, bottom):
    consumers = [top] if not isinstance(top, StackConsumer) else top.parts
    if isinstance(bottom, StackConsumer):
        consumers += bottom.parts
    else:
        consumers.append(bottom)

    return StackConsumer(*consumers)

class StackPipe(_ComposedStreamProcessor, Pipe):
    """
    Some pipes stacked onto each other.
    """
    def __init__(self, *pipes):
        assert len(pipes) >= 2

        tinit = reduce(lambda x,y: x * y, (p.type_init() for p in pipes))
        tin = reduce(lambda x,y: x * y, (p.type_in() for p in pipes))
        tout = reduce(lambda x,y: x * y, (p.type_out() for p in pipes))

        super(StackPipe, self).__init__( (tinit, tin, tout), *pipes)

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

class StackProducer(_ComposedStreamProcessor, Producer):
    """
    Some producers stacked onto each other.

    Will stop to produce if the first producer stops to produce values.
    """
    def __init__(self, *producers):
        assert len(producers) >= 2

        tinit = reduce(lambda x,y: x * y, (p.type_init() for p in producers))
        tout = reduce(lambda x,y: x * y, (p.type_out() for p in producers))
        super(StackProducer, self).__init__( (tinit, tout), *producers)

    def produce(self, env):
        gens = []
        for part, e in zip(self.parts, env["envs"]):
            gens.append(part.produce(e))

        while True:
            vals = []
            for gen in gens:
                vals.append(next(gen))
            yield tuple(vals)

class StackConsumer(_ComposedStreamProcessor, Consumer):
    """
    Some consumers stacked onto each other. 
    """
    def __init__(self, *consumers):
        assert len(consumers) >= 2

        tinit = reduce(lambda x,y: x * y, (c.type_init() for c in consumers))
        tresult = reduce(lambda x,y: x * y, (c.type_result() for c in consumers))
        tin = reduce(lambda x,y: x * y, (c.type_in() for c in consumers))
        super(StackConsumer, self).__init__( (tinit, tresult, tin), *consumers)

    def get_initial_env(self, *params):
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


class MixedStreamProcessor(StreamProcessor):
    """
    A stacked stream part that contains producers, pipes and consumers as well.

    Can't be executed as is, just used during construction of stream processes.
    """
    def __init__(self, *parts):
        self.parts = list(parts)

        consumers = list(filter( lambda x: not isinstance(x, (Producer, StreamProcess))
                          , self.parts))
        if len(consumers) > 0:
            tin = reduce( lambda x,y: x * y
                             , (p.type_in() for p in consumers))
        else:
            tin = () 

        producers = list(filter( lambda x: not isinstance(x, (Consumer, StreamProcess))
                          , self.parts))
        if len(producers) > 0:
            tout = reduce( lambda x,y: x * y
                              , (p.type_out() for p in producers))
        else:
            tout = () 

        tinit = reduce(lambda x,y: x * y, (p.type_init() for p in parts))
        tresult = reduce(lambda x,y: x * y, (p.type_result() for p in parts))

        super(MixedStreamProcessor, self).__init__(tinit, tresult, tin, tout)

    def get_initial_env(self, *params):
        raise RuntimeError("Do not attempt to run a ClosedEndStackPipe!!")

    def shutdown_env(self, env):
        raise RuntimeError("Do not attempt to run a ClosedEndStackPipe!!")

def stack_mixed(top, bottom):
    stacked_classes = MixedStreamProcessor, StackProducer, StackConsumer, StackPipe
    parts = [top] if not isinstance(top, stacked_classes) else top.parts
    if isinstance(bottom, stacked_classes):
        parts += bottom.parts
    else:
        parts.append(bottom)

    return MixedStreamProcessor(*parts)

def compose_mixed_stream_parts(left, right):
    if isinstance(left, StackProducer):
        return compose_stack_producer_mixed_stream(left, right)
    if isinstance(right, StackConsumer):
        return compose_mixed_stream_stacked_consumer(left, right)
    if isinstance(left, MixedStreamProcessor):
        if isinstance(right, MixedStreamProcessor):
            return compose_two_mixed_streams(left, right)
        return compose_mixed_stream_consumer(left, right)
    if isinstance(right, MixedStreamProcessor):
        return compose_producer_mixed_stream(left, right)

    raise TypeError("Can't compose '%s' and '%s'" % (left, right))
       

def compose_stack_producer_mixed_stream(left, right):
    parts = []
    r_len = len(right.parts)
    r_i = 0

    for pr in left.parts:
        r_i, r_cur = _get_next_consuming(r_len, r_i, right.parts, parts)
        assert r_cur is not None
        parts.append(pr >> r_cur)
    parts += right.parts[r_i:]

    assert len(parts) == r_len

    return _stack_stream_parts(parts)
    

def compose_mixed_stream_stacked_consumer(left, right):
    parts = []
    l_len = len(left.parts)
    l_i = 0

    for co in right.parts:
        l_i, l_cur = _get_next_producing(l_len, l_i, left.parts, parts)
        assert l_cur is not None
        parts.append(l_cur >> co)
    parts += left.parts[l_i:]

    assert len(parts) == l_len

    return _stack_stream_parts(parts) 

def compose_two_mixed_streams(left, right):
    parts = []
    l_i = 0
    r_i = 0
    l_len = len(left.parts)
    r_len = len(right.parts)

    # Combine everything that produces from the left
    # with everything that consumes from the right in
    # the order of appeareance.
    l_cur, r_cur = None, None
    while l_i < l_len and r_i < r_len:
        l_i, l_cur = _get_next_producing(l_len, l_i, left.parts, parts)
        r_i, r_cur = _get_next_consuming(r_len, r_i, right.parts, parts)

        # Check whether there is stuff we could combine.
        if l_cur is not None and r_cur is not None:
            parts.append(l_cur >> r_cur)
            l_cur, r_cur = None, None
        elif l_cur is not None:
            parts.append(l_cur)
            l_cur = None
        elif r_cur is not None:
            parts.append(r_cur)
            r_cur = None

    parts += left.parts[l_i:]
    parts += right.parts[r_i:]

    assert l_cur is None 
    assert r_cur is None 

    return _stack_stream_parts(parts)

def compose_producer_mixed_stream(left, right):
    parts = []
    r_i, r_cur = _get_next_consuming(len(right.parts), 0, right.parts, parts)
    assert r_cur is not None
    parts.append(left >> r_cur)
    parts += right.parts[r_i:]

    return _stack_stream_parts(parts)

def compose_mixed_stream_consumer(left, right):
    parts = []
    l_i, l_cur = _get_next_producing(len(left.parts), 0, left.parts, parts)
    assert l_cur is not None
    parts.append(l_cur >> right)
    parts += left.parts[l_i:]

    return _stack_stream_parts(parts) 
   
###############################################################################
#
# Helpers for composing stacked stream parts
#
###############################################################################

def _get_next_not_instance(l, i, search, dump, cls):
    while True and i < l:
        cur = search[i]
        if not isinstance(cur, cls):
            return i+1,cur
        dump.append(cur) 
        i += 1
    return l,None

def _get_next_consuming(l, i, search, dump):
    return _get_next_not_instance(l, i, search, dump, (Producer,StreamProcess))

def _get_next_producing(l, i, search, dump):
    return _get_next_not_instance(l, i, search, dump, (Consumer, StreamProcess))
 
def _stack_stream_parts(parts):
    all_producers = True
    all_consumers = True
    all_processes = True
    for part in parts:
        all_producers = all_producers and isinstance(part, Producer)
        all_consumers = all_consumers and isinstance(part, Consumer)
        all_processes = all_processes and isinstance(part, StreamProcess)

    if all_processes:
        producers = []
        consumers = []

        for part in parts:
            p = part.producer
            c = part.consumer

            if isinstance(c, PrependPipe):
                p = p >> c.parts[0]
                c = c.parts[1]

            producers.append(p)
            consumers.append(c)

        sp = reduce(lambda t,b: t * b, producers)
        sc = reduce(lambda t,b: t * b, consumers)

        return sp >> sc

    if all_producers:
        return StackProducer(*parts)
    if all_consumers:
        return StackConsumer(*parts)

    return MixedStreamProcessor(*parts)
    
        

