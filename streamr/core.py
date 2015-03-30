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

from .types import Type, ArrowType, ProductType, ALL, sequence, unit

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
        return "%s -> %s : %s" % ( self.type_init(), self.type_result()
                                 , self.type_arrow())

    def get_initial_env(self, params):
        """
        Initialize a new runtime environment for the stream processor.

        params adheres to type_init.
        """
        # For processors with single init param.
        if len(params) == 1:
            params = params[0]
        if not self.type_init().contains(params):
            raise TypeError("Expected value of type '%s' got '%s'" % 
                            (self.type_init(), params))

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
    def __init__(self, result = ()):
        self.result = result

class MayResume(object):
    def __init__(self, result = ()):
        self.result = result

class Exhausted(BaseException):
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
    def __init__(self, type_in, type_out, processors, substitutions):
        # It could be the case, that there are multiple processors
        # where one processors requires a tuple type and the other
        # processors expect unit. The tuple then needs special
        # treatment in get_initial_env as it is not wrapped enough. 
        # to make this logic work. 
        self.special_get_env_treatment = False
        cnt_unit = 0
        cnt_product = 0
        for p in processors:
            assert isinstance(p, StreamProcessor)
            tinit = p.type_init()
            if isinstance(tinit, ProductType):
                cnt_product += 1
            elif tinit == unit:
                cnt_unit += 1
        if cnt_product == 1 and cnt_product + cnt_unit == len(processors):
            self.special_get_env_treatment = True

        tinit = (Type.get(*[p.type_init() for p in processors])
                     .substitute_vars(substitutions))
        tresult = (Type.get(*[p.type_result() for p in processors])
                       .substitute_vars(substitutions))

        super(ComposedStreamProcessor, self).__init__(tinit, tresult, type_in, type_out)

        self.processors = list(processors)
 
    def get_initial_env(self, params):
        super(ComposedStreamProcessor, self).get_initial_env(params)

        envs = []

        # Counter on the position of the next param to consume
        # for the initalisation of the processors.
        i = 0
        for p in self.processors:
            tinit = p.type_init()
            if tinit is unit:
                envs.append(p.get_initial_env(()))
                continue

            if isinstance(tinit, ProductType):
                # See comment in constructor
                if self.special_get_env_treatment:
                    assert tinit.contains(params)
                    envs.append(p.get_initial_env(params))
                    continue

                assert tinit.contains(params[i])
                envs.append(p.get_initial_env(params[i]))
            else:
                assert tinit.contains(params[i])
                envs.append(p.get_initial_env((params[i],)))
            i += 1
        
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
        tarr, substitutions = sequence([p.type_arrow() for p in processors], True) 

        super(SequentialStreamProcessor, self).__init__( tarr.type_in()
                                                       , tarr.type_out()
                                                       , processors
                                                       , substitutions)

    def get_initial_env(self, params):
        env = super(SequentialStreamProcessor, self).get_initial_env(params)
        env["rt"] = self.runtime_engine.get_initial_env_for_seq(self.processors)
        return env

    def step(self, env, await, send):
        par = self.runtime_engine.RT( self.processors, env["envs"], env["rt"]
                                    , await, send)
        return self.runtime_engine.step_seq(par)


class ParallelStreamProcessor(ComposedStreamProcessor):
    """
    A processor that processes it subprocessors in parallel.
    """
    def __init__(self, processors):
        tin = Type.get(*[p.type_in() for p in processors])
        tout = Type.get(*[p.type_out() for p in processors])

        super(ParallelStreamProcessor, self).__init__(tin, tout, processors, {})

    def get_initial_env(self, params):
        env = super(ParallelStreamProcessor, self).get_initial_env(params)
        env["rt"] = self.runtime_engine.get_initial_env_for_par(self.processors)
        return env

    def step(self, env, await, send):
        par = self.runtime_engine.RT( self.processors, env["envs"], env["rt"]
                                    , await, send)
        return self.runtime_engine.step_par(par)

def subprocess(process):
    return Subprocess(process) 

class Subprocess(StreamProcessor):
    def __init__(self, process):
        ok = (isinstance(process, StreamProcessor) and
              process.type_in() == () and
              process.type_out() == () and
              process.type_init() != () and
              process.type_result() != ())
        if not ok: 
            raise TypeError("Can't create a subprocess from '%s'" % process)

        self.process = process
        super(Subprocess, self).__init__( (), ()
                                        , process.type_init()
                                        , process.type_result())

    def get_initial_env(self, params):
        super(Subprocess, self).get_initial_env(params)
        return self.runtime_engine.get_initial_env_for_sub(self.process)

    def step(self, env, await, send):
        par = self.runtime_engine.RT( self.process, (), env
                                    , await, send)
        return self.runtime_engine.step_sub(par)


