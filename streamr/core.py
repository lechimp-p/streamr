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
Those resources are guaranteed being teared down by passing them to teardown 
method after a stream process has run.

Stream processes are consumer driven, that is, a consumer pulls data from
upstream until he has enough data or there is no more data in the upstream.
"""

from .types import Type, ArrowType, ProductType, sequence, unit

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

    def setup(self, params, result):
        """
        Initialize a new runtime environment for the stream processor.

        @param type_result  param
        @param callback     result  Can be used to signal an initial result.

        @return mixed       Environment that will be passed to each step.
        """
        # For processors with single init param.
        if len(params) == 1:
            params = params[0]
        if not self.type_init().contains(params):
            raise TypeError("Expected value of type '%s' got '%s'" % 
                            (self.type_init(), params))

    def teardown(self, env):
        """
        Teardown the environment produced by setup.

        Is guaranteed to be called for every environment created by setup. 
        """
        pass

    def step(self, env, stream):
        """
        Performs the actual processing.

        @param mixed    env     As created by setup. 
        @param Stream   stream  Interface to the stream.

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
# Interface to the stream.
#
###############################################################################

class _NoRes(object):
    """
    We use this to distinguish a non-result from None.
    """
    pass

class Stream(object):
    """
    This interface is passed to the processors to access the stream.
    """
    def await(self):
        """
        Get the next piece of data from upstream.
        """
        raise NotImplementedError
    def send(self, val):
        """
        Send a piece of data downstream.
        """
        raise NotImplementedError
    def result(self, val = _NoRes):
        """
        Signal a result or that there is no result.
        """
        raise NotImplementedError

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
        tinit = (Type.get(*[p.type_init() for p in processors])
                     .substitute_vars(substitutions))
        tresult = (Type.get(*[p.type_result() for p in processors])
                       .substitute_vars(substitutions))

        super(ComposedStreamProcessor, self).__init__(tinit, tresult, type_in, type_out)

        self.processors = list(processors)
 
    def setup(self, params, result):
        super(ComposedStreamProcessor, self).setup(params, result)


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

    def setup(self, params, result):
        super(SequentialStreamProcessor, self).setup(params, result)
        return self.runtime_engine.setup_seq_rt(self.processors, params, result)
    def teardown(self, rt):
        return self.runtime_engine.teardown_seq_rt(rt)
    def step(self, rt, stream):
        return rt.step(stream)


class ParallelStreamProcessor(ComposedStreamProcessor):
    """
    A processor that processes it subprocessors in parallel.
    """
    def __init__(self, processors):
        tin = Type.get(*[p.type_in() for p in processors])
        tout = Type.get(*[p.type_out() for p in processors])
        super(ParallelStreamProcessor, self).__init__(tin, tout, processors, {})

    def setup(self, params, result):
        super(ParallelStreamProcessor, self).setup(params, result)
        return self.runtime_engine.setup_par_rt(self.processors, params, result)
    def teardown(self, rt):
        return self.runtime_engine.teardown_par_rt(rt)
    def step(self, rt, stream):
        return rt.step(stream)


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

    def setup(self, params, result):
        super(Subprocess, self).setup(params, result)
        return self.runtime_engine.setup_sub_rt(self.process, params, result)

    def teardown(self, rt):
        return self.runtime_engine.teardown_par_rt(rt)

    def step(self, rt, stream):
        return rt.step(stream)
