# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import StreamProcessor, Stop, MayResume, Exhausted, subprocess
from .types import Type

###############################################################################
#
# Some classes that simplify the usage of StreamProcessor
#
###############################################################################

class Producer(StreamProcessor):
    """
    A producer is the source for a stream, that is, it produces new data for
    the downstream without an upstream and creates no result.
    """
    def __init__(self, type_init, type_out):
        super(Producer, self).__init__(type_init, (), (), type_out)

    def step(self, env, await, send):
        return self.produce(env, send)

    def produce(self, env, send):
        """
        Generate new data to be send downstream.

        The data must have the type declared with type_out.
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
        return self.consume(env, await)

    def consume(self, env, await):
        """
        Consume data from upstream.

        Consume must be able to process values with types according to type_in. 
        Could return a result. 
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

    def step(self, env, await, send):
        return self.transform(env, await, send)

    def transform(self, env, await, send):
        """
        Take data from upstream and generate data for downstream. Must adhere to
        the types from type_in and type_out.
        """
        raise NotImplementedError("Pipe::transform: implement "
                                  "me for class %s!" % type(self))


###############################################################################
#
# Some processors to perform simple and common tasks. 
#
###############################################################################

class _NoValue(object):
    """ 
    None could be some value, so we need something else to mark a non value.
    """
    pass


class ConstProducer(Producer):
    """
    A const producer sends a custom value downstream infinitely.

    The value to be produced could either be given to the constructor
    or set via get_initial_env.
    """
    def __init__(self, value = _NoValue, value_type = _NoValue, amount = _NoValue):
        """
        Either pass a value for the const value to be produced or
        a type of the value if the value should be set via get_initial_env.
        """
        if value is _NoValue and value_type is _NoValue:
            raise TypeError("Either pass value or value_type.")

        self.value = value
        self.amount = amount
        if value is not _NoValue:
            super(ConstProducer, self).__init__((), type(value))
        else:
            super(ConstProducer, self).__init__(value_type, value_type)

    def get_initial_env(self, params):
        super(ConstProducer, self).get_initial_env(params)
        if self.value is not _NoValue:
            return [self.value, 0]

        return [params[0], 0]

    def produce(self, env, send):
        if self.amount != _NoValue:
            if env[1] == self.amount:
                return Stop()
            env[1] += 1
        send(env[0])
        return MayResume()

def const(value = _NoValue, value_type = _NoValue, amount = _NoValue):
    """
    A const producer sends a custom value downstream infinitely.

    The value to be produced could either be given to the constructor
    or set via get_initial_env.
    """
    return ConstProducer(value, value_type, amount)


class ListProducer(Producer):
    """
    The ListProducerroducer sends the values from a list downstream.

    The list could either be given to the constructor or get_initial_env.
    """
    def __init__(self, vlist = _NoValue, item_type = _NoValue):
        """
        Pass a list of values as vlist or the type of items the list should
        contain, if the list of values should be given to get_initial_env.

        Throws, when vlist contains values of different types.

        Makes a copy of the supplied list.
        """
        if vlist is _NoValue and item_type is _NoValue:
            raise TypeError("Either pass value or item_type.")

        if vlist is _NoValue:
            self.vlist = vlist
            self.item_type = Type.get(item_type)
            super(ListProducer, self).__init__([item_type], item_type)
        else:
            if len(vlist) == 0:
                raise ValueError("vlist is empty list.")

            item_type = Type.infer(vlist[0])
            if not Type.get([item_type]).contains(vlist):
                raise TypeError("Expected list with items of type '%s'only." % item_type)
            self.vlist = list(vlist)
            super(ListProducer, self).__init__((), item_type)

    def get_initial_env(self, params):
        super(ListProducer, self).get_initial_env(params)
        if self.vlist is not _NoValue:
            vlist = self.vlist
        else:
            vlist = list(params[0])

        return { "index" : 0, "list" : vlist, "len" : len(vlist) }

    def produce(self, env, send):
        if env["index"] >= env["len"]:
            raise Exhausted()

        send(env["list"][env["index"]])
        env["index"] += 1

        if env["index"] == env["len"]:
            return Stop()        

        return MayResume()

def from_list(vlist = _NoValue, item_type = _NoValue):
    """
    A producer that sends the values from a list downstream.

    The list could either be given to the constructor or on initialisation.
    """
    return ListProducer(vlist, item_type)


class ListConsumer(Consumer):
    """
    Puts consumed values to a list.

    Either appends to the list given in the constructor or creates a fresh list 
    in get_initial_env.
    """
    def __init__(self, append_to = None, max_amount = None):
        tvar = Type.get()
        self.append_to = append_to
        self.max_amount = max_amount 
        if append_to is None:
            super(ListConsumer, self).__init__((), [tvar], tvar)
        else:
            super(ListConsumer, self).__init__((), (), tvar)

    def get_initial_env(self, params):
        super(ListConsumer, self).get_initial_env(params)
        if self.append_to is None:
            return [False, []]
        return [False]

    def consume(self, env, await):
        if self.append_to is None:
            if not env[0]:
                env[0] = True
                return MayResume([])
            env[1].append(await())
            if self.max_amount is not None and len(env[1]) >= self.max_amount:
                return Stop(env[1])
            return MayResume(env[1])

        if not env[0]:
            env[0] = True
            return MayResume()
        if self.max_amount is not None and len(self.append_to) > self.max_amount:
            return Stop()
        self.append_to.append(await())
        return MayResume()

def to_list(append_to = None, max_amount = None):
    """
    Put consumed values to a list.

    Appends to an exisiting list if append_to is used. Creates a fresh
    list and returns in as result otherwise.
    """
    return ListConsumer(append_to, max_amount)
 

class LambdaPipe(Pipe):
    """
    A pipe that processes each piece of data with a lambda.
    """
    def __init__(self, type_in, type_out, _lambda):
        super(LambdaPipe, self).__init__((), type_in, type_out)
        self._lambda = _lambda
    def transform(self, env, await, send):
        self._lambda(await, send)
        return MayResume()  

def pipe(type_in, type_out, _lambda = None):
    """
    Decorate a function to become a pipe that works via await and send and
    resumes infinitely.
    """
    if _lambda is None:
        def decorator(fun):
            return LambdaPipe(type_in, type_out, fun)

        return decorator
    return LambdaPipe(type_in, type_out, _lambda)

def transformation(type_in, type_out, _lambda = None):
    """
    Decorate a function to be a pipe.
    """
    if _lambda is None:
        def decorator(fun):
            l = lambda await, send: send(fun(await()))
            return LambdaPipe(type_in, type_out, l)

        return decorator
    return LambdaPipe(type_in, type_out, _lambda)


class FilterPipe(Pipe):
    """
    A pipe that only lets data pass that matches a predicate.
    """
    max_tries_per_step = 10

    def __init__(self, type_init, type_io):
        super(FilterPipe, self).__init__(type_init, type_io, type_io)

    def transform(self, env, await, send):
        for _ in range(0, self.max_tries_per_step):
            val = await()
            if self.predicate(env, val):
                send(val)
                return MayResume()
        return MayResume()

    def predicate(self, env, val):
        """
        Check whether the value is ok. Return bool.
        """
        raise NotImplementedError("FilterPipe::predicate: implement "
                                  "me for class %s!" % type(self))

class LambdaFilterPipe(FilterPipe):
    """
    A filter whos predicate is a lambda.
    """
    def __init__(self, type_io, _lambda):
        super(LambdaFilterPipe, self).__init__((), type_io)
        self._lambda = _lambda
    def predicate(self, env, val):
        return self._lambda(val)

def pass_if(type_io, _lambda = None):
    """
    Decorate a predicate and get a pipe that only lets values
    through when the predicate matches.
    """
    if _lambda is None:
        def decorator(fun):
            return LambdaFilterPipe(type_io, fun)

        return decorator
    return LambdaFilterPipe(type_io, _lambda)


def tee():
    _any = Type.get()
    @transformation(_any, (_any, _any))
    def tee(v):
        return v,v
    return tee


def nop():
    _any = Type.get()
    @transformation(_any, _any)
    def nop(v):
        return v
    return nop


def maps(a_pipe):
    """
    Maps a pipe over lists from upstream and sends the resulting list
    downstream.

    The pipe is initialized once per list and produces one result per list,
    which are collected in a result list.
    """
    if a_pipe.type_in() == () or a_pipe.type_out() == ():
        raise TypeError("Expected pipe as argument.")

    tinit = a_pipe.type_init()
    tresult = a_pipe.type_result()
    tin = a_pipe.type_in()
    tout = a_pipe.type_out()
 
    mapper = subprocess( from_list(item_type = tin) >>
                         a_pipe >>
                         to_list())

    if a_pipe.type_init() != ():
        mapper = nop * const(value_type = tinit) >> mapper

    if a_pipe.type_result() != ():
        mapper = mapper >> to_list() * nop
       
    return mapper 




# Maybe to be reused for generator style producers
#                
#class EnvAndGen(object):
#    def __init__(self, env, gen):
#        self.env = env
#        self.gen = gen
#
#    def step(self, env, await, send):
#        if not isinstance(env, EnvAndGen):
#            env = EnvAndGen(env, self.produce(env))
#
#        send(next(env.gen))
#        return MayResume(())
#
#
# ... and consumers
#    def step(self, env, await, send):
#        def _upstream():
#            while True:
#                val = await()
#                yield val
#
#        return self.consume(env, _upstream())
#
# ... and pipes 
#    def step(self, env, await, send):
#        if not isinstance(env, EnvAndGen):
#            def _upstream():
#                while True:
#                    val = await()
#                    yield val
#
#            env = EnvAndGen(env, self.transform(env, _upstream()))
#
#        send(next(env.gen))
#        return MayResume(())        




