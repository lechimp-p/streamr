# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import StreamProcessor, Stop, Resume, MayResume, Exhausted 
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


class ConstP(Producer):
    """
    A const producer sends a custom value downstream infinitely.

    The value to be produced could either be given to the constructor
    or set via get_initial_env.
    """
    def __init__(self, value = _NoValue, value_type = _NoValue):
        """
        Either pass a value for the const value to be produced or
        a type of the value if the value should be set via get_initial_env.
        """
        if value is _NoValue and value_type is _NoValue:
            raise TypeError("Either pass value or value_type.")

        self.value = value
        if value is not _NoValue:
            super(ConstP, self).__init__((), type(value))
        else:
            super(ConstP, self).__init__(value_type, value_type)

    def get_initial_env(self, *value):
        if self.value is not _NoValue:
            if len(value) != 0:
                raise TypeError("Value already passed in constructor.")
            return self.value

        if len(value) != 1 or not self.type_init().contains(value[0]):
            raise TypeError("Expected value of type '%s' not"
                            " '%s'" % (self.type_init(), value))

        return value[0]

    def produce(self, env, send):
        send(env)
        return MayResume()

class ListP(Producer):
    """
    The ListProducer sends the values from a list downstream.

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
            super(ListP, self).__init__([item_type], item_type)
        else:
            if len(vlist) == 0:
                raise ValueError("vlist is empty list.")

            item_type = self._getItemType(vlist)
            self._checkList(vlist, item_type)
            self.vlist = list(vlist)
            super(ListP, self).__init__((), item_type)

    @staticmethod
    def _getItemType(vlist):
        return Type.get(type(vlist[0]))

    @staticmethod
    def _checkList(vlist, item_type):
        for v in vlist:
            if not item_type.contains(v):
                raise TypeError("Expected item of type '%s', got '%s'" 
                                % (item_type, v))

    def get_initial_env(self, *vlist):
        if self.vlist is not _NoValue:
            if len(vlist) != 0:
                raise TypeError("Value already passed in constructor.")
            vlist = self.vlist
        else:
            if len(vlist) != 1 or not self.type_init().contains(vlist[0]):
                raise TypeError("Expected list of type '%s' not"
                                " '%s'" % (self.type_init(), vlist))
            vlist = list(vlist[0])
            self._checkList(vlist, self.item_type)

        return { "index" : 0, "list" : vlist, "len" : len(vlist) }

    def produce(self, env, send):
        if env["index"] >= env["len"]:
            raise Exhausted()

        send(env["list"][env["index"]])
        env["index"] += 1

        if env["index"] == env["len"]:
            return Stop()        

        return MayResume()


class ListC(Consumer):
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
            super(ListC, self).__init__((), [tvar], tvar)
        else:
            super(ListC, self).__init__((), (), tvar)

    def get_initial_env(self):
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
        for i in range(0, self.max_tries_per_step):
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


def filter_p(type_io, _lambda = None):
    """
    Decorate a predicate and get a pipe that only lets values
    through when the predicate matches.
    """
    if _lambda is None:
        def decorator(fun):
            return LambdaFilterPipe(type_io, fun)

        return decorator
    return LambdaFilterPipe(type_io, _lambda)


_any = Type.get()
@transformation(_any, _any)
def tee(v):
    return v,v

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




