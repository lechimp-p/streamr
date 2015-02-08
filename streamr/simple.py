# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .base import Producer, Pipe

class RepeatP(Producer):
    def __init__(self, value):
        self.value = value
        self.type = type(value)

    def type_out(self):
        return self.type

    def get_initial_state(self):
        return None
    def shutdown_state(self, state):
        pass

    def produce(self, state):
        while True:
            yield self.value

class StatelessPipe(Pipe):
    def __init__(self, type_in, type_out, fun):
        self._type_in = type_in
        self._type_out = type_out
        self.fun = fun
    
    def type_in(self):
        return self._type_in
    def type_out(self):
        return self._type_out

    def get_initial_state(self):
        return None
    def shutdown_state(self, state):
        pass

    def transform(self, await, state):
        return self.fun(await)

def statelessPipe(type_in, type_out):
    return lambda fun: StatelessPipe(type_in, type_out, fun)

def simpleTransformation(type_in, type_out):
    def call_transformer(fun, await):
        while True:
            value = await()
            yield fun(value)

    return lambda fun: StatelessPipe( 
                            type_in, type_out
                            , lambda await: call_transformer(fun, await)) 

def chunks(type, amount):
    @statelessPipe(type, tuple(amount * [type]))
    def chunks(await):
        while True:
            values = tuple([await() for i in range(0,amount)])
            yield values
    return chunks
