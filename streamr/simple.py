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
        return self.value
    def can_produce(self, state):
        return True

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
        value = await()
        return self.fun(value)

def statelessPipe(type_in, type_out):
    return lambda fun: StatelessPipe(type_in, type_out, fun)
