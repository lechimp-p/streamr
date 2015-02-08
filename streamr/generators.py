# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .base import Producer

from itertools import tee

class GeneratorP(Producer):
    def __init__(self, type, generator):
        self.type = type
        self.generator = generator

    def type_out(self):
        return self.type

    class NONE:
        pass

    @staticmethod
    def iterate(gen):
        try:
            val = gen.__next__()
        except StopIteration:
            val = NONE
        return (gen, val)

    def get_initial_state(self):
        self.generator, other = tee(self.generator)
        return self.iterate(other)
    def shutdown_state(self, state):
        pass

    def produce(self, state):
        generator, val = state
        state = self.iterate(state)
        return val
    def can_produce(self, state):
        return state[1] == NONE
