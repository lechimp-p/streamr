# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .base import Producer

from itertools import tee

class GeneratorP(Producer):
    def __init__(self, type, generator):
        self.type = type
        self.generator = generator

    def type_out(self):
        return self.type

    def get_initial_state(self):
        return None
    def shutdown_state(self, state):
        pass

    def produce(self, state):
        self.generator, other = tee(self.generator)
        return other
