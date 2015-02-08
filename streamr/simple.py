# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .base import Producer

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
