# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .base import Producer, Consumer

class ListC(Consumer):
    def __init__(self, type, amount = None):
        self.type = type
        self.amount = amount

    def type_in(self):
        return self.type

    def get_initial_env(self):
        return []
    def shutdown_env(self, env):
        pass

    def consume(self, l, await):
        l = []
        while True:
            try:
                value = await()
            except StopIteration:
                return l
            l.append(value)
            if not self.amount is None and len(l) == self.amount:
                return l
