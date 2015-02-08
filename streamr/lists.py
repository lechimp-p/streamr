# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .base import Producer

class ListP(Producer):
    def __init__(self, list):
        self.list = list
        self.length = len(list)

    def get_inital_state(self):
        return 0
    def shutdown_state(self):
        pass

    def produce(self, index):
        return self.list[index]
    def can_produce(self, index):
        return index < self.length

class ListC(Consumer):
    def get_initial_state(self):
        return []
    def shutdown_state(self):
        pass

    def consume(self, value, list):
        list.append(value)
    def can_consume(self, list):
        return MayContinue;
    def result(self, list):
        return list
        
