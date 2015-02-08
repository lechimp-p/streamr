# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .base import Producer, Consumer, MayContinue

class ListP(Producer):
    def __init__(self, type, list):
        self.type = type
        self.list = list
        self.length = len(list)

    def type_out(self):
        return self.type

    def get_initial_state(self):
        return [0]
    def shutdown_state(self, state):
        pass

    def produce(self, index):
        index[0] += 1
        return self.list[index[0]-1]
    def can_produce(self, index):
        return index[0] < self.length

class ListC(Consumer):
    def __init__(self, type):
        self.type = type

    def type_in(self):
        return self.type

    def get_initial_state(self):
        return []
    def shutdown_state(self, state):
        pass

    def consume(self, value, list):
        list.append(value)
    def can_continue(self, list):
        return MayContinue;
    def result(self, list):
        return list
        
