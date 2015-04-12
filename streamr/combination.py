# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import SequentialStreamProcessor, ParallelStreamProcessor


class SimpleCombinationEngine(object):
    """
    Very simple engine to provide combination operations for stream processors.

    Could be switched for a more sophisticated engine, e.g. for performance purpose.
    """
    def combine_sequential(self, left, right):
        not_combinable = ( left.is_consumer() or 
                           right.is_producer() or
                           left.is_runnable() or 
                           right.is_runnable() or 
                           not self._combinable(left, right))

        if not_combinable:
            raise TypeError("Can't combine %s and %s." % (left, right))

        is_sp = lambda x: isinstance(x, SequentialStreamProcessor)
        procs = ((left.processors if is_sp(left) else [left]) +
                 (right.processors if is_sp(right) else [right])) 

        return SequentialStreamProcessor(procs)

    def combine_parallel(self, top, bottom):
        is_pp = lambda x: isinstance(x, ParallelStreamProcessor)
        procs = ((top.processors if is_pp(top) else [top]) +
                 (bottom.processors if is_pp(bottom) else [bottom])) 

        return ParallelStreamProcessor(procs)

    @staticmethod
    def _combinable(l, r):
        try:
            l.type_arrow().__mod__(r.type_arrow())
            return True
        except TypeError:
            return False
