# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import StreamProcessor, SequentialStreamProcessor, ParallelStreamProcessor


class SimpleCompositionEngine(object):
    """
    Very simple engine to provide composition operations for stream processors.

    Could be switched for a more sophisticated engine, e.g. for performance purpose.
    """
    def compose_sequential(self, left, right):
        not_composable = ( left.is_consumer() or 
                           right.is_producer() or
                           left.is_runnable() or 
                           right.is_runnable() or 
                           not self._composable(left, right))

        if not_composable:
            raise TypeError("Can't compose %s and %s." % (left, right))

        return SequentialStreamProcessor([left, right])

    def compose_parallel(self, top, bottom):
        return ParallelStreamProcessor([top, bottom])

    @staticmethod
    def _composable(l, r):
        try:
            l.type_arrow().__mod__(r.type_arrow())
            return True
        except TypeError:
            return False
     

StreamProcessor.composition_engine = SimpleCompositionEngine()


