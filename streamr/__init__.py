# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

# Common exports
from .simple import ( Producer, Consumer, Pipe, ConstP, ListP, ListC, pipe
                    , transformation, filter_p, tee, nop)
from .core import Resume, MayResume, Stop, StreamProcessor

# Engine initialisations
from streamr.composition import SimpleCompositionEngine
StreamProcessor.composition_engine = SimpleCompositionEngine()

from streamr.runtime import SimpleRuntimeEngine 
StreamProcessor.runtime_engine = SimpleRuntimeEngine()

__all__ = [ "Resume", "MayResume", "Stop", "Producer", "Consumer", "Pipe"
          , "ConstP", "ListP", "ListC", "pipe", "filter_p", "transformation"
          , "tee", "nop" ]
