# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

# Common exports
from .base import ( Producer, Consumer, Pipe, const, from_list, to_list, pipe
                  , transformation, pass_if, tee, nop, maps)
from .core import Resume, MayResume, Stop, StreamProcessor, subprocess

# Engine initialisations
from streamr.combination import SimpleCombinationEngine
StreamProcessor.combination_engine = SimpleCombinationEngine()

from streamr.runtime import SimpleRuntimeEngine 
StreamProcessor.runtime_engine = SimpleRuntimeEngine()

__all__ = [ "Resume", "MayResume", "Stop", "Producer", "Consumer", "Pipe"
          , "const", "from_list", "to_list", "pipe", "pass_if", "transformation"
          , "tee", "nop", "subprocess", "maps" ]
