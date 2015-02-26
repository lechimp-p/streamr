# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import StreamProcessor, Stop, Resume, MayResume

class SimpleRuntimeEngine(object):
    """
    Very simple runtime engine, that loops process until either Stop is reached
    or StopIteration is thrown.
    """
    def run(self, process, params):
        assert process.is_runnable()
        assert process.type_init().contains(params)

        upstream = (i for i in [])           
        def downstream(val):
            raise RuntimeError("Process should not send a value downstream")
        
        res = None
        env = process.get_initial_env(*params)
        try:
            while True:
                res = process.step(env, upstream, downstream)
                assert isinstance(res, (Stop, MayResume, Resume))
                if isinstance(res, Stop):
                    return res.result
            
        except StopIteration:
            if isinstance(res, (Stop, MayResume)):
                return res.result
            raise RuntimeError("Process did not return result.")
        finally:
            process.shutdown_env(env)

StreamProcessor.runtime_engine = SimpleRuntimeEngine()

