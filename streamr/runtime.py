# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import Stop, Resume, MayResume, Exhausted
from .types import unit

class SimpleRuntimeEngine(object):
    """
    Very simple runtime engine, that loops process until either Stop is reached
    or StopIteration is thrown.

    Implements a single threaded demand driven implementation with caches.
    """
    def run(self, process, params):
        assert process.is_runnable()
        assert process.type_init().contains(params)

        def await(val):
            raise RuntimeError("Process should not send a value downstream, "
                               "but send %s" % val)
        def send(val):
            raise RuntimeError("Process should not send a value downstream, "
                               "but send %s" % val)
        
        res = None
        env = process.get_initial_env(*params)
        try:
            while True:
                res = process.step(env, await, send)
                assert isinstance(res, (Stop, MayResume, Resume))
                if isinstance(res, Stop):
                    return res.result
            
        except Exhausted:
            if isinstance(res, (Stop, MayResume)):
                return res.result
            raise RuntimeError("Process did not return result.")
        finally:
            process.shutdown_env(env)

    class _NoRes(object):
        pass

    class RT(object):
        """
        Holds everything that is needed to run a process.
        """
        def __init__(self, processors, envs, rt_env, await, send):
            self.processors = processors
            self.envs = envs
            self.rt_env = rt_env
            self.await = await
            self.send = send

    ###########################################################################
    #
    # Methods for sequential processing.
    #
    ###########################################################################

    def get_initial_env_for_seq(self, processors):
        """
        Get the environment required by the runtime for sequential processing of
        stream processors.
        """
        amount_processors = len(processors)
        return { "caches"     : [[]] * amount_processors
               , "results"    : [self._NoRes()] * amount_processors
               , "exhausted"  : [False] * amount_processors
               , "amount"     : amount_processors
               , "amount_res" : len(list(filter(
                                            lambda x: x.type_result() != unit, 
                                            processors)))
               }

    def step_seq(self, rt):
        """
        Perform a step of the processors sequentially.
        """
        assert isinstance(rt, self.RT)

        amount_procs = len(rt.processors)
        assert rt.rt_env["amount"] == amount_procs

        res = rt.processors[-1].step( rt.envs[-1]
                                    , self._seq_upstream(rt, amount_procs - 1)
                                    , self._seq_downstream(rt, amount_procs - 1)
                                 )       

        if isinstance(res, Resume):
            return Resume()

        rt.rt_env["results"][-1] = res.result

        if isinstance(res, Stop):
            is_Res = lambda x: not isinstance(x, self._NoRes)
            results = list(filter(is_Res, rt.rt_env["results"]))
            if (len(results) != rt.rt_env["amount_res"]):
                raise RuntimeError("Last stream processor signals stop,"
                                   " but there are not enough results.")

        r = self._seq_rectify_result(rt.processors, rt.rt_env)
        if isinstance(res, MayResume):
            if r == ():
                return MayResume()
            return MayResume(r)
        return Stop(r)
        

    def _seq_downstream(self, rt, i):
        """
        Downstream from i'th processor.
        """
        assert isinstance(rt, self.RT)
        assert i < rt.rt_env["amount"]
        assert i >= 0

        if i == rt.rt_env["amount"] - 1:
            return rt.send

        def _send(val):
            rt.rt_env["caches"][i].append(val)

        return _send 

    def _seq_upstream(self, rt, i):
        """
        Upstream to i'th processor.
        """
        assert isinstance(rt, self.RT)
        assert i < rt.rt_env["amount"]
        assert i >= 0

        if i == 0:
            return rt.await

        def _await():
            while len(rt.rt_env["caches"][i-1]) == 0:
                if rt.rt_env["exhausted"][i-1]:
                    raise Exhausted()
                p = rt.processors[i-1]
                us = self._seq_upstream(rt, i-1)
                ds = self._seq_downstream(rt, i-1)
                res = p.step(rt.envs[i-1], us, ds)
                if not isinstance(res, Resume) and p.type_result() != unit:
                    rt.rt_env["results"][i-1] = res.result
                if isinstance(res, Stop):
                    rt.rt_env["exhausted"][i-1] = True
            return rt.rt_env["caches"][i-1].pop(0)

        return _await

    def _seq_rectify_result(self, processors, rt_env):
        res = rt_env["results"]
        if rt_env["amount_res"] == 0:
            return ()
        res = list(filter(lambda x: not isinstance(x, self._NoRes), res)) 
        if rt_env["amount_res"] == 1:
            return res[0]
        return tuple(res)

    ###########################################################################
    #
    # Methods for parallel processing.
    #
    ###########################################################################

    def get_initial_env_for_par(self, processors):
        """
        Get the environment required by the runtime for parallel processing of
        stream processors.
        """
        amount_processors = len(processors)
        return { "caches_in"  : [[]] * amount_processors
               , "caches_out" : [[]] * amount_processors
               , "exhausted"  : [False] * amount_processors
               , "amount"     : amount_processors
               , "amount_res" : len(list(filter(
                                            lambda x: x.type_result() != unit, 
                                            processors)))

                , "amount_in" : len(list(filter(
                                            lambda x: x.type_in() != unit,
                                            processors)))
                , "amount_out": len(list(filter(lambda x: x.type_out() != unit, 
                                            processors)))
               }

    def step_par(self, rt):
        assert isinstance(rt, self.RT)

        amount_procs = len(rt.processors)
        assert rt.rt_env["amount"] == amount_procs

        rt.rt_env["results"] = []
        stop = False
        must_resume = False
        for p,i in zip(rt.processors, range(0, amount_procs)):
            r = p.step( rt.envs[i]
                      , self._par_upstream(rt, i)
                      , self._par_downstream(rt, i)
                      )
            stop = stop or isinstance(r, Stop)
            must_resume = must_resume or isinstance(r, Resume)
            if not isinstance(r, Resume) and p.type_result() != unit:
                rt.rt_env["results"].append(r.result)

        if stop and must_resume:
            raise RuntimeError("One consumer wants to stop, while another needs to resume.")

        self._par_flush_caches_out(rt) 

        if must_resume:
            return Resume()

        res = self._par_rectify_res(rt.processors, rt.rt_env)

        if stop:
            return Stop(res)

        return MayResume(res)


    def _par_downstream(self, rt, i):
        """
        Downstream from i't processor.
        """
        assert isinstance(rt, self.RT)
        assert i < rt.rt_env["amount"]
        assert i >= 0

        def _send(val):
            rt.rt_env["caches_out"][i].append(val)
        return _send

    def _par_upstream(self, rt, i):
        """
        Upstream to i't processor.
        """
        assert isinstance(rt, self.RT)
        assert i < rt.rt_env["amount"]
        assert i >= 0

        def _await():
            while len(rt.rt_env["caches_in"][i]) == 0:
                self._par_fill_caches_in(rt)
            return rt.rt_env["caches_in"][i].pop(0)

        return _await

    def _par_fill_caches_in(self, rt):
        assert isinstance(rt, self.RT)
        assert rt.rt_env["amount_in"] > 0

        if rt.rt_env["amount_in"] == 1:
            res = [rt.await()]
        else:
            res = list(rt.await())

        for p,i in zip(rt.processors, range(0, rt.rt_env["amount"])):
            if p.type_in() != unit:
                rt.rt_env["caches_in"][i].append(res.pop(0))

    def _par_flush_caches_out(self, rt):
        assert isinstance(rt, self.RT)
        if rt.rt_env["amount_out"] == 0:
            return

        resume = True
        while resume:
            res = [] 
            for p,cache in zip(rt.processors, rt.rt_env["caches_out"]):  
                if p.type_out() == unit: 
                    continue
                if len(cache) == 0:
                    resume = False
                    break
                res.append(cache.pop(0))
            if resume:
                if rt.rt_env["amount_out"] == 1:
                    rt.send(res[0])
                else:
                    rt.send(tuple(res)) 

        # Reinsert the values we could not send downstream to cache
        for r, cache in zip(res, rt.rt_env["caches_out"]):
            cache.insert(0, r)

    def _par_rectify_res(self, processors, rt_env):
        res = rt_env["results"]
        assert len(res) == rt_env["amount_res"]

        if rt_env["amount_res"] == 1:
            return res[0]

        return tuple(res)
