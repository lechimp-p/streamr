# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import StreamProcessor, Stop, Resume, MayResume

class SimpleRuntimeEngine(object):
    """
    Very simple runtime engine, that loops process until either Stop is reached
    or StopIteration is thrown.

    Implements a single threaded demand driven implementation with caches.
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

    class _NoRes:
        pass

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
                                            lambda x: x.type_result() != (), 
                                            processors)))
               }

    def step_seq(self, processors, envs, rt_env, upstream, downstream):
        """
        Perform a step of the processors sequentially.
        """
        amount_procs = len(processors)
        assert rt_env["amount"] == amount_procs

        res = processors[-1].step( envs[-1]
                                 , self._seq_upstream( processors, envs, rt_env 
                                                     , upstream, downstream
                                                     , amount_procs - 1)
                                 , self._seq_downstream( processors, envs, rt_env
                                                       , upstream, downstream
                                                       , amount_procs - 1)
                                 )       

        if isinstance(res, Resume):
            return Resume()

        rt_env["results"][-1] = res.result

        if isinstance(res, Stop):
            if (len(list(filter( lambda x: not isinstance(x, self._NoRes)
                              , rt_env["results"])))
                != rt_env["amount_res"]):
                raise RuntimeError("Last stream processor signals stop,"
                                   " but there are not enough results.")

        r = self._seq_rectify_result(processors, rt_env)
        if isinstance(res, MayResume):
            return MayResume(r)
        return Stop(r)
        

    def _seq_downstream(self, processors, envs, rt_env, upstream, downstream, i):
        """
        Downstream from i'th processor.
        """
        assert i < rt_env["amount"]
        assert i >= 0

        if i == rt_env["amount"] - 1:
            return downstream

        def _downstream(val):
           rt_env["caches"][i].append(val)

        return _downstream 

    def _seq_upstream(self, processors, envs, rt_env, upstream, downstream, i):
        """
        Upstream to i'th processor.
        """
        assert i < rt_env["amount"]
        assert i >= 0

        if i == 0:
            return upstream

        def _upstream():
            us = self._seq_upstream( processors, envs, rt_env
                                   , upstream, downstream, i-1)
            ds = self._seq_downstream( processors, envs, rt_env
                                     , upstream, downstream,i-1)
            while True:
                while len(rt_env["caches"][i-1]) == 0:
                    if rt_env["exhausted"][i-1]:
                        raise StopIteration()
                    p = processors[i-1]
                    res = p.step( envs[i-1], us, ds)
                    if not isinstance(res, Resume) and p.type_result() != ():
                        rt_env["results"][i-1] = res.result
                    if isinstance(res, Stop):
                        rt_env["exhausted"][i-1] = True
                while len(rt_env["caches"][i-1]) > 0:
                    yield rt_env["caches"][i-1].pop(0)

        return _upstream() 

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
                                            lambda x: x.type_result() != (), 
                                            processors)))

                , "amount_in" : len(list(filter(
                                            lambda x: x.type_in() != (),
                                            processors)))
                , "amount_out": len(list(filter(lambda x: x.type_out() != (), 
                                            processors)))
               }

    def step_par(self, processors, envs, rt_env, upstream, downstream):
        amount_procs = len(processors)
        assert rt_env["amount"] == amount_procs

        rt_env["results"] = []
        stop = False
        must_resume = False
        for p,i in zip(processors, range(0, amount_procs)):
            r = p.step( envs[i]
                      , self._par_upstream( processors, envs, rt_env
                                          , upstream, downstream, i)
                      , self._par_downstream( processors, envs, rt_env
                                            , upstream, downstream, i)
                      )
            stop = stop or isinstance(r, Stop)
            must_resume = must_resume or isinstance(r, Resume)
            if not isinstance(r, Resume) and p.type_result() != ():
                rt_env["results"].append(r.result)

        if stop and must_resume:
            raise RuntimeError("One consumer wants to stop, while another needs to resume.")

        self._par_flush_caches_out(processors, rt_env, downstream) 

        if must_resume:
            return Resume()

        res = self._par_rectify_res(processors, rt_env)

        if stop:
            return Stop(res)

        return MayResume(res)


    def _par_downstream(self, processors, envs, rt_env, upstream, downstream, i):
        """
        Downstream from i't processor.
        """
        assert i < rt_env["amount"]
        assert i >= 0

        def _downstream(val):
            rt_env["caches_out"][i].append(val)
        return _downstream

    def _par_upstream(self, processors, envs, rt_env, upstream, downstream, i):
        """
        Upstream to i't processor.
        """
        assert i < rt_env["amount"]
        assert i >= 0
        assert processors[i].type_in() != ()

        while True:
            while len(rt_env["caches_in"][i]) == 0:
                self._par_fill_caches_in(processors, envs, rt_env, upstream)
            while len(rt_env["caches_in"][i]) > 0:
                yield rt_env["caches_in"][i].pop(0)

    def _par_fill_caches_in(self, processors, envs, rt_env, upstream):
        assert rt_env["amount_in"] > 0

        if rt_env["amount_in"] == 1:
            res = [next(upstream)]
        else:
            res = list(next(upstream))

        for p,i in zip(processors, range(0, rt_env["amount"])):
            if p.type_in() != ():
                rt_env["caches_in"][i].append(res.pop(0))

    def _par_flush_caches_out(self, processors, rt_env, downstream):
        if rt_env["amount_out"] == 0:
            return

        resume = True
        while resume:
            res = [] 
            for p,cache in zip(processors, rt_env["caches_out"]):  
                if p.type_out() == ():
                    continue
                if len(cache) == 0:
                    resume = False
                    break
                res.append(cache.pop(0))
            if resume:
                if rt_env["amount_out"] == 1:
                    downstream(res[0])
                else:
                    downstream(tuple(res)) 

        # Reinsert the values we could not send downstream to cache
        for r, cache in zip(res, rt_env["caches_out"]):
            cache.insert(0, r)

    def _par_rectify_res(self, processors, rt_env):
        res = rt_env["results"]
        assert len(res) == rt_env["amount_res"]

        if rt_env["amount_res"] == 1:
            return res[0]

        return tuple(res)




StreamProcessor.runtime_engine = SimpleRuntimeEngine()

