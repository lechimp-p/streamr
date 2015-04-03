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
        if len(params) == 1:
            assert process.type_init().contains(params[0])
        else:
            assert process.type_init().contains(params)


        def await(val):
            raise RuntimeError("Process should not send a value downstream, "
                               "but send %s" % val)
        def send(val):
            raise RuntimeError("Process should not send a value downstream, "
                               "but send %s" % val)
        
        res = None
        env = process.get_initial_env(params)

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

    def get_seq_rt(self, processors, envs):
        """
        Get runtime for sequential execution.
        """
        return SimpleSequentialRuntime(processors, envs)

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

        res = self._par_rectify_res(rt.rt_env)

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

    def _par_rectify_res(self, rt_env):
        res = rt_env["results"]
        assert len(res) == rt_env["amount_res"]

        if rt_env["amount_res"] == 1:
            return res[0]

        return tuple(res)

    ###########################################################################
    #
    # Methods for subprocesses.
    #
    ###########################################################################

    def get_initial_env_for_sub(self, process):
        return ()

    def step_sub(self, rt):
        init = rt.await()
        if not isinstance(init, tuple):
            init = (init, )
        result = rt.processors.run(*init)
        rt.send(result)
        return MayResume()    



class _NoRes(object):
    pass


def _result_mapping(processors):
    """
    Creates a dict with i -> j entries, where for each processor
    that returns a result a new j is introduced and for each
    processor that produces no result j == None. Also returns
    the amount of processors that return a result.
    """
    j = 0
    res = {}
    for i, p in enumerate(processors):
        if p.type_result() == unit:
            res[i] = None
        else:
            res[i] = j
            j += 1
    return res, j


class SimpleSequentialRuntime(object):
    """
    The runtime to be used by SequentialStreamProcessors.
    """
    def __init__(self, processors, envs):
        assert len(processors) == len(envs)
        self.processors = processors
        self.envs = envs
        self._amount_procs = len(processors)
        # Caches to store values in the pipe between processors.
        self._caches = [[]] * (self._amount_procs - 1)
        self._res_map, self._amount_res = _result_mapping(processors) 
        self._results = [_NoRes] * self._amount_res
        # Tracks whether a processor is exhausted
        self._exhausted = [False] * (self._amount_procs - 1)
        self._cur_amount_res = 0

    def step(self, await, send):     
        # This is a pull based implementation, so we do a step
        # on the last processor in the pipe.
        last_proc_index = self._amount_procs - 1 
        res = self.processors[last_proc_index].step( 
                       self.envs[last_proc_index]
                     , self._upstream(last_proc_index, send, await)
                     , self._downstream(last_proc_index, send)
                     )       

        # This for sure means we need to resume.
        if isinstance(res, Resume):
            # TODO: I think it would be correct to set the result
            # of the processor to no result here.
            self._delete_result(last_proc_index)
            return Resume()

        # We now know for sure, that there is a result, since
        # Stop or MayResume were send.
        self._write_result(last_proc_index, res.result)

        if isinstance(res, MayResume):
            if self._has_enough_results():
                return MayResume(*self._normalized_result())
            return Resume()
        else: # isinstance(res, Stop) == True
            if self._has_enough_results(): 
                return Stop(*self._normalized_result())
            raise RuntimeError("Last stream processor signals stop,"
                               " but there are not enough results.")

    def _downstream(self, i, send):
        """
        Downstream from i'th processor.
        """
        assert i >= 0
        assert i < self._amount_procs

        # This is the the downstream from the last
        # processor and therefore identically with
        # the downstream of the complete 
        if i == self._amount_procs - 1:
            return send

        # This is the downstream from some processor
        # inside the stream, so we cache the value for
        # later usage.
        def _send(val):
            self._caches[i].append(val)

        return _send 

    def _upstream(self, i, send, await):
        """
        Upstream to i'th processor.
        """
        assert i >= 0
        assert i < self._amount_procs

        # This is the upstream to the frontmost processors
        # so we need to use the upstrean of the while process.
        if i == 0:
            return await

        # This is the upstream for a processor inside the stream.
        left_index = i - 1
        def _await():
            # It is fed by the processor to the left.
            while len(self._caches[left_index]) == 0:
                # If the processor is exhausted, this processor is
                # exhausted to.
                if self._exhausted[left_index]:
                    raise Exhausted()
                
                # So we need to invoke the processor to the left
                # to get a new value.
                res = self.processors[left_index].step(
                                  self.envs[left_index]
                                , self._upstream(left_index, send, await)
                                , self._downstream(left_index, send)
                                )

                # We also need to deal with it's result.
                if isinstance(res, Resume):
                    self._delete_result(left_index)
                elif isinstance(res, Stop):
                    self._exhausted[left_index] = True
                    self._write_result(left_index, res.result)
                elif isinstance(res, MayResume):
                    self._write_result(left_index, res.result)
            # When there is a result, we give it to the calling processor.
            return self._caches[left_index].pop(0)
        return _await

    def _write_result(self, index, result):
        i = self._res_map[index]
        if i is None:
            return
        if self._results[i] == _NoRes:
            self._cur_amount_res += 1
        self._results[i] = result

    def _delete_result(self, index):
        i = self._res_map[index]
        if i is None:
            return
        if self._results[i] != _NoRes:
            self._cur_amount_res -= 1
        self._results[i] = _NoRes

    def _normalized_result(self):
        assert self._has_enough_results()
        if self._amount_res == 0:
            return () 
        if self._amount_res == 1:
            return (self._results[0], )
        return (tuple(self._results), )

    def _has_enough_results(self):
        return self._cur_amount_res == self._amount_res
