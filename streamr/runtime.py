# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import Stop, Resume, MayResume, Exhausted
from .types import unit, ALL, ANY

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

    def get_par_rt(self, processors, envs):
        """
        Get the runtime for parallel execution.
        """
        return SimpleParallelRuntime(processors, envs)

    def get_sub_rt(self, processor):
        """
        Get the runtime for parallel execution.
        """
        return SimpleSubprocessRuntime(processor)


class SimpleRuntime(object):
    def __init__(self, processors, envs):
        assert len(processors) == len(envs)
        self.processors = processors
        self.envs = envs
        self._amount_procs = len(processors)
        # Caches to store values in the pipe between processors.
        self._res_map, self._amount_res = _result_mapping(processors) 
        self._results = [_NoRes] * self._amount_res
        # Tracks whether a processor is exhausted
        self._exhausted = [False] * self._amount_procs
        self._cur_amount_res = 0

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


class SimpleSequentialRuntime(SimpleRuntime):
    """
    The runtime to be used by SequentialStreamProcessors.
    """
    def __init__(self, processors, envs):
        super(SimpleSequentialRuntime, self).__init__(processors, envs)

        # Caches to store values in the pipe between processors.
        self._caches = [list() for _ in range(0, self._amount_procs - 1)]

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


class SimpleParallelRuntime(SimpleRuntime):
    """
    The runtime to be used by ParallelStreamProcessors.
    """
    def __init__(self, processors, envs):
        super(SimpleParallelRuntime, self).__init__(processors, envs)
        self._in_map, self._amount_in = _in_mapping(processors)
        self._out_map, self._amount_out = _out_mapping(processors)
        self._caches_in = [list() for _ in range(0, self._amount_in)]
        self._caches_out = [list() for _ in range(0, self._amount_out)]

    def step(self, await, send):
        resume = False
        exhausted = False 
        for i, p in enumerate(self.processors):
            try:
                res = p.step( self.envs[i]
                            , self._upstream(await, i)
                            , self._downstream(i)
                            )
            # We make sure that each processor gets the chance
            # to run an equal number of steps.
            except Exhausted:
                exhausted = True
                continue
                 
            if isinstance(res, Resume):
                self._delete_result(i)
                resume = True
            elif isinstance(res, MayResume):
                self._write_result(i, res.result)
            elif isinstance(res, Stop):
                self._write_result(i, res.result)
                self._exhausted[i] = True

        self._send_downstream(send)

        if exhausted:
            raise Exhausted

        exhausted = ANY(self._exhausted)
        if exhausted and resume:
            raise RuntimeError("One processor wants to resume while other "
                               "wants to stop.")
        if resume or not self._has_enough_results():
            return Resume() 
        if exhausted:
            if not self._has_enough_results():
                raise RuntimeError("One processor wants to stop, but there "
                                   "are not enough results.")
            return Stop(*self._normalized_result())
        return MayResume(*self._normalized_result())

    def _downstream(self, i):
        """
        Downstream from the i'th processor.
        """
        assert i >= 0
        assert i < self._amount_procs

        def _send(val):
            _i = self._out_map[i]
            if _i is None:
                raise RuntimeError("Processor %d send value but has no type_out.")
            self._caches_out[_i].append(val)

        return _send

    def _upstream(self, await, i):
        """
        Upstream to the i'th processor.
        """
        assert i >= 0
        assert i < self._amount_procs

        def _await():
            _i = self._in_map[i]
            if _i is None:           
                raise RuntimeError("Processor %d awaits value but has no type_in.")
            while len(self._caches_in[_i]) == 0:
                val = await()
                if self._amount_in == 1:
                    self._caches_in[0].append(val)        
                    continue
                for j, p in enumerate(self.processors):
                    _j = self._in_map[j]
                    if _j is None:
                        continue
                    self._caches_in[_j].append(val[_j])        

            return self._caches_in[_i].pop(0)

        return _await

    def _send_downstream(self, send):
        if self._amount_out == 0:
            return

        can_send = not ANY(map(lambda x: x == [], self._caches_out))
        while can_send:
            data = []
            for i in range(0, self._amount_procs):
                _i = self._out_map[i]
                if _i is None:
                    continue
                data.append(self._caches_out[_i].pop(0))
                if len(self._caches_out[_i]) == 0:
                    can_send = False
            if self._amount_out == 1:
                send(data[0])
            else:
                send(tuple(data))


class SimpleSubprocessRuntime(object):
    def __init__(self, process):
        self.process = process

    def step(self, await, send):
        init = await()
        if not isinstance(init, tuple):
            init = (init, )
        result = self.process.run(*init)
        send(result)
        return MayResume()

def _result_mapping(processors):
    return _mapping(processors, lambda p: p.type_result())

def _in_mapping(processors):
    return _mapping(processors, lambda p: p.type_in())

def _out_mapping(processors):
    return _mapping(processors, lambda p: p.type_out())

def _mapping(processors, type_lambda):
    """
    Creates a dict with i -> j entries, where for every processor where 
    type_lambda is not unit a new j is introduced and for every other 
    processor j is None. Also returns the amount of processors where 
    type_lambda is not None. 
    """
    j = 0
    res = {}
    for i, p in enumerate(processors):
        if type_lambda(p) == unit:
            res[i] = None
        else:
            res[i] = j
            j += 1
    return res, j


