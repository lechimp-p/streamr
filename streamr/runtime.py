# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import Stop, Resume, MayResume, Exhausted, _NoRes, Stream
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

        class _Stream(NoUpDownStream):
            def result(self, val = _NoRes):
                res[0] = val
       
        res = [_NoRes] 
        env = process.get_initial_env(params)
        stream = _Stream()

        try:
            while True:
                state = process.step(env, stream)
                assert state in (Stop, MayResume, Resume)
                if state == Stop:
                    break

        except Exhausted:
            pass
        finally:
            process.shutdown_env(env)

        if process.type_result() != ():
            if res[0] == _NoRes:
                raise RuntimeError("Process did not return result.")
            return res[0]
        else:
            return ()

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


class NoUpDownStream(Stream):
    def await(self):
        raise RuntimeError("Process should not send a value downstream, "
                           "but send %s" % val)
    def send(self, val):
        raise RuntimeError("Process should not send a value downstream, "
                           "but send %s" % val)


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
            raise RuntimeError("Process %d tried to write result but "
                               "has no result type." % i)    
        if self._results[i] == _NoRes:
            self._cur_amount_res += 1
        if result == _NoRes:
            self._cur_amount_res -= 1
        self._results[i] = result

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

    def step(self, stream):     
        # This is a pull based implementation, so we do a step
        # on the last processor in the pipe.
        last_proc_index = self._amount_procs - 1 
        state = self.processors[last_proc_index].step( 
                       self.envs[last_proc_index]
                     , SequentialStream(
                            self, last_proc_index, stream)
                     )       
        assert state in (Stop, MayResume, Resume)

        # This for sure means we need to resume.
        if state == Resume:
            #self._delete_result(last_proc_index)
            return Resume

        # We now know for sure, that there is a result, since
        # Stop or MayResume were send.
        #self._write_result(last_proc_index, res.result)

        if state == MayResume:
            if not self._has_enough_results():
                return Resume
            if self._amount_res > 0:
                stream.result(*self._normalized_result())
            return MayResume
        else: # state == Stop
            if self._amount_res > 0:
                if self._has_enough_results(): 
                    stream.result(*self._normalized_result())
                else:
                    raise RuntimeError("Last stream processor signals stop,"
                                       " but there are not enough results.")
            return Stop

class SequentialStream(Stream):
    def __init__(self, runtime, index, stream):
        self.runtime = runtime
        self.index = index
        self.stream = stream

    def await(self):
        """
        Upstream of the index'th processor.
        """
        assert self.index >= 0
        assert self.index < self.runtime._amount_procs

        # This is the upstream to the frontmost processors
        # so we need to use the upstrean of the while process.
        if self.index == 0:
            return self.stream.await()

        # This is the upstream for a processor inside the stream.
        left_index = self.index - 1

        # It is fed by the processor to the left.
        while len(self.runtime._caches[left_index]) == 0:
            # If the processor is exhausted, this processor is
            # exhausted to.
            if self.runtime._exhausted[left_index]:
                raise Exhausted
            
            # So we need to invoke the processor to the left
            # to get a new value.
            state = self.runtime.processors[left_index].step(
                              self.runtime.envs[left_index]
                            , SequentialStream(
                                    self.runtime, left_index, self.stream)
                            )
            assert state in (Stop, MayResume, Resume)

            if state == Stop:
                self.runtime._exhausted[left_index] = True

        # When there is a result, we give it to the calling processor.
        return self.runtime._caches[left_index].pop(0)

    def send(self, val):
        """
        Downstream from index'th processor.
        """
        assert self.index >= 0
        assert self.index < self.runtime._amount_procs

        # This is the the downstream from the last
        # processor and therefore identically with
        # the downstream of the complete 
        if self.index == self.runtime._amount_procs - 1:
            return self.stream.send(val)

        # This is the downstream from some processor
        # inside the stream, so we cache the value for
        # later usage.
        self.runtime._caches[self.index].append(val)

    def result(self, val = _NoRes):
        self.runtime._write_result(self.index, val)


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

    def step(self, stream):
        resume = False
        exhausted = False 
        for i, p in enumerate(self.processors):
            try:
                state = p.step( self.envs[i]
                              , ParallelStream(self, i, stream)
                              )
                assert state in (Stop, Resume, MayResume)
            # We make sure that each processor gets the chance
            # to run an equal number of steps.
            except Exhausted:
                exhausted = True
                continue
                 
            if state == Resume:
                resume = True
            elif state == Stop:
                self._exhausted[i] = True

        self._send_downstream(stream)

        if exhausted:
            raise Exhausted

        exhausted = ANY(self._exhausted)
        if exhausted and resume:
            raise RuntimeError("One processor wants to resume while other "
                               "wants to stop.")
        if resume or not self._has_enough_results():
            return Resume 
        if exhausted:
            if self._amount_res > 0:
                if not self._has_enough_results():
                    raise RuntimeError("One processor wants to stop, but there "
                                       "are not enough results.")
                stream.result(*self._normalized_result())
            return Stop
        if self._amount_res > 0:
            stream.result(*self._normalized_result())
        return MayResume

    def _send_downstream(self, stream):
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
                stream.send(data[0])
            else:
                stream.send(tuple(data))


class ParallelStream(Stream):
    def __init__(self, runtime, index, stream):
        self.runtime = runtime
        self.index = index
        self.stream = stream

    def await(self):
        """
        Upstream of the index'th processor.
        """
        assert self.index >= 0
        assert self.index < self.runtime._amount_procs

        _i = self.runtime._in_map[self.index]
        if _i is None:           
            raise RuntimeError("Processor %d awaits value but has no type_in.")
        while len(self.runtime._caches_in[_i]) == 0:
            val = self.stream.await()
            if self.runtime._amount_in == 1:
                self.runtime._caches_in[0].append(val)        
                continue
            for j, p in enumerate(self.runtime.processors):
                _j = self.runtime._in_map[j]
                if _j is None:
                    continue
                self.runtime._caches_in[_j].append(val[_j])        

        return self.runtime._caches_in[_i].pop(0)

    def send(self, val):
        """
        Downstream from the index'th processor.
        """
        assert self.index >= 0
        assert self.index < self.runtime._amount_procs

        _i = self.runtime._out_map[self.index]
        if _i is None:
            raise RuntimeError("Processor %d send value but has no type_out.")
        self.runtime._caches_out[_i].append(val)

    def result(self, val = _NoRes):
        self.runtime._write_result(self.index, val)


class SimpleSubprocessRuntime(object):
    def __init__(self, process):
        self.process = process

    def step(self, stream):
        init = stream.await()
        if not isinstance(init, tuple):
            init = (init, )
        result = self.process.run(*init)
        stream.send(result)
        return MayResume

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


