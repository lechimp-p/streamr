# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .core import Stop, Resume, MayResume, Exhausted, _NoRes, Stream, StreamProcessor
from .types import unit, ANY, ProductType

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

        class _Stream(Stream):
            def await(self):
                raise RuntimeError("Process should not await values from upstream.")
            def send(self, val):
                raise RuntimeError("Process should not send a value downstream, "
                                   "but send %s" % val)

            def result(self, val = _NoRes):
                res[0] = val
       
        res = [_NoRes] 
        stream = _Stream()
        env = process.setup(params, stream.result)

        try:
            while True:
                state = process.step(env, stream)
                assert state in (Stop, MayResume, Resume)
                if state == Stop:
                    break

        except Exhausted:
            pass
        finally:
            process.teardown(env)

        if process.type_result() != ():
            if res[0] == _NoRes:
                raise RuntimeError("Process did not return result.")
            return res[0]
        else:
            return ()

    def setup_seq_rt(self, processors, params, result):
        """
        Get runtime for sequential execution.
        """
        return SimpleSequentialRuntime(processors, params, result)

    def teardown_seq_rt(self, rt):
        """
        Teardown a runtime for sequential execution.
        """
        rt.teardown()

    def setup_par_rt(self, processors, params, result):
        """
        Get the runtime for parallel execution.
        """
        return SimpleParallelRuntime(processors, params, result)

    def teardown_par_rt(self, rt):
        """
        Teardown a runtime for sequential execution.
        """
        rt.teardown()

    def setup_sub_rt(self, processor, params, result):
        """
        Get the runtime for parallel execution.
        """
        return SimpleSubprocessRuntime(processor, params, result)

    def teardown_sub_rt(self, rt):
        """
        Teardown a runtime for sequential execution.
        """
        rt.teardown()


class SimpleRuntime(object):
    def __init__(self, processors, params, result):
        self.res_map, self.amount_res = _result_mapping(processors) 
        self.results = [_NoRes] * self.amount_res
        self.cur_amount_res = 0

        self.processors = processors
        self.envs = self._setup_envs(processors, params, result) 
        self.amount_procs = len(processors)

        # Tracks whether a processor is exhausted
        self.exhausted = [False] * self.amount_procs

    def write_result(self, index, result):
        i = self.res_map[index]
        if i is None:
            raise RuntimeError("Process %d tried to write result but "
                               "has no result type." % index)
        if self.results[i] == _NoRes:
            self.cur_amount_res += 1
        if result == _NoRes:
            self.cur_amount_res -= 1
        self.results[i] = result

    def normalized_result(self):
        assert self.has_enough_results()
        if self.amount_res == 0:
            return ()
        if self.amount_res == 1:
            return (self.results[0], )
        return (tuple(self.results), )

    def has_enough_results(self):
        return self.cur_amount_res == self.amount_res

    def teardown(self):
        for p, e in zip(self.processors, self.envs):
            p.teardown(e)
        

    def _setup_envs(self, processors, params, result):
        # It could be the case, that there are multiple processors
        # where one processors requires a tuple type and the other
        # processors expect unit. The tuple then needs special
        # treatment as it is not wrapped enough to make this logic 
        # work. 
        t = SimpleRuntime._should_treat_params_in_setup_special(processors)
        special_setup_param_treatment = t 

        envs = []

        def __result(j,v):
            self.write_result(j,v)
            if self.has_enough_results():
                result(*self.normalized_result())



        # Counter on the position of the next param to consume
        # for the initalisation of the processors.
        i = 0
        for j,p in enumerate(processors):
            tinit = p.type_init()
            _result = lambda v: __result(j, v)
            if tinit is unit:
                envs.append(p.setup((), _result))
                continue

            if isinstance(tinit, ProductType):
                # See comment in constructor
                if special_setup_param_treatment:
                    assert tinit.contains(params)
                    envs.append(p.setup(params, _result))
                    continue

                assert tinit.contains(params[i])
                envs.append(p.setup(params[i], _result))
            else:
                assert tinit.contains(params[i])
                envs.append(p.setup((params[i],), _result))
            i += 1
        
        return envs

    @staticmethod
    def _should_treat_params_in_setup_special(processors):
        cnt_unit = 0
        cnt_product = 0
        for p in processors:
            assert isinstance(p, StreamProcessor)
            tinit = p.type_init()
            if isinstance(tinit, ProductType):
                cnt_product += 1
            elif tinit == unit:
                cnt_unit += 1
        if cnt_product == 1 and cnt_product + cnt_unit == len(processors):
            return True
        return False


class SimpleSequentialRuntime(SimpleRuntime):
    """
    The runtime to be used by SequentialStreamProcessors.
    """
    def __init__(self, processors, params, result):
        super(SimpleSequentialRuntime, self).__init__(processors, params, result)

        # Caches to store values in the pipe between processors.
        self.caches = [list() for _ in range(0, self.amount_procs - 1)]

    def step(self, stream):     
        # This is a pull based implementation, so we do a step
        # on the last processor in the pipe.
        last_proc_index = self.amount_procs - 1 
        state = self.processors[last_proc_index].step( 
                        self.envs[last_proc_index],
                        SequentialStream(self, last_proc_index, stream))

        assert state in (Stop, MayResume, Resume)

        # This for sure means we need to resume.
        if state == Resume:
            return Resume

        if state == MayResume:
            if not self.has_enough_results():
                return Resume
            return MayResume
        else: # state == Stop
            if self.amount_res > 0 and not self.has_enough_results(): 
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
        assert self.index < self.runtime.amount_procs

        # This is the upstream to the frontmost processors
        # so we need to use the upstrean of the while process.
        if self.index == 0:
            return self.stream.await()

        # This is the upstream for a processor inside the stream.
        left_index = self.index - 1

        # It is fed by the processor to the left.
        while len(self.runtime.caches[left_index]) == 0:
            # If the processor is exhausted, this processor is
            # exhausted to.
            if self.runtime.exhausted[left_index]:
                raise Exhausted
            
            # So we need to invoke the processor to the left
            # to get a new value.
            state = self.runtime.processors[left_index].step(
                            self.runtime.envs[left_index],
                            SequentialStream(self.runtime, left_index, 
                                             self.stream))

            assert state in (Stop, MayResume, Resume)

            if state == Stop:
                self.runtime.exhausted[left_index] = True

        # When there is a result, we give it to the calling processor.
        return self.runtime.caches[left_index].pop(0)

    def send(self, val):
        """
        Downstream from index'th processor.
        """
        assert self.index >= 0
        assert self.index < self.runtime.amount_procs

        # This is the the downstream from the last
        # processor and therefore identically with
        # the downstream of the complete 
        if self.index == self.runtime.amount_procs - 1:
            return self.stream.send(val)

        # This is the downstream from some processor
        # inside the stream, so we cache the value for
        # later usage.
        self.runtime.caches[self.index].append(val)

    def result(self, val = _NoRes):
        self.runtime.write_result(self.index, val)
        if self.runtime.has_enough_results():
            self.stream.result(*self.runtime.normalized_result())


class SimpleParallelRuntime(SimpleRuntime):
    """
    The runtime to be used by ParallelStreamProcessors.
    """
    def __init__(self, processors, params, result):
        super(SimpleParallelRuntime, self).__init__(processors, params, result)
        self.in_map, self.amount_in = _in_mapping(processors)
        self.out_map, self.amount_out = _out_mapping(processors)
        self.caches_in = [list() for _ in range(0, self.amount_in)]
        self.caches_out = [list() for _ in range(0, self.amount_out)]

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
                self.exhausted[i] = True

        # TODO: This could be done as soon as there are enough
        # values like we do with result.
        self._send_downstream(stream)

        if exhausted:
            raise Exhausted

        exhausted = ANY(self.exhausted)
        if exhausted and resume:
            raise RuntimeError("One processor wants to resume while other "
                               "wants to stop.")
        if resume or (self.amount_res > 0 and not self.has_enough_results()):
            return Resume 
        if exhausted:
            if self.amount_res > 0 and not self.has_enough_results():
                raise RuntimeError("One processor wants to stop, but there "
                                   "are not enough results.")
            return Stop
        return MayResume

    def _send_downstream(self, stream):
        if self.amount_out == 0:
            return

        can_send = not ANY(map(lambda x: x == [], self.caches_out))
        while can_send:
            data = []
            for i in range(0, self.amount_procs):
                _i = self.out_map[i]
                if _i is None:
                    continue
                data.append(self.caches_out[_i].pop(0))
                if len(self.caches_out[_i]) == 0:
                    can_send = False
            if self.amount_out == 1:
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
        assert self.index < self.runtime.amount_procs

        _i = self.runtime.in_map[self.index]
        if _i is None:           
            raise RuntimeError("Processor %d awaits value but has no type_in.")
        while len(self.runtime.caches_in[_i]) == 0:
            val = self.stream.await()
            if self.runtime.amount_in == 1:
                self.runtime.caches_in[0].append(val)        
                continue
            for j in range(len(self.runtime.processors)):
                _j = self.runtime.in_map[j]
                if _j is None:
                    continue
                self.runtime.caches_in[_j].append(val[_j])        

        return self.runtime.caches_in[_i].pop(0)

    def send(self, val):
        """
        Downstream from the index'th processor.
        """
        assert self.index >= 0
        assert self.index < self.runtime.amount_procs

        _i = self.runtime.out_map[self.index]
        if _i is None:
            raise RuntimeError("Processor %d send value but has no type_out.")
        self.runtime.caches_out[_i].append(val)

    def result(self, val = _NoRes):
        self.runtime.write_result(self.index, val)
        if self.runtime.has_enough_results():
            self.stream.result(*self.runtime.normalized_result())


class SimpleSubprocessRuntime(object):
    def __init__(self, process, _, __):
        self.process = process

    def teardown(self):
        pass

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


