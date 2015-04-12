# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr.core import ( StreamProcessor, Stop, Resume, MayResume, Exhausted
                         , subprocess, Stream, _NoRes )
from streamr.types import Type, unit, ArrowType


###############################################################################
#
# Tests for implementations in base.
#
###############################################################################

@pytest.fixture
def pr():
    return MockProducer(int, 10)

@pytest.fixture
def pr_str():
    return MockProducer(str, "Hello World!")

@pytest.fixture
def co():
    return MockConsumer(int, 10)

@pytest.fixture
def co_str():
    return MockConsumer(str, 10)

@pytest.fixture
def co_any():
    return MockConsumer(Type.get(), 10)

@pytest.fixture
def pi():
    return MockPipe(int, int, lambda x: 2 * x)

@pytest.fixture
def pi_int_str():
    return MockPipe(int, str, lambda x: "%s" % x)

@pytest.fixture
def pi_str_int():
    return MockPipe(str, int, len)

@pytest.fixture
def pi_any():
    tvar = Type.get()
    return MockPipe(tvar, tvar)

@pytest.fixture
def pi_any2():
    tvar = Type.get()
    return MockPipe(tvar, tvar)

@pytest.fixture
def all_sps(pr, co, pi):
    return [pr, co, pi]

class TestCompositionBase(object):
    """
    Test whether composition of different stream parts lead to the
    expected results.

    Objects from the classes need to respect the follwing rules, 
    where abbreaviations for the names are used.
    """
    # any >> Pr = error
    def test_AnyCompPr(self, all_sps, pr):
        for sp in all_sps:
            with pytest.raises(TypeError) as excinfo:
                sp >> pr
            assert "compose" in str(excinfo.value)

    # Co >> any = error
    def test_CoCompAny(self, all_sps, co):
        for sp in all_sps:
            with pytest.raises(TypeError) as excinfo:
                co >> sp
            assert "compose" in str(excinfo.value)

    # Pi >> Pi = Pi
    def test_PiCompPi(self, pi):
        assert (pi >> pi).is_pipe()

    # Pr >> Pi = Pr
    def test_PrCompPi(self, pr, pi):
        assert (pr >> pi).is_producer()

    # Pi >> Co = Co
    def test_PiCompCo(self, pi, co):
        assert (pi >> co).is_consumer()

    # Pr >> Co = SP
    def test_PrCompCo(self, pr, co):
        assert (pr >> co).is_runnable()

    # SP >> any = error
    def test_SPCompAny(self, all_sps, pr, co):
        spt = pr >> co
        for sp in all_sps:
            with pytest.raises(TypeError)as excinfo:
                spt >> sp
            assert "compose" in str(excinfo.value)

    # any >> SP = error
    def test_SPCompAny(self, all_sps, pr, co):
        spt = pr >> co
        for sp in all_sps:
            with pytest.raises(TypeError) as excinfo:
                sp >> spt
            assert "compose" in str(excinfo.value)

    def test_tupleInitType(self):
        val = [None]
        class TupleInitStreamProcessor(StreamProcessor):
            def __init__(self):
                super(TupleInitStreamProcessor, self).__init__((int, int), (), (), ())
            def setup(self, params, result):
                val[0] = params
            def step(self, env, stream):
                return Stop

        sp = TupleInitStreamProcessor()
        sp.run(1,2)
        assert val[0] == (1,2)

    def test_pipeWithInitAndResult(self):
        from streamr.simple import from_list, to_list

        class TimesX(StreamProcessor):
            def __init__(self):
                super(TimesX, self).__init__(int, int, int, int)
            def setup(self, params, result):
                result(params[0])
                return params[0]
            def step(self, env, stream):
                stream.send(env * stream.await())
                return MayResume

        sp = from_list([1]) >> TimesX() >> to_list()
        assert sp.type_in() == () 
        assert sp.type_out() == () 
        assert sp.type_init() == int 
        assert sp.type_result() == (int, [int]) 

        assert sp.run(1) == (1, [1])
        assert sp.run(2) == (2, [2])

    def test_initBug(self):
        from streamr import const, to_list

        c = const(value_type = int, amount = 10)
        l = to_list()
        
        sp = c >> l
        assert sp.run(1) == [1] * 10

        sp = c * c >> l * l 
        assert sp.run(1,2) == ([1] * 10, [2] * 10)

    def test_resultsWithCombinators(self):
        class ResultsIn(StreamProcessor):
            def __init__(self, type_result, val, type_in = (), type_out = ()):
                self.val = val
                super(ResultsIn, self).__init__((), type_result, type_in, type_out)
            def setup(self, params, result):
                if self.type_result() != ():
                    result(self.val)
                return [0]
            def step(self, env, stream):
                if env[0] < 2:
                    env[0] += 1
                    if self.type_out() != ():
                        stream.send(object())
                    if self.type_in() != ():
                        stream.await()
                    return MayResume
                else:
                    return Stop

        r1 = ResultsIn(int, 10)
        r2 = ResultsIn(str, "hello")
        assert r1.run() == 10
        assert r2.run() == "hello" 
        assert (r1 * r2).run() == (10, "hello")

        r3 = ResultsIn(float, 13.0, (), object)
        r4 = ResultsIn(dict, {}, object, ())
        assert (r3 >> r4).run() == (13.0, {})
        assert (r1 * (r3 >> r4)).run() == (10, (13.0, {}))
        assert ((r3 >> r4) * r1).run() == ((13.0, {}), 10)
        assert (r1 * r2 * (r3 >> r4)).run() == (10, "hello", (13.0, {}))
        assert (r1 * r3 >> r4).run() == ((10, 13.0), {})
        assert (r3 >> r2 * r4).run() == (13.0, ("hello", {}))
        assert (r1 * r3 >> r2 * r4).run() == ((10, 13.0), ("hello",{}))

        r5 = ResultsIn((), (), (), object)
        r6 = ResultsIn(int, 1, object, object)
        r7 = ResultsIn(float, 2.0, object, ())
        assert (r5 >> r6 >> r7).run() == (1,2.0)

        r8 = ResultsIn((), (), object, object)
        r9 = ResultsIn((), (), (), object)
        r10 = ResultsIn(str, "hello", object, ())
        r11 = ResultsIn(str, "world", object, ())
        assert ((r9 * r9) >> (r10 * r8) >> r11).run() == ("hello", "world")


class TestCompositionTyped(object):
    def test_PrCompCoAny(self, pr, pr_str, co_any):
        sp1 = pr >> co_any
        assert sp1.run() == [10]*10

        sp2 = pr_str >> co_any
        assert sp2.run() == ["Hello World!"]*10

    def test_PrStrCompCoInt(self, pr_str, co):
        with pytest.raises(TypeError) as excinfo:
            pr_str >> co
        assert "compose" in str(excinfo.value)

    def test_PrStrCompPiAny(self, pr_str, pi_any):
        pr = pr_str >> pi_any
        assert pr.type_out() == Type.get(str)

    def test_PiAnyCompPiAny(self, pi_any, pi_any2):
        assert pi_any.type_out() != pi_any2.type_in()
        pi = pi_any >> pi_any2
        assert pi.type_out() == pi.type_in()

    def test_noNestedParallelComposition(self, pr, co):
        sp = ((pr * pr) * pr) >> (co * (co * co))
        assert sp.run()
        

class TestStreamProcessResults(object):
    def test_PrCompCo(self, pr, co):
        sp = pr >> co
        assert sp.run() == [10]*10
    def test_PrCompPiCompCo(self, pr, pi, co):
        sp = pr >> pi >> co
        assert sp.run() == [20]*10
    def test_PrCompPiCompPiCompCo(self, pr, pi, co):
        sp = pr >> pi >> pi >> co
        assert sp.run() == [40]*10


class TestStacking(object):
    def test_stackProducers(self, pr, pr_str):
        p = pr * pr_str

        assert p.is_producer()
        assert p.type_out() == pr.type_out() * pr_str.type_out() 

    def test_stackPipes(self, pi, pi_any):
        p = pi * pi_any

        assert p.is_pipe()
        assert p.type_in() == pi.type_in() * pi_any.type_in()
        assert p.type_out() == pi.type_out() * pi_any.type_out()

    def test_stackConsumers(self, co, co_str):
        c = co * co_str

        assert c.is_consumer()
        assert c.type_in() == co.type_in() * co_str.type_in()

    def test_stackPipeConsumer(self, pi, co):
        p = pi * co

        assert not p.is_producer()
        assert not p.is_consumer()
        assert not p.is_runnable()
        assert p.is_pipe()
        assert p.type_in() == pi.type_in() * co.type_in()
        assert p.type_out() == pi.type_out()

    def test_stackProducerPipe(self, pr, pi):
        p = pr * pi

        assert not p.is_producer()
        assert not p.is_consumer()
        assert not p.is_runnable()
        assert p.is_pipe()
        assert p.type_in() == pi.type_in()
        assert p.type_out() == pr.type_out() * pi.type_in()

    def test_stackStackedProducerAndMixedStack1(self, pr, pi, co):
        p = (pr * pr) >> (pi * co)

        assert p.is_producer()
        assert not p.is_consumer()
        assert not p.is_runnable()
        assert not p.is_pipe()
        assert p.type_in() is unit 
        assert p.type_out() == pi.type_out()

    def test_stackStackedProducerAndMixedStack2(self, pr, pi, co):
        p = (pr * pr * pr) >> (pi * pi * co)

        assert p.is_producer()
        assert not p.is_consumer()
        assert not p.is_runnable()
        assert not p.is_pipe()
        assert p.type_in() is unit 
        assert p.type_out() == pi.type_out() * pi.type_out()

    def test_stackStackedProducerAndMixedStack3(self, pr, pi, co):
        p = (pr * pr * pr) >> (pi * pi * co) >> (pi * co)

        assert p.is_producer()
        assert not p.is_consumer()
        assert not p.is_runnable()
        assert not p.is_pipe()
        assert p.type_in() is unit 
        assert p.type_out() == pi.type_out()

    def test_result1(self, pr, pi, co):
        sp = (pr * pr) >> (pi * pi) >> (co * co)
        assert sp.run() == ([20]*10, [20]*10)

    def test_result2(self, pr, pi, co):
        sp = (pr * pr) >> (pi * co) >> co
        assert sp.run() == ([10]*10, [20]*10)

    def test_result2a(self, pr, co):
        sp = (pr * (pr >> co)) >> co
        assert sp.run() == ([10]*10, [10]*10)

    def test_result2b(self, pr, co):
        sp = (pr >> co) * (pr >> co)
        assert sp.run() == ([10]*10, [10]*10)

    def test_result3(self, pr, pi, co):
        sp = pr >> (pi * pr) >> (co * co)
        assert sp.run() == ([20]*10, [10]*10)

    def test_result4(self, pr, pi, co):
        sp = (pr * pr) >> (pi * pi) >> (pi * pi) >> (co * co)
        assert sp.run() == ([40]*10, [40]*10)

    def test_result5(self, pr, pi, co):
        sp = (pr * pr * pr) >> (pi * pi * co) >> (pi * co) >> co
        assert sp.run() == ([10]*10, [20]*10, [40]*10)

    def test_result6(self, pr, pi, co):
        sp = pr >> (pi * pr) >> (pi * pi * pr) >> (co * co * co)
        assert sp.run() == ([40]*10, [20]*10, [10]*10)

    def test_result7(self, pr, pi, co):
        sp = (pr * pr * pr) >> (co * pi * pi) >> (co * pi) >> co
        assert sp.run() == ([10]*10, [20]*10, [40]*10)

    def test_result8(self, pr, pi, co):
        sp = pr >> (pr * pi) >> (pr * pi * pi) >> (co * co * co)
        assert sp.run() == ([10]*10, [20]*10, [40]*10)

    def test_result9(self, pr, pi, co):
        sp = pr >> (pr * pi * pr) >> (pi * co * pi) >> (co * co)

        assert sp.run() == ([20]*10, ([20]*10, [20]*10))


###############################################################################
#
# Base classes for tests on stream parts.
#
###############################################################################

class _TestStreamProcessor(object):
    @pytest.fixture
    def env_params(self):
        return ()

    @pytest.fixture
    def max_amount(self):
        return 100

    class _NoValue:
        pass

    @pytest.fixture
    def result(self):
        return self._NoValue

    def test_typeOut(self, processor):
        assert isinstance(processor.type_out(), Type)

    def test_typeIn(self, processor):
        assert isinstance(processor.type_in(), Type)

    def test_typeInit(self, processor):
        assert isinstance(processor.type_init(), Type)

    def test_typeResult(self, processor):
        assert isinstance(processor.type_result(), Type)

    def test_typeArrow(self, processor):
        assert processor.type_arrow() == ArrowType.get( processor.type_in()
                                                      , processor.type_out())

class _TestProducer(_TestStreamProcessor):
    @pytest.fixture
    def processor(self, producer):
        return producer

    def test_isProducer(self, producer):
        assert producer.is_producer()

    def test_typeOutIsNotVariable(self, producer):
        assert not producer.type_out().is_variable()

    def test_producedValues(self, producer, env_params, max_amount, result):
        count = 0
        tout = producer.type_out()
        _NoValue = self._NoValue

        class _Stream(Stream):
            def await(self):
                assert False

            def send(self, v):
                if result != _NoValue:
                    assert result.pop(0) == v
                if not tout.is_variable():
                    assert tout.contains(v)

            def result(self, res = _NoRes):
                assert producer.type_result() != () 

        stream = _Stream()
        env = producer.setup(env_params, stream.result)

        try:
            for i in range(0, max_amount):
                state = producer.step(env, stream)
                if state == Stop:
                    break
        except Exhausted:
            pass
        finally:
            producer.teardown(env)

class _TestConsumer(_TestStreamProcessor):
    @pytest.fixture
    def processor(self, consumer):
        return consumer 

    def test_isConsumer(self, consumer):
        assert consumer.is_consumer()

    def test_consumedValues(self, consumer, env_params, max_amount, test_values, result):
        t = consumer.type_in()

        class _Stream(Stream):
            def await(self):
                if len(test_values) == 0:
                    raise Exhausted()

                return test_values.pop(0)

            def send(self, val):
                assert False

            def result(self, val = _NoRes):
                res[0] = val

        res = [_NoRes]
        state = None
        stream = _Stream()
        env = consumer.setup(env_params, stream.result)
       
        try:
            count = 0
            while True and count < max_amount:
                count += 1
                state = consumer.step(env, stream)
                if state == Stop:
                    break 
        except Exhausted:
            pass
        finally:
            consumer.teardown(env)

        if result != self._NoValue:
            assert result == res[0] 
            if not consumer.type_result().is_variable():
                assert consumer.type_result().contains(res[0])

class _TestPipe(_TestStreamProcessor):
    @pytest.fixture
    def processor(self, pipe):
        return pipe 

    def test_isPipe(self, pipe):
        assert pipe.is_pipe()

    def test_typeIn(self, pipe):
        assert isinstance(pipe.type_in(), Type)

    def test_typeOut(self, pipe):
        assert isinstance(pipe.type_out(), Type)

    def test_transformedValues(self, pipe, env_params, max_amount, test_values, result):
        tin = pipe.type_in()
        tout = pipe.type_out()
        send_was_called = [False]
        _NoValue = self._NoValue

        class _Stream(Stream):
            def await(self):
                if len(test_values) == 0:
                    raise Exhausted
                v = test_values.pop(0) 
                if not tin.is_variable():
                    assert tin.contains(v)
                # TODO: There is no test weather the pipe is correct when
                # tin is a TypeVar.
                return v

            def send(self, v):
                send_was_called[0] = True
                if result != _NoValue:
                    assert result.pop(0) == v
                if not tout.is_variable():
                    assert tout.contains(v)
                # TODO: There is no test weather the pipe is correct when
                # tout is a TypeVar.

            def result(self, val = _NoRes):
                # A pipe should not have a result.
                assert False

        stream = _Stream()
        env = pipe.setup(env_params, stream.result)

        try:
            for i in range(0, min(max_amount, len(test_values))):
                state = pipe.step(env, stream)
                assert state in (Stop, MayResume, Resume)
                if state == Stop:
                    break 
        except Exhausted:
            pass
        finally:
            pipe.teardown(env)

        assert send_was_called[0]


###############################################################################
#
# Test of products of composition.
#
###############################################################################

class TestAppendPipe(_TestProducer):
    @pytest.fixture
    def producer(self, pr, pi):
        return pr >> pi

    @pytest.fixture
    def max_amount(self):
        return 10   

    @pytest.fixture
    def env_params(self):
        return ()

def TestPrependPipe(_TestConsumer):
    @pytest.fixture
    def consumer(self, pi, co):
        return pi >> co

    @pytest.fixture
    def test_values(self):
        return list(range(0, 10))

    @pytest.fixture
    def env_params(self):
        return()

class TestFusePipe(_TestPipe):
    @pytest.fixture
    def pipe(self, pi):
        return pi >> pi

    @pytest.fixture
    def test_values(self):
        return list(range(0, 10))

    @pytest.fixture
    def max_amount(self):
        return 10   

    @pytest.fixture
    def env_params(self):
        return()

###############################################################################
#
# Test of products of stacking.
#
###############################################################################

class TestStackPipe(_TestPipe):
    @pytest.fixture
    def pipe(self, pi):
        return pi * pi

    @pytest.fixture
    def test_values(self):
        return [(i,i) for i in range(0, 10)]

    @pytest.fixture
    def max_amount(self):
        return 10   

    @pytest.fixture
    def env_params(self):
        return()

class TestStackProducer(_TestProducer):
    @pytest.fixture
    def producer(self, pr):
        return pr * pr

    @pytest.fixture
    def max_amount(self):
        return 10   

    @pytest.fixture
    def env_params(self):
        return()

class TestStackConsumer(_TestConsumer):
    @pytest.fixture
    def consumer(self, co):
        return co * co

    @pytest.fixture
    def test_values(self):
        return [(i,i) for i in range(0, 10)]

    @pytest.fixture
    def env_params(self):
        return()

###############################################################################
#
# Mocks for stream parts including tests.
#
###############################################################################

class MockProducer(StreamProcessor):
    def __init__(self, ttype, value):
        super(MockProducer, self).__init__((), (), (), ttype)
        self.value = value

    def setup(self, _, __):
        return 0

    def step(self, env, stream):
        if env >= 100:
            raise RuntimeError("I did not expect this to run that long...")
        env += 1
        stream.send(self.value)
        return MayResume

class TestMockProducer(_TestProducer):
    @pytest.fixture
    def producer(self):
        return MockProducer(int, 10)

    @pytest.fixture
    def max_amount(self):
        return 10

    @pytest.fixture
    def env_params(self):
        return()

class MockConsumer(StreamProcessor):
    def __init__(self, ttype, max_amount = None):
        super(MockConsumer, self).__init__((), [ttype], ttype, ())
        self.max_amount = max_amount
    def setup(self, _, __):
        return [] 
    def teardown(self, env):
        pass

    def step(self, env, stream):
        env.append(stream.await())
        stream.result(env)

        if self.max_amount is not None and len(env) >= self.max_amount:
            return Stop

        return MayResume

class TestMockConsumer(_TestConsumer):
    @pytest.fixture
    def consumer(self):
        return MockConsumer(int)

    @pytest.fixture
    def test_values(self):
        return list(range(0, 10))

    @pytest.fixture
    def env_params(self):
        return()

class MockPipe(StreamProcessor):
    def __init__(self, type_in, type_out, transform = None):
        super(MockPipe, self).__init__((), (), type_in, type_out)
        self.trafo = (lambda x : x) if transform is None else transform

    def step(self, env, stream):
        stream.send(self.trafo(stream.await()))
        return MayResume

class TestMockPipe(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return MockPipe(int, int)

    @pytest.fixture
    def test_values(self):
        return list(range(0, 10))

    @pytest.fixture
    def max_amount(self):
        return 10

    @pytest.fixture
    def env_params(self):
        return()


class TestSubprocess(_TestPipe):
    def test_types(self, pr, pi, co):
        with pytest.raises(TypeError) as excinfo:
            subprocess(pr)
        assert "subprocess" in str(excinfo.value)

        with pytest.raises(TypeError) as excinfo:
            subprocess(pi)
        assert "subprocess" in str(excinfo.value)

        with pytest.raises(TypeError) as excinfo:
            subprocess(co)
        assert "subprocess" in str(excinfo.value)

        sp = pr >> pi

        assert sp.type_init() == ()
        assert sp.type_result() == ()

        with pytest.raises(TypeError) as excinfo:
            subprocess(sp)
        assert "subprocess" in str(excinfo.value)

        sp = StreamProcessor(int, int, (), ())
        sub = subprocess(sp)
        
        assert sp.type_init() == int 
        assert sp.type_result() == int 
        assert sp.type_in() == () 
        assert sp.type_out() ==() 
        assert sub.type_init() == ()
        assert sub.type_result() == ()
        assert sub.type_in() == int
        assert sub.type_out() == int

    @pytest.fixture
    def pipe(self):
        self.amount_of_calls_to_get_env = 0
        test = self

        class TestProcess(StreamProcessor):
            def __init__(self):
                super(TestProcess, self).__init__(int, [int], (), ())
            def setup(self, params, result):
                result([params[0]] * 10) 
                test.amount_of_calls_to_get_env += 1
                return None 
            def step(self, env, stream):
                return Stop

        return subprocess(TestProcess())

    @pytest.fixture
    def test_values(self):
        return [1, 2, 3]

    @pytest.fixture
    def max_amount(self):
        return 10

    @pytest.fixture
    def env_params(self):
        return ()

    @pytest.fixture
    def result(self, test_values):
        return [ [i] * 10 for i in test_values ]    

    def test_twoInitsAndResults(self):
        from streamr import const, to_list, from_list, subprocess

        c = const(value_type = int, amount = 10)
        l = to_list()
        proc = c * c >> l * l 

        sp = from_list([(1,2)]) >> subprocess(proc) >> to_list()
        assert sp.type_result() == [([int],[int])]
        assert sp.run() == [([1] * 10, [2] * 10)]
