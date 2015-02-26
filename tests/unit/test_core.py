# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr.core import StreamProcessor, Stop, Resume, MayResume, Exhausted
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

    def test_crashDueNoProductIso(self, pr, co):
        with pytest.raises(TypeError) as excinfo:
            sp = ((pr * pr) * pr) >> (co * (co * co))
        assert "compose" in str(excinfo.value)
        

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
        assert sp.run() == (([10]*10, [20]*10), [40]*10)

    def test_result6(self, pr, pi, co):
        sp = pr >> (pi * pr) >> (pi * pi * pr) >> (co * co * co)
        assert sp.run() == (([40]*10, [20]*10), [10]*10)

    def test_result7(self, pr, pi, co):
        sp = (pr * pr * pr) >> (co * pi * pi) >> (co * pi) >> co
        assert sp.run() == (([10]*10, [20]*10), [40]*10)

    def test_result8(self, pr, pi, co):
        sp = pr >> (pr * pi) >> (pr * pi * pi) >> (co * co * co)
        assert sp.run() == (([10]*10, [20]*10), [40]*10)

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

    def test_typeOfProducedValues(self, producer, env_params, max_amount):
        env = producer.get_initial_env(*env_params)
        t = producer.type_out()
        count = 0

        def downstream(val):
            assert t.contains(val)

        def upstream():
            assert False
            return None

        for i in range(0, max_amount):
            producer.step(env, upstream, downstream)

        producer.shutdown_env(env)

class _TestConsumer(_TestStreamProcessor):
    @pytest.fixture
    def processor(self, consumer):
        return consumer 

    def test_isConsumer(self, consumer):
        assert consumer.is_consumer()

    def test_consumesValuesOfType(self, consumer, env_params, max_amount, test_values, result):
        env = consumer.get_initial_env(*env_params)
        t = consumer.type_in()

        def upstream():
            if len(test_values) == 0:
                raise Exhausted()

            return test_values.pop(0)

        def downstream(val):
            assert False

       
        try:
            count = 0
            while True and count < max_amount:
                count += 1
                res = consumer.step(env, upstream, downstream)
                if isinstance(res, Stop):
                    break 
        except Exhausted:
            assert isinstance(res, MayResume)
        finally:
            consumer.shutdown_env(env)

        if result != self._NoValue and not isinstance(res, Resume):
            assert result == res.result 

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

    def test_transformsValuesAccordingToTypes(self, pipe, env_params, max_amount, test_values, result):
        env = pipe.get_initial_env(*env_params)
        tin = pipe.type_in()
        tout = pipe.type_out()
        def upstream():
            if len(test_values) == 0:
                raise Exhausted()
            v = test_values.pop(0) 
            assert tin.contains(v)
            return v

        def downstream(val):
            assert tout.contains(val)

        try:
            for i in range(0, min(max_amount, len(test_values))):
                res = pipe.step(env, upstream, downstream)
                if isinstance(res, Stop):
                    return
        except StopIteration:
            assert isinstance(res, MayResume)
            return
        finally:
            pipe.shutdown_env(env)

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
        return()

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

    def get_initial_env(self):
        return 0

    def step(self, env, await, send):
        if env >= 100:
            raise RuntimeError("I did not expect this to run that long...")
        env += 1
        send(self.value)
        return MayResume()

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
    def get_initial_env(self, *params):
        return [] 
    def shutdown_env(self, env):
        pass

    def step(self, env, await, send):
        env.append(await())

        if self.max_amount is not None and len(env) >= self.max_amount:
            return Stop(env)

        return MayResume(env)

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

    def step(self, env, await, send):
        send(self.trafo(await()))
        return MayResume()

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
