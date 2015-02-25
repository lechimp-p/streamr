import pytest

from streamr import *
from streamr.types import Type


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
        assert isinstance(pi >> pi, Pipe)

    # Pr >> Pi = Pr
    def test_PrCompPi(self, pr, pi):
        assert isinstance(pr >> pi, Producer)

    # Pi >> Co = Co
    def test_PiCompCo(self, pi, co):
        assert isinstance(pi >> co, Consumer)

    # Pr >> Co = SP
    def test_PrCompCo(self, pr, co):
        assert isinstance(pr >> co, StreamProcess)

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
        assert sp1.run(None) == [10]*10

        sp2 = pr_str >> co_any
        assert sp2.run(None) == ["Hello World!"]*10

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
        

class TestStreamProcessResults(object):
    def test_PrCompCo(self, pr, co):
        sp = pr >> co
        assert sp.run(None) == [10]*10
    def test_PrCompPiCompCo(self, pr, pi, co):
        sp = pr >> pi >> co
        assert sp.run(None) == [20]*10
    def test_PrCompPiCompPiCompCo(self, pr, pi, co):
        sp = pr >> pi >> pi >> co
        assert sp.run(None) == [40]*10


class TestStacking(object):
    def test_stackProducers(self, pr, pr_str):
        p = pr * pr_str

        assert isinstance(p, Producer)
        assert p.type_out() == pr.type_out() * pr_str.type_out() 

    def test_stackPipes(self, pi, pi_any):
        p = pi * pi_any

        assert isinstance(p, Pipe)
        assert p.type_in() == pi.type_in() * pi_any.type_in()
        assert p.type_out() == pi.type_out() * pi_any.type_out()

    def test_stackConsumers(self, co, co_str):
        c = co * co_str

        assert isinstance(c, Consumer)
        assert c.type_in() == co.type_in() * co_str.type_in()

    def test_stackPipeConsumer(self, pi, co):
        p = pi * co

        assert isinstance(p, MixedStreamProcessor)
        assert p.type_in() == pi.type_in() * co.type_in()
        assert p.type_out() == pi.type_out()

    def test_stackProducerPipe(self, pr, pi):
        p = pr * pi

        assert isinstance(p, MixedStreamProcessor)
        assert p.type_in() == pi.type_in()
        assert p.type_out() == pr.type_out() * pi.type_in()

    def test_stackStackedProducerAndMixedStack1(self, pr, pi, co):
        p = (pr * pr) >> (pi * co)

        assert isinstance(p, MixedStreamProcessor)
        assert p.type_in() is Type.get(None)
        assert p.type_out() == pi.type_out()

    def test_stackStackedProducerAndMixedStack2(self, pr, pi, co):
        p = (pr * pr * pr) >> (pi * pi * co)

        assert isinstance(p, MixedStreamProcessor)
        assert p.type_in() is Type.get(None)
        assert p.type_out() == pi.type_out() * pi.type_out()

    def test_stackStackedProducerAndMixedStack3(self, pr, pi, co):
        p = (pr * pr * pr) >> (pi * pi * co) >> (pi * co)

        assert isinstance(p, MixedStreamProcessor)
        assert p.type_in() is Type.get(None)
        assert p.type_out() == pi.type_out()

    def test_result1(self, pr, pi, co):
        sp = (pr * pr) >> (pi * pi) >> (co * co)
        assert sp.run(None) == ([20]*10, [20]*10)

    def test_result2(self, pr, pi, co):
        sp = (pr * pr) >> (pi * co) >> co
        assert sp.run(None) == ([20]*10, [10]*10)

    def test_result3(self, pr, pi, co):
        sp = pr >> (pi * pr) >> (co * co)
        assert sp.run(None) == ([20]*10, [10]*10)

    def test_result4(self, pr, pi, co):
        sp = (pr * pr) >> (pi * pi) >> (pi * pi) >> (co * co)
        assert sp.run(None) == ([40]*10, [40]*10)

    def test_result5(self, pr, pi, co):
        sp = (pr * pr * pr) >> (pi * pi * co) >> (pi * co) >> co
        assert sp.run(None) == ([40]*10, [20]*10, [10]*10)

    def test_result6(self, pr, pi, co):
        sp = pr >> (pi * pr) >> (pi * pi * pr) >> (co * co * co)
        assert sp.run(None) == ([40]*10, [20]*10, [10]*10)

    def test_result7(self, pr, pi, co):
        sp = (pr * pr * pr) >> (co * pi * pi) >> (co * pi) >> co
        assert sp.run(None) == ([10]*10, [20]*10, [40]*10)

    def test_result8(self, pr, pi, co):
        sp = pr >> (pr * pi) >> (pr * pi * pi) >> (co * co * co)
        assert sp.run(None) == ([10]*10, [20]*10, [40]*10)

    def test_result9(self, pr, pi, co):
        sp = pr >> (pr * pi * pr) >> (pi * co * pi) >> (co * co)

        assert sp.run(None) == ([20]*10, [20]*10, [20]*10)




    


###############################################################################
#
# Base classes for tests on stream parts.
#
###############################################################################

class _TestProducer(object):
    def test_isInstanceOfProducer(self, producer):
        assert isinstance(producer, Producer) 

    def test_typeOut(self, producer):
        assert isinstance(producer.type_out(), Type)

    def test_typeOutIsNotVariable(self, producer):
        assert not producer.type_out().is_variable()

    def test_typeOfProducedValues(self, producer, max_amount):
        env = producer.get_initial_env(None)
        t = producer.type_out()
        count = 0
        for var in producer.produce(env):
            assert t.contains(var) 

            count += 1
            if count > max_amount:
                return 

class _TestConsumer(object):
    def test_isInstanceOfConsumer(self, consumer):
        assert isinstance(consumer, Consumer)

    def test_typeIn(self, consumer):
        assert isinstance(consumer.type_in(), Type)

    def test_consumesValuesOfType(self, consumer, test_values):
        env = consumer.get_initial_env(None)
        t = consumer.type_in()
        gen = (i for i in test_values)
        def upstream():
            v = next(gen)
            assert t.contains(v)
            yield v

        us = upstream()
        
        res = None
        while True:
            try:
                res = consumer.consume(env, us)
                if isinstance(res, Stop):
                    return
            except StopIteration:
                assert isinstance(res, MayResume)
                return

class _TestPipe(object):
    def test_isInstanceOfPipe(self, pipe):
        assert isinstance(pipe, Pipe)

    def test_typeIn(self, pipe):
        assert isinstance(pipe.type_in(), Type)

    def test_typeOut(self, pipe):
        assert isinstance(pipe.type_out(), Type)

    def test_transformsValuesAccordingToTypes(self, pipe, test_values, max_amount):
        env = pipe.get_initial_env(None)
        tin = pipe.type_in()
        tout = pipe.type_out()
        gen = (i for i in test_values)
        def upstream():
            v = next(gen)
            assert tin.contains(v)
            yield v
        
        count = 0
        for var in pipe.transform(env, upstream()):
            assert tout.contains(var)

            count += 1
            if count > max_amount:
                return


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

def TestPrependPipe(_TestConsumer):
    @pytest.fixture
    def consumer(self, pi, co):
        return pi >> co

    @pytest.fixture
    def test_values(self):
        return range(0, 10)

class TestFusePipe(_TestPipe):
    @pytest.fixture
    def pipe(self, pi):
        return pi >> pi

    @pytest.fixture
    def test_values(self):
        return range(0, 10)

    @pytest.fixture
    def max_amount(self):
        return 10   

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

class TestStackProducer(_TestProducer):
    @pytest.fixture
    def producer(self, pr):
        return pr * pr

    @pytest.fixture
    def max_amount(self):
        return 10   

class TestStackConsumer(_TestConsumer):
    @pytest.fixture
    def consumer(self, co):
        return co * co

    @pytest.fixture
    def test_values(self):
        return [(i,i) for i in range(0, 10)]

###############################################################################
#
# Mocks for stream parts including tests.
#
###############################################################################

class MockProducer(Producer):
    def __init__(self, ttype, value):
        super(MockProducer, self).__init__(None, ttype)
        self.value = value
    def produce(self, env):
        while True:
            yield self.value

class TestMockProducer(_TestProducer):
    @pytest.fixture
    def producer(self):
        return MockProducer(int, 10)

    @pytest.fixture
    def max_amount(self):
        return 10

class MockConsumer(Consumer):
    def __init__(self, ttype, max_amount = None):
        super(MockConsumer, self).__init__(None, [ttype], ttype)
        self.max_amount = max_amount
    def get_initial_env(self, _):
        return [] 
    def shutdown_env(self, env):
        pass
    def consume(self, env, upstream):
        try:
            env.append(next(upstream))
        except StopIteration:
            return Stop(env)

        if self.max_amount is not None and len(env) >= self.max_amount:
            return Stop(env)

        return MayResume(env)

class TestMockConsumer(Consumer):
    @pytest.fixture
    def consumer(self):
        return MockConsumer(int)

    @pytest.fixture
    def test_values(self):
        return range(0, 10)

class MockPipe(Pipe):
    def __init__(self, type_in, type_out, transform = None):
        super(MockPipe, self).__init__(None, type_in, type_out)
        self.trafo = (lambda x : x) if transform is None else transform
    def transform(self, env, upstream):
        for var in upstream:
            yield self.trafo(var)

class TestMockPipe(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return MockPipe(int, int)

    @pytest.fixture
    def test_values(self):
        return range(0, 10)

    @pytest.fixture
    def max_amount(self):
        return 10
