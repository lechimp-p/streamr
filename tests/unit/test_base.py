import pytest

from streamr import *
from streamr.types import Type


###############################################################################
#
# Tests for implementations in base.
#
###############################################################################

class TestCompositionBase():
    """
    Test whether composition of different stream parts lead to the
    expected results.
    """

    @pytest.fixture
    def pr(self):
        return MockProducer(int, 10)
    
    @pytest.fixture
    def co(self):
        return MockConsumer(int)

    @pytest.fixture
    def pi(self):
        return MockPipe(int, int)

    @pytest.fixture
    def all_sps(self, pr, co, pi):
        return [pr, co, pi]

    # Objects from the classes need to respect the follwing rules, where abbreaviations
    # for the names are used
    #
    # any >> Pr = error
    def test_AnyCompPr(self, all_sps, pr):
        for sp in all_sps:
            with pytest.raises(TypeError) as excinfo:
                sp >> pr

    # Co >> any = error
    def test_CoCompAny(self, all_sps, co):
        for sp in all_sps:
            with pytest.raises(TypeError) as excinfo:
                co >> sp

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
            with pytest.raises(TypeError) as excinfo:
                spt >> sp

    # any >> SP = error
    def test_SPCompAny(self, all_sps, pr, co):
        spt = pr >> co
        for sp in all_sps:
            with pytest.raises(TypeError) as excinfo:
                sp >> spt


###############################################################################
#
# Base classes for tests on stream parts.
#
###############################################################################

class _TestProducer():
    def test_isInstanceOfProducer(self, producer):
        assert isinstance(producer, Producer) 

    def test_typeOut(self, producer):
        assert isinstance(producer.type_out(), Type)

    def test_typeOfProducedValues(self, producer, max_amount):
        env = producer.get_initial_env()
        t = producer.type_out()
        count = 0
        for var in producer.produce(env):
            assert t.contains(var) 

            count += 1
            if count > max_amount:
                return 

class _TestConsumer():
    def test_isInstanceOfConsumer(self, consumer):
        assert isinstance(consumer, Consumer)

    def test_typeIn(self, consumer):
        assert isinstance(consumer.type_in(), Type)

    def test_consumesValuesOfType(self, consumer, test_values):
        env = consumer.get_initial_env()
        t = consumer.type_in()
        gen = (i for i in test_values)
        def upstream():
            v = gen.__next__()
            assert t.contains(v)
            yield v

        consumer.consume(env, upstream())

class _TestPipe():
    def test_isInstanceOfPipe(self, pipe):
        assert isinstance(pipe, Pipe)

    def test_typeIn(self, pipe):
        assert isinstance(pipe.type_in(), Type)

    def test_typeOut(self, pipe):
        assert isinstance(pipe.type_out(), Type)

    def test_transformsValuesAccordingToTypes(self, pipe, test_values, max_amount):
        env = pipe.get_initial_env()
        tin = pipe.type_in()
        tout = pipe.type_out()
        gen = (i for i in test_values)
        def upstream():
            v = gen.__next__()
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
# Mocks for stream parts including tests.
#
###############################################################################

class MockProducer(Producer):
    def __init__(self, ttype, value):
        self.ttype = Type.get(ttype)
        self.value = value
    def type_out(self):
        return self.ttype
    def get_initial_env(self):
        return None
    def shutdown_env(self, env):
        pass
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
    def __init__(self, ttype):
        self.ttype = Type.get(ttype)
    def type_in(self):
        return self.ttype
    def get_initial_env(self):
        return None 
    def shutdown_env(self, env):
        pass
    def consume(self, env, upstream):
        return [v for v in upstream]  

class TestMockConsumer(Consumer):
    @pytest.fixture
    def consumer(self):
        return MockConsumer(int)

    @pytest.fixture
    def test_values(self):
        return range(0, 10)

class MockPipe(Pipe):
    def __init__(self, type_in, type_out, transform = None):
        self.tin = Type.get(type_in)
        self.tout = Type.get(type_out)
        self.trafo = lambda x : x if transform is None else transform
    def type_in(self):
        return self.tin
    def type_out(self):
        return self.tout
    def get_initial_env(self):
        return None 
    def shutdown_env(self, env):
        pass
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
