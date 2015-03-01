# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr import Producer, Consumer, Pipe, MayResume, Stop, Resume, ConstP
from test_core import _TestProducer, _TestConsumer, _TestPipe 
from streamr.types import Type

###############################################################################
#
# Mocks for simple processors including tests.
#
###############################################################################

# Producer

class MockProducer(Producer):
    def __init__(self, ttype):
        super(MockProducer,self).__init__(ttype,ttype)
    def get_initial_env(self, value):
        return value
    def produce(self, env, send):
        send(env)
        return MayResume()

class TestMockProducer(_TestProducer):
    @pytest.fixture( params = 
        [ (int,1)
        , (str,"str")
        , ([int],list(range(0,10)))
        ] )
    def types(self, request):
        return request.param

    @pytest.fixture
    def producer(self, types):
        return MockProducer(types[0])

    @pytest.fixture
    def env_params(self, types):
        return (types[1],)


# Consumer

class MockConsumer(Consumer):
    def __init__(self, ttype):
        super(MockConsumer,self).__init__((),[ttype],ttype)
    def get_initial_env(self):
        return [] 
    def consume(self, env, await):
        env.append(await())
        return MayResume(env)

class TestMockConsumer(_TestConsumer):
    @pytest.fixture( params = 
        [ (int,list(range(0,10)))
        , (str,["str"]*10)
        , ([int],[list(range(0,10))]*10)
        ] )
    def types(self, request):
        return request.param

    @pytest.fixture
    def consumer(self, types):
        return MockConsumer(types[0])

    @pytest.fixture
    def result(self, types):
        return list(types[1])

    @pytest.fixture
    def test_values(self, types):
        return list(types[1])

# Pipe

class MockPipe(Pipe):
    def __init__(self, type_in, type_out, trafo):
        super(MockPipe, self).__init__((), type_in, type_out)
        self.trafo = trafo
    def transform(self, env, await, send):
        send(self.trafo(await()))
        return MayResume()

class TestMockPipe(_TestPipe):
    @pytest.fixture( params = 
        [ (int,int,lambda x:x*2, [(i, i*2) for i in range(0,10)])
        , (str,str,lambda x:"\"%s\""%x, [("str", "\"str\"")]*10)
        ] )
    def types(self, request):
        return request.param

    @pytest.fixture
    def pipe(self, types):
        return MockPipe(types[0], types[1], types[2])

    @pytest.fixture
    def test_values(self, types):
        return [i[0] for i in types[3]]

    @pytest.fixture
    def result(self, types):
        return [i[1] for i in types[3]]

###############################################################################
#
# Test for concrete stream processors in simple
#
###############################################################################


class TestConstProducer(_TestProducer):
    @pytest.fixture(params =
        [ (ConstP(value = 10), 10, ())
        , (ConstP(value_type = int), 10, (10,))
        ])
    def producers(self, request, max_amount):
        par = list(request.param)
        par[1] = [par[1]] * max_amount
        return par

    @pytest.fixture
    def producer(self, producers):
        return producers[0]

    @pytest.fixture
    def env_params(self, producers):
        return producers[2]

    @pytest.fixture
    def result(self, producers):
        return producers[1]

    def test_eitherValueOrValueType(self):
        with pytest.raises(TypeError) as excinfo:
            ConstP()
        assert "value_type" in str(excinfo.value)

    def test_valueConstructorOrEnv(self):
        num = 10
        c1 = ConstP(num)
        assert c1.type_out() == Type.get(int)
        assert c1.get_initial_env() == num 
        with pytest.raises(TypeError) as excinfo:
           c1.get_initial_env(num) 
        assert "constructor" in str(excinfo.value)

        c2 = ConstP(value_type = int)
        assert c2.type_out() == Type.get(int)
        with pytest.raises(TypeError) as excinfo:
            c2.get_initial_env()
        assert "value" in str(excinfo.value)
        assert c2.get_initial_env(num) == num
