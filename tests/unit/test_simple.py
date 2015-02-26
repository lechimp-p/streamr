# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr import Producer, Consumer, Pipe, MayResume, Stop, Resume
from test_core import _TestProducer, _TestConsumer, _TestPipe 

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

    
