# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr import (Producer, Consumer, Pipe, MayResume, Stop, Resume, ConstP,
                    ListP, ListC, pipe)
from test_core import _TestProducer, _TestConsumer, _TestPipe 
from streamr.types import Type, unit

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


class TestListProducer(_TestProducer):
    @pytest.fixture(params =
        [ (ListP(vlist = [10]*10), [10]*10, ())
        , (ListP(item_type = int), [10]*10, ([10]*10,))
        ])
    def producers(self, request, max_amount):
        return request.param

    @pytest.fixture
    def max_amount(self):
        return 10

    @pytest.fixture
    def producer(self, producers):
        return producers[0]

    @pytest.fixture
    def env_params(self, producers):
        return producers[2]

    @pytest.fixture
    def result(self, producers):
        return producers[1]

    def test_eitherListOrItemType(self):
        with pytest.raises(TypeError) as excinfo:
            ListP()
        assert "item_type" in str(excinfo.value)

    def test_valueConstructorOrEnv(self):
        l = [10]
        c1 = ListP(l)
        assert c1.type_out() == Type.get(int)
        assert c1.get_initial_env()
        with pytest.raises(TypeError) as excinfo:
           c1.get_initial_env(l) 
        assert "constructor" in str(excinfo.value)

        c2 = ListP(item_type = int)
        assert c2.type_out() == Type.get(int)
        with pytest.raises(TypeError) as excinfo:
            c2.get_initial_env()
        assert "list" in str(excinfo.value)
        assert c2.get_initial_env(l)

    def test_noEmptyListConstructor(self):
        with pytest.raises(ValueError) as excinfo:
            ListP([])
        assert "empty" in str(excinfo.value)

    def test_noMixedTypesConstructor(self):
        with pytest.raises(TypeError) as excinfo:
            ListP([1, "foo"])
        assert "item" in str(excinfo.value)

    def test_constWithListHashUnitInitType(self):
        p = ListP([10]*10)
        assert p.type_init() == unit


class TestListConsumer(_TestConsumer):
    @pytest.fixture( params = ["result", "append"])
    def style(self, request):
        return request.param

    @pytest.fixture
    def consumer(self, style):
        if style == "result":
            return ListC()
        if style == "append":
            return ListC([])

    @pytest.fixture
    def result(self, style, max_amount):
        if style == "result":
            return [10] * max_amount
        if style == "append":
            return ()

    @pytest.fixture
    def test_values(self, style, max_amount):
        return [10] * max_amount

    def test_append(self):
        l = []
        c = ListC(l)
        inp = [10] * 10

        sp = ListP(inp) >> c
        sp.run()

        assert l == inp 

    def test_replaceResultType(self):
        c = ListC() # List consumes values and results
                    # in a list of those values. Yet
                    # is has no definite type...
        assert c.type_in().is_variable()

        p = ConstP(10)
        assert p.type_out() == int
        
        sp = p >> c
        assert sp.type_result() == [int]

from streamr.simple import LambdaPipe
        
class TestLambdaPipe(_TestPipe):
    @pytest.fixture( params = 
        [ (int,int,lambda x:x*2, [(i, i*2) for i in range(0,10)])
        , (str,str,lambda x:"\"%s\""%x, [("str", "\"str\"")]*10)
        ] )
    def types(self, request):
        return request.param

    @pytest.fixture
    def pipe(self, types):
        return LambdaPipe(types[0], types[1], types[2])

    @pytest.fixture
    def test_values(self, types):
        return [i[0] for i in types[3]]

    @pytest.fixture
    def result(self, types):
        return [i[1] for i in types[3]]

class TestPipeDecorator(_TestPipe):
    @pytest.fixture
    def pipe(self):
        @pipe(int, int)
        def double(a):
            return 2 * a
        return double

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [2 * i for i in range(0, 10)]    
