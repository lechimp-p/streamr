# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr import ( Producer, Consumer, Pipe, MayResume, Stop, Resume, const
                    , from_list, to_list, transformation, pipe, pass_if, tee
                    , nop, maps)
from test_core import _TestProducer, _TestConsumer, _TestPipe 
from streamr.types import Type, unit
from streamr.core import StreamProcessor

###############################################################################
#
# Mocks for simple processors including tests.
#
###############################################################################

# Producer

class MockProducer(Producer):
    def __init__(self, ttype):
        super(MockProducer,self).__init__(ttype,ttype)
    def get_initial_env(self, params):
        assert len(params) == 1
        return params[0] 
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
    def get_initial_env(self, params):
        assert params == ()
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
        [ (const(value = 10), 10, ())
        , (const(value_type = int), 10, (10,))
        , (const(value = 10, amount = 1), [10], ()) 
        ])
    def producers(self, request, max_amount):
        par = list(request.param)
        if not isinstance(par[1], list):
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
            const()
        assert "value_type" in str(excinfo.value)

    def test_valueConstructorOrEnv(self):
        num = 10
        c1 = const(num)
        assert c1.type_out() == Type.get(int)
        assert c1.get_initial_env(()) == [num,0] 
        with pytest.raises(TypeError) as excinfo:
           c1.get_initial_env((num,)) 
        assert "type" in str(excinfo.value)

        c2 = const(value_type = int)
        assert c2.type_out() == Type.get(int)
        with pytest.raises(TypeError) as excinfo:
            c2.get_initial_env(())
        assert "value" in str(excinfo.value)
        assert c2.get_initial_env((num,)) == [num,0]

    def test_amount(self):
        sp = const(10, amount = 2) >> to_list()
        assert sp.run() == [10, 10]


class TestListProducer(_TestProducer):
    @pytest.fixture(params =
        [ (from_list(vlist = [10]*10), [10]*10, ())
        , (from_list(item_type = int), [10]*10, ([10]*10,))
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
            from_list()
        assert "item_type" in str(excinfo.value)

    def test_valueConstructorOrEnv(self):
        l = [10]
        c1 = from_list(l)
        assert c1.type_out() == Type.get(int)
        assert c1.get_initial_env(())
        with pytest.raises(TypeError) as excinfo:
           c1.get_initial_env((l,)) 
        assert "type" in str(excinfo.value)

        c2 = from_list(item_type = int)
        assert c2.type_out() == Type.get(int)
        with pytest.raises(TypeError) as excinfo:
            c2.get_initial_env(())
        assert "type" in str(excinfo.value)
        assert c2.get_initial_env((l,))

    def test_noEmptyListConstructor(self):
        with pytest.raises(ValueError) as excinfo:
            from_list([])
        assert "empty" in str(excinfo.value)

    def test_noMixedTypesConstructor(self):
        with pytest.raises(TypeError) as excinfo:
            from_list([1, "foo"])
        assert "item" in str(excinfo.value)

    def test_constWithListHashUnitInitType(self):
        p = from_list([10]*10)
        assert p.type_init() == unit

    def test_complexTypeOut(self):
        p = from_list([([10], "foo")])
        assert p.type_out() == Type.get([int], str)


class TestListProducerProduct(_TestProducer):
    @pytest.fixture
    def max_amount(self):
        return 3 

    @pytest.fixture
    def producer(self):
        return from_list([1,2,3]) * from_list([4,5,6]) 

    @pytest.fixture
    def env_params(self):
        return ()

    @pytest.fixture
    def result(self):
        return [(1,4), (2,5), (3,6)] 


class TestListConsumer(_TestConsumer):
    @pytest.fixture( params = ["result", "append"])
    def style(self, request):
        return request.param

    @pytest.fixture
    def consumer(self, style):
        if style == "result":
            return to_list()
        if style == "append":
            return to_list([])

    @pytest.fixture
    def result(self, style, max_amount):
        if style == "result":
            return [10] * (max_amount-1)
        if style == "append":
            return ()

    @pytest.fixture
    def test_values(self, style, max_amount):
        return [10] * max_amount

    def test_append(self):
        l = []
        c = to_list(l)
        inp = [10] * 10

        sp = from_list(inp) >> c
        sp.run()

        assert l == inp 

    def test_replaceResultType(self):
        c = to_list() # List consumes values and results
                    # in a list of those values. Yet
                    # is has no definite type...
        assert c.type_in().is_variable()

        p = const(10)
        assert p.type_out() == int
        
        sp = p >> c
        assert sp.type_result() == [int]

    def test_maxAmount(self):
        c = to_list(max_amount = 2)
        sp = const(10) >> c
        assert sp.run() == [10, 10]

    def test_empty(self):
        pr = from_list(["foo"])
        c = to_list()
        sp = pr >> pass_if(str, lambda x: False) >> c
        res = sp.run()
        assert res == []


class TestTransformationDecorator(_TestPipe):
    @pytest.fixture
    def pipe(self):
        @pipe(int, int)
        def double(await, send):
            send(2 * await())
        return double

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [2 * i for i in range(0, 10)]    


from streamr.simple import LambdaPipe
        
class TestLambdaPipe(_TestPipe):
    @pytest.fixture( params = 
        [ (int,int,lambda a,s:s(a()*2), [(i, i*2) for i in range(0,10)])
        , (str,str,lambda a,s:s("\"%s\""%a()), [("str", "\"str\"")]*10)
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
        def double(await, send):
            send(2 * await())
        return double

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [2 * i for i in range(0, 10)]    


class TestTransformationDecorator(_TestPipe):
    @pytest.fixture
    def pipe(self):
        @transformation(int, int)
        def double(a):
            return 2 * a
        return double

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [2 * i for i in range(0, 10)]    


from streamr.simple import FilterPipe
        
class TestFilterPipe(_TestPipe):
    @pytest.fixture
    def pipe(self):
        class MyFilterPipe(FilterPipe):
            def predicate(self, env, a):
                return a % 2 == 0
        return MyFilterPipe((), int)

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [2 * i for i in range(0, 5)]    


from streamr.simple import LambdaFilterPipe

class TestLambdaFilterPipe(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return LambdaFilterPipe(int, lambda x : x % 2 == 0)

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [2 * i for i in range(0, 5)]    

                
class TestFilterDecorator(_TestPipe):
    @pytest.fixture
    def pipe(self):
        @pass_if(int)
        def even(a):
            return a % 2 == 0
        return even 

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [2 * i for i in range(0, 5)]    

class TestTee(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return tee()

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [(i,i) for i in range(0, 10)]    

    def test_typeOut(self, pipe):
        assert pipe.type_out() == Type.get(pipe.type_in(), pipe.type_in())

class TestNop(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return nop() 

    @pytest.fixture
    def test_values(self):
        return [i for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [i for i in range(0, 10)]

class TestListProducerProductAndListConsumerAndNop(_TestProducer):
    @pytest.fixture
    def max_amount(self):
        return 3 

    @pytest.fixture
    def producer(self):
        return (   from_list([1,2,3]) * from_list([4,5,6]) 
                >> (to_list() * nop()))

    @pytest.fixture
    def env_params(self):
        return ()

    @pytest.fixture
    def result(self):
        return [4, 5, 6] 

class TestListProducerProductAndListConsumerAndNop2(_TestProducer):
    @pytest.fixture
    def max_amount(self):
        return 3 

    @pytest.fixture
    def producer(self):
        return (  (from_list([1,2,3]) >> to_list())
                * (from_list([4,5,6]) >> nop()))

    @pytest.fixture
    def env_params(self):
        return ()

    @pytest.fixture
    def result(self):
        return [4, 5, 6] 



class TestMaps(_TestPipe):
    @pytest.fixture
    def pipe(self):
        @transformation(int, str)
        def double(a):
            return"%s" % (2*a)
        return maps(double) 

    @pytest.fixture
    def test_values(self):
        return [[i]*10 for i in range(0, 10)]

    @pytest.fixture
    def result(self):
        return [["%s" % (i*2)]*10 for i in range(0, 10)]

    def test_typesCorrect(self, pipe):
        assert pipe.type_in() == [int]
        assert pipe.type_out() == [str]
        assert pipe.type_init() == ()
        assert pipe.type_result() == ()

    def test_pipeOnly(self, pipe):
        with pytest.raises(TypeError) as excinfo:
            maps(from_list([1]))
        assert "pipe" in str(excinfo.value)
        with pytest.raises(TypeError) as excinfo:
            maps(to_list())
        assert "pipe" in str(excinfo.value)
        with pytest.raises(TypeError) as excinfo:
            maps(from_list([1]) >> to_list())
        assert "pipe" in str(excinfo.value)

    class TimesX(StreamProcessor):
        def __init__(self):
            super(TestMaps.TimesX, self).__init__(int, int, int, int)
        def get_initial_env(self, param):
            assert len(param) == 1
            return param[0] 
        def step(self, env, await, send):
            send(env * await())
            return MayResume(env)

    def test_mapsWithInitAndResult(self):
        sp = from_list([1,2,3]) >> self.TimesX() >> to_list()     
        assert sp.run(1) == (1, [1,2,3]) 
        assert sp.run(2) == (2, [2,4,6]) 

        sp = from_list([[1,2,3], [4,5,6]]) >> maps(self.TimesX()) >> to_list()
        assert sp.type_init() == int
        assert sp.type_result() == ([int], [[int]])
        assert sp.run(1) == ([1, 1], [[1,2,3],[4,5,6]])
        assert sp.run(2) == ([2, 2], [[2,4,6],[8,10,12]])

    def test_mapsPotentialBug1(self):
        sp = from_list([[1,2,3]]) >> maps(self.TimesX()) >> to_list()
        assert sp.type_init() == int
        assert sp.type_result() == ([int], [[int]])
        assert sp.run(1) == ([1], [[1,2,3]])

    def test_mapsPotentialBug2(self):
        sp = from_list([[1,2,3], []]) >> maps(self.TimesX()) >> to_list()
        assert sp.type_init() == int
        assert sp.type_result() == ([int], [[int]])
        assert sp.run(1) == ([1, 1], [[1,2,3], []])

    def test_initForEveryList(self):
        init_count = [0]

        class CheckGetInitialEnv(StreamProcessor):
            def __init__(self):
                super(CheckGetInitialEnv, self).__init__((), (), int, int)
            def get_initial_env(self, params):
                assert params == ()
                init_count[0] += 1
            def step(self, env, await, send):
                send(await())
                return MayResume()

        sp = from_list([[1,2,3], [4,5,6]]) >> maps(CheckGetInitialEnv()) >> to_list()
        assert sp.run() == [[1,2,3], [4,5,6]]
        assert init_count[0] == 2

def test_combinatorBug():
    fl1 = from_list([1,2,3])
    fl2 = from_list([4.0,5.0,6.0])
    prod = fl1 * fl2 
    assert prod.processors == [fl1, fl2]

    tl = to_list()
    n1 = nop()
    tl_n = tl * n1
    assert tl_n.processors == [tl, n1]

    tl2 = to_list()

    sp = prod >> (nop() * nop()) >> (tl * tl2)
    assert sp.run() == ([1,2,3], [4.0,5.0,6.0])

    sp = prod >> tl_n >> tl2 
    assert sp.run() == ([1,2,3], [4.0,5.0,6.0])

def test_combinatorSymmetry():
    tl = to_list()
    tl_n = nop() * tl
    tl2 = to_list()
    prod = from_list([1,2,3]) * from_list([4.0,5.0,6.0])

    sp = prod >> tl_n >> tl2
    assert sp.run() == ([4.0,5.0,6.0],[1,2,3])


