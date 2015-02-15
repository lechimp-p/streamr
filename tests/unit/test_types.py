# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr.types import *

class Base(object):
    pass
class Sub(Base):
    pass

class Other(object):
    pass

@pytest.fixture
def base():
    return Type.get(Base)

@pytest.fixture
def sub():
    return Type.get(Sub)

@pytest.fixture
def other():
    return Type.get(Other)



class TestPyTypes(object):
    def test_equality(self, base, sub):
        assert base == base
        assert sub == sub
        assert not (base == sub)
        assert not (sub == base)

    def test_lowerThan(self, base, sub):
        assert not (base < base)
        assert base <= base
        assert not (sub < sub)
        assert sub <= sub 
        assert base < sub
        assert base <= sub
        assert not (sub < base)
        assert not (sub <= base) 
 
    def test_greaterThan(self, base, sub):
        assert not (base > base)
        assert base >= base
        assert not (sub > sub)
        assert sub >= sub 
        assert not (base > sub)
        assert not (base >= sub)
        assert sub > base
        assert sub >= base

    def test_noRelationToOther(self, base, sub, other):
        assert not (base == other)
        assert base != other
        assert not (base <= other)
        assert not (base < other)
        assert not (base >= other)
        assert not (base > other)

class TestProductTypes(object):
    def test_comparison1(self, base, sub):
        t1 = Type.get(base, base)
        t2 = Type.get(base, base)

        assert t1 == t2
        assert t1 <= t2
        assert t1 >= t2
        assert t2 <= t1
        assert t2 >= t2
        assert not (t1 < t2)
        assert not (t1 > t2)
        assert not (t2 < t1)
        assert not (t2 > t1)
        assert not (t2 != t1)

    def test_comparison2(self, base, sub):
        t1 = Type.get(base, sub)
        t2 = Type.get(base, base)

        assert t1 != t2
        assert not (t1 == t2)
        assert t2 != t1
        assert not (t2 == t1)
        assert t1 >= t2
        assert not (t1 > t2)
        assert not (t1 <= t2)
        assert not (t1 < t2)
        assert not (t2 >= t1)
        assert not (t2 > t1)
        assert t2 <= t1
        assert not (t2 < t1)

    def test_comparison3(self, base, sub):
        t1 = Type.get(sub, sub)
        t2 = Type.get(base, base)

        assert t1 != t2
        assert not (t1 == t2)
        assert t2 != t1
        assert not (t2 == t1)
        assert t1 >= t2
        assert t1 > t2
        assert not (t1 <= t2)
        assert not (t1 < t2)
        assert not (t2 >= t1)
        assert not (t2 > t1)
        assert t2 <= t1
        assert t2 < t1

    def test_comparison4(self, base, other):
        t1 = Type.get(base, other)
        t2 = Type.get(base, base)

        assert t1 != t2
        assert not (t1 == t2)
        assert t2 != t1
        assert not (t2 == t1)
        assert not (t1 >= t2)
        assert not (t1 > t2)
        assert not (t1 <= t2)
        assert not (t1 < t2)
        assert not (t2 >= t1)
        assert not (t2 > t1)
        assert not (t2 <= t1)
        assert not (t2 < t1)

    def test_isomorphism(self, base):
        t1 = Type.get(ProductType.get(base, base), base)
        t2 = Type.get(base, ProductType.get(base, base))
        t3 = Type.get(base, base, base)

        assert t1 == t2
        assert t2 == t1
        assert t1 == t3
        assert t3 == t1
        assert t3 == t2
        assert t2 == t3

class TestListTypes(object):
    def test_comparison(self, base, sub, other):
        t1 = Type.get([base])
        t2 = Type.get([sub])
        t3 = Type.get([other])

        assert t1 == t1
        assert t1 <= t2
        assert t2 >= t1
        assert t1 < t2
        assert t2 > t1
        assert t1 != t2
        assert t1 != t3
        assert not (t1 == t3)
        assert not (t1 >= t3)
        assert not (t1 <= t3)
        assert not (t1 > t3)
        assert not (t1 < t3)

class TestArrowType(object):
    def test_comparison1(self, base):
        t1 = ArrowType.get(base, base)
        t2 = ArrowType.get(base, base)

        assert t1 == t2
        assert t1 >= t2
        assert t1 <= t2
        assert not(t1 < t2)
        assert not(t1 > t2)
        assert not(t1 != t2)
        assert t2 == t1
        assert t2 >= t1
        assert t2 <= t1
        assert not(t2 < t1)
        assert not(t2 > t1)
        assert not(t2 != t1)

    def test_comparison2(self, base, sub, other):
        t1 = ArrowType.get(base, other) # base -> other
        t2 = ArrowType.get(sub, other)  # sub -> other

        # This looks counterintuitive, since it reverses the order of the >
        # applied on sub and base.
        # Reasoning is as such: If if have a function from base to other, i can
        # use it in places where i need a function from sub to other, since sub
        # should contain the same information than base. On the other hand, i can't
        # use a function from sub to other in places where a function from base to
        # other is expected, since the function might need information from sub, 
        # that base can't provide.
        assert t1 >= t2
        assert t1 > t2
        assert t2 <= t1
        assert t2 < t1
        assert not (t1 == t2)
        assert t1 != t2

    def test_comparison3(self, base, sub, other):
        t1 = ArrowType.get(other, base) # other -> base
        t2 = ArrowType.get(other, sub)  # other -> sub

        # Here the same order of > applies then on base and sub.
        assert t2 >= t1
        assert t2 > t1
        assert t1 < t2
        assert t1 <= t2
        assert not (t1 == t2)
        assert t1 != t2

class TestTypeVar(object):
    def test_uniqueness(self, base):
        t1 = Type.get()
        t2 = Type.get()

        assert t1 == t1
        assert t1 != t2
        assert t2 == t2
        assert t2 != t1

class TestApplicationType(object):
    def test_application1(self, base):
        fun = ArrowType.get(base, base)
        app = ApplicationType.get(fun, base)

        assert app == base
