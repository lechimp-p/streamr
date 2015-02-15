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
    return PyType.get(Base)

@pytest.fixture
def sub():
    return PyType.get(Sub)

@pytest.fixture
def other():
    return PyType.get(Other)



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
        t1 = ProductType.get(base, base)
        t2 = ProductType.get(base, base)

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
        t1 = ProductType.get(base, sub)
        t2 = ProductType.get(base, base)

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
        t1 = ProductType.get(sub, sub)
        t2 = ProductType.get(base, base)

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
        t1 = ProductType.get(base, other)
        t2 = ProductType.get(base, base)

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
        t1 = ProductType.get(ProductType.get(base, base), base)
        t2 = ProductType.get(base, ProductType.get(base, base))
        t3 = ProductType.get(base, base, base)

        assert t1 == t2
        assert t2 == t1
        assert t1 == t3
        assert t3 == t1
        assert t3 == t2
        assert t2 == t3
