# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr.types import *

class TestPyTypes(object):
    class Base(object):
        pass
    class Sub(Base):
        pass
    
    @pytest.fixture
    def base(self):
        print("HELLO WORLD!")
        return PyType(TestPyTypes.Base)

    @pytest.fixture
    def sub(self):
        self.sub = PyType(TestPyTypes.Sub)

    def test_equality(base, sub):
        assert base == base
        assert sub == sub
        assert base == sub
        assert sub == base

    def test_lowerThan(base, sub):
        assert not (base < base)
        assert base <= base
        assert not (sub < sub)
        assert sub <= sub 
        assert base < sub
        assert base <= sub
        assert not (sub < base)
        assert not (sub <= base) 
 
    def test_greaterThan(base, sub):
        assert not (base > base)
        assert base >= base
        assert not (sub > sub)
        assert sub >= sub 
        assert not (base > sub)
        assert not (base >= sub)
        assert sub > base
        assert sub >= base
