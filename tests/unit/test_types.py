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
        return PyType(TestPyTypes.Base)

    @pytest.fixture
    def sub(self):
        return PyType(TestPyTypes.Sub)

    def test_equality(self, base, sub):
        assert base == base
        assert sub == sub
        assert base == sub
        assert sub == base

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
