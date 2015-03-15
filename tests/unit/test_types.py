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

@pytest.fixture
def base_inst():
    return Base()

@pytest.fixture
def sub_inst():
    return Sub()

@pytest.fixture
def other_inst():
    return Other()

@pytest.fixture
def unit():
    return Type.get(())


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

    def test_contains(self, base, base_inst, sub_inst, other_inst):
        assert base.contains(base_inst)
        assert base.contains(sub_inst)
        assert not base.contains(other_inst)

    def test_containsIn(self):
        t1 = Type.get(int)
        assert t1.contains(10)
        assert not t1.contains("foo")

    def test_cached(self):
        t1 = Type.get(int)
        t2 = Type.get(int)
        assert t1 == t2


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

    def test_noIsomorphism(self, base):
        t1 = Type.get(ProductType.get(base, base), base)
        t2 = Type.get(base, ProductType.get(base, base))
        t3 = Type.get(base, base, base)

        assert t1 != t2
        assert t2 != t1
        assert t1 != t3
        assert t3 != t1
        assert t3 != t2
        assert t2 != t3

    def test_contains(self, base, base_inst, other_inst):
        t = Type.get((base, base))

        assert t.contains((base_inst, base_inst))
        assert not t.contains((base_inst, other_inst))

    def test_noConstructionCrashWithType(self):
        t1 = Type.get(ProductType.get(int, int), int)
        t2 = Type.get((int,int),int)
        
        assert t1 == t2
        

    def test_construct(self):
        t1 = Type.get(int,(int,int))
        t2 = Type.get(int,int,int)
        t3 = Type.get((int,int),int)

        l = [1,2,3]

        assert t1.contains(t1.construct(list(l)))
        assert t2.contains(t2.construct(list(l)))
        assert t3.contains(t3.construct(list(l)))

        assert t1.construct(list(l)) == (1,(2,3))
        assert t2.construct(list(l)) == (1,2,3)
        assert t3.construct(list(l)) == ((1,2),3)

    def test_deconstruct(self):
        t1 = Type.get(int,(int,int))
        t2 = Type.get(int,int,int)
        t3 = Type.get((int,int),int)

        p1 = (1,(2,3))
        p2 = (1,2,3)
        p3 = ((1,2),3)

        l = [1,2,3]

        assert t1.deconstruct(p1) == l
        assert t2.deconstruct(p2) == l
        assert t3.deconstruct(p3) == l


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

    def test_contains(self, base, base_inst, other_inst):
        t = Type.get([base])

        assert t.contains([])
        assert t.contains([base_inst])
        assert t.contains([base_inst, base_inst])
        assert not t.contains([other_inst])
        assert not t.contains([base_inst, other_inst])


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

    def test_composition(self, base, sub, other):
        t1 = ArrowType.get(other, sub)
        t2 = ArrowType.get(sub, base)
        t3 = ArrowType.get(other, base)

        assert t1 % t2 == t3

    def test_composition2(self, base):
        t1 = Type.get()
        t2 = ArrowType.get(t1, t1)
        t3 = ArrowType.get(base, base)

        t4 = t2 % t3
        assert t4 == t3

    def test_composition3(self, base, other):
        t1 = ArrowType.get(base, base)
        t2 = ArrowType.get(other, other)
        
        with pytest.raises(TypeError) as excinfo:
            t1 % t2
        assert "compose" in str(excinfo.value)

    def test_composition4(self, base, other):
        v1 = Type.get()
        v2 = Type.get()
        t1 = ArrowType.get(v1, (v1, base))
        t2 = ArrowType.get((other, v2), v2)

        t3 = t1 % t2
        assert t3 == ArrowType(other, base)

#    def test_composition_replacements(self, base):
#        t1 = Type.get()
#        t2 = ArrowType.get(t1, t1)
#        t3 = ArrowType.get(base, base)
#
#        repl = {}
#        t4 = t2.compose(t3, repl)
#        assert repl == { t1 : base }


class TestTypeVar(object):
    def test_uniqueness(self, base):
        t1 = Type.get()
        t2 = Type.get()

        assert t1 == t1
        assert t1 != t2
        assert t2 == t2
        assert t2 != t1

#    def test_isVariable(self, base):
#        t1 = Type.get()
#        t2 = Type.get(base)
#        l1 = Type.get([t1])
#        l2 = Type.get([t2])
#        p1 = Type.get((t1,))
#        p2 = Type.get((t2,))
#        p3 = Type.get((t1,t2))
#        p4 = Type.get((t2,t1))
#        a1 = ArrowType.get(t1,t1)
#        a2 = ArrowType.get(t2,t2)
#        a3 = ArrowType.get(t1,t2)
#        a4 = ArrowType.get(t2,t1)
#
#        assert t1.is_variable()
#        assert not t2.is_variable()
#        assert l1.is_variable()
#        assert not l2.is_variable()
#        assert p1.is_variable()
#        assert not p2.is_variable()
#        assert p3.is_variable()
#        assert p4.is_variable()
#
#    def test_isSatisfiedBy(self, base, other):
#        v1 = Type.get()
#        v2 = Type.get()
#        bl = Type.get([base])
#        vl = Type.get([v1])
#        bp = Type.get(base, base)
#        vp = Type.get(v1, v1)
#        vp2 = Type.get(v1, v2)
#        
#        
#        assert v1.is_satisfied_by(base)
#        assert v1.is_satisfied_by(v2)
#        assert v1.is_satisfied_by(bl)
#        assert v1.is_satisfied_by(vl)
#        assert v1.is_satisfied_by(bp)
#        assert v1.is_satisfied_by(vp)
#        assert v1.is_satisfied_by(vp2)
#        assert vp.is_satisfied_by(bp)
#        assert vp2.is_satisfied_by(bp)
#        assert vl.is_satisfied_by(bl)
#        assert not vl.is_satisfied_by(bp)
#        assert not vp.is_satisfied_by(bl)


class TestApplicationType(object):
    def test_application1(self, base):
        fun = ArrowType.get(base, base)
        app = fun(base)

        assert app == base

    def test_application2(self, base):
        var = Type.get()
        fun = ArrowType.get(var,var)
        app = fun(base)

        assert app == base

    def test_application3(self, base, other):
        var = Type.get()
        fun = ArrowType.get(var, (var, other))
        app = fun(base)

        assert app == (base, other)


class TestUnitType(object):
    def test_hasCorrectClass(self, unit):
        assert isinstance(unit, Type)    
        assert isinstance(unit, UnitType)    

    def test_containsNone(self, unit):
        assert unit.contains(tuple())

    def test_isIdForProduct(self, unit, base):
        t1 = unit * base
        t2 = unit * unit

        assert t1 == base
        assert t2 == unit 
    
class TestUnify(object):
    def test_unifyWithSame(self, unit, base):
        assert unit.unify(unit) == unit
        assert base.unify(base) == base 
    def test_noUnifyWithDifferent(self, unit, base):
        with pytest.raises(TypeError) as excinfo:
            unit.unify(base)
        assert "unify" in str(excinfo.value)
        with pytest.raises(TypeError) as excinfo:
            base.unify(unit)
        assert "unify" in str(excinfo.value)
    def test_unifyWithSubclass(self, base, sub):
        base.unify(sub) == sub
        sub.unify(base) == sub
    def test_unifyWithTypeVar(self, base):
        v = Type.get()
        assert base.unify(v) == base
        assert v.unify(base) == base
    def test_unifyProductTypes(self, base):
        v1 = Type.get()
        v2 = Type.get()
        assert Type.get(base, base).unify((v1, v2)) == (base, base)
    def test_unifyListType(self, base):
        v1 = Type.get()
        l1 = Type.get([base])
        l2 = Type.get([v1])
        assert l1.unify(l2) == [base]
        assert l2.unify(l1) == [base]
    def test_noUnifyIncompatibleProductTypes(self, base, other):
        v1 = Type.get()
        p1 = Type.get(v1,base)
        p2 = Type.get(other,v1)
        with pytest.raises(TypeError) as excinfo:
            p1.unify(p2)
        assert "unify" in str(excinfo.value)
        with pytest.raises(TypeError) as excinfo:
            p2.unify(p1)
        assert "unify" in str(excinfo.value)
    def test_unifyProductTypes2(self, base, sub):
        v1 = Type.get()
        p1 = Type.get(v1, base)
        p2 = Type.get(sub, v1)
        assert p1.unify(p2) == (sub, sub)
        assert p2.unify(p1) == (sub, sub)
