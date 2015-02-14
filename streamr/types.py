# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

"""
# Type checking and inference

To maintain type safety when writing streaming pipelines the parts of the 
pipelines are typed. On construction of a pipeline it should be possible to 
check, whether two parts of a pipeline fit together.

It is not enough to use the standard isinstance or type functions of python, as
we like to express things like sum or product types or type variables as well.

The different possible types are implemented as subclasses from the type base
class, where the objects are only used as value objects. The processing is done
by and engine, thus the hierarchy of Type is to be considered closed when the
standard engine is used.

Types could be used with the python comparison operators. The meaning of the
comparison operations is a little different from their standard meaning on 
integers e.g..

Equality (==) is to be considered as "equality up to isomorphism" (may computer
science people forgive my loose terminology). That means to (a == b) == True for 
two types a and b, when there exists a transformation from values of type a to
values of type b. Think of tuples like (1,(2,3)) and ((1,2),3).

The lower than and greather than operators are used to express subclassing,
where the direction of the operator shows the direction in which a casting is
possible, thus subclass >= class.
"""

class Type(object):
    """
    Base class representing a type.
    """
    def __lt__(self, other):
        return Type.engine.lt(self, other)
    def __le__(self, other):
        return Type.engine.le(self, other)
    def __eq__(self, other):
        return Type.engine.eq(self, other)
    def __ne__(self, other):
        return Type.engine.ne(self, other)
    def __ge__(self, other):
        return Type.engine.ge(self, other)
    def __gt__(self, other):
        return Type.engine.gt(self, other)
     

class PyType(Type):
    """
    Represents a python class.
    """
    def __init__(self, py_type):
        self.py_type = py_type

    @staticmethod
    def create(py_type):
        return PyType(py_type)

class ProductType(Type):
    """
    Represents a product type, that is a tuple of types.
    """
    def __init__(self, types):
        self.types = types

class TypeEngine(object):
    """
    Engine that does type checking and inference.
    """
    def lt(self, l, r):
        res = self.compare(l, r)
        return False if res is None else res == -1
    def le(self, l, r):
        res = self.compare(l, r)
        return False if res is None else res <= 0 
    def eq(self, l, r):
        res = self.compare(l, r)
        return False if res is None else res == 0
    def ne(self, l, r):
        res = self.compare(l, r)
        return True if res is None else res != 0
    def ge(self, l, r):
        res = self.compare(l, r)
        return False if res is None else res >= 0 
    def gt(self, l, r):
        res = self.compare(l, r)
        return False if res is None else res == 1

    def compare(self, l, r):
        """
        Returns 0 on equality -1 on l < r and 1 on l > r. If the types have
        nothing to do which each other, None is returned.
        """
        if id(l) == id(r):
            return 0

        if not isinstance(l, Type):
            return self.compare(PyType.create(l), r)
        if not isinstance(r, Type):
            return self.compare(l, PyType.create(r))

        tys = (type(l), type(r))
        if tys == (PyType, PyType):
            return self.comparePyTypes(l, r)
        return None
        
    def comparePyTypes(self, l, r):
        """
        Helper for compare.
        """
        lty = l.py_type
        rty = r.py_type

        if lty == rty:
            return 0

        if issubclass(lty, rty):
            return 1

        if issubclass(rty, lty):
            return -1
             
        return None
 
Type.engine = TypeEngine()
