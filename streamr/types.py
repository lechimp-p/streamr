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

from functools import reduce

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
    def get(py_type):
        if isinstance(py_type, Type):
            return py_type
        return PyType(py_type)


class ProductType(Type):
    """
    Represents a product type, that is a tuple of types.
    """
    def __init__(self, *types):
        self.types = types

    @staticmethod
    def get(*types):
        if len(types) == 1:
            return PyType.get(types[0])

        def flatten_product_types(types):
            res = []
            for t in types:
                if isinstance(t, ProductType):
                    res += flatten_product_types(t.types)
                else:
                    res.append(t)
            return res

        types = flatten_product_types(types)

        return ProductType(*types)

class ListType(Type):
    """
    Represents a list type with items of one fixed other type.
    """
    def __init__(self, item_type):
        self.item_type = item_type
    
    @staticmethod
    def get(item_type):
        return ListType(PyType.get(item_type))

class TypeEngine(object):
    """
    Engine that does type checking and inference.
    """
    def lt(self, l, r):
        return self.withComparisons(l, r, {
              PyType :      lambda l, r: 
                l.py_type != r.py_type and issubclass(r.py_type, l.py_type)
            , ProductType : lambda l, r: 
                len(l.types) == len(r.types) 
                and ALL((v[0] < v[1] for v in zip(l.types, r.types)))
            , ListType :    lambda l, r:
                l.item_type < r.item_type
        })
    def le(self, l, r):
        return self.withComparisons(l, r, {
              PyType :     lambda l, r: 
                issubclass(r.py_type, l.py_type)
            , ProductType: lambda l, r:
                len(l.types) == len(r.types)
                and ALL((v[0] <= v[1] for v in zip(l.types, r.types)))
            , ListType :    lambda l, r:
                l.item_type <= r.item_type
        })
    def eq(self, l, r):
        return self.withComparisons(l, r, {
              PyType :     lambda l, r:
                l.py_type == r.py_type
            , ProductType: lambda l, r:
                len(l.types) == len(r.types)
                and ALL((v[0] == v[1] for v in zip(l.types, r.types)))
            , ListType :    lambda l, r:
                l.item_type == r.item_type
        })
    def ne(self, l, r):
        return self.withComparisons(l, r, {
              PyType :     lambda l, r:
                l.py_type != r.py_type
            , ProductType: lambda l, r:
                len(l.types) != len(r.types)
                or ANY((v[0] != v[1] for v in zip(l.types, r.types)))
            , ListType :    lambda l, r:
                l.item_type != r.item_type
        })
    def ge(self, l, r):
        return self.withComparisons(l, r, {
              PyType :     lambda l, r:
                issubclass(l.py_type, r.py_type)
            , ProductType: lambda l, r:
                len(l.types) == len(r.types)
                and ALL((v[0] >= v[1] for v in zip(l.types, r.types)))
            , ListType :    lambda l, r:
                l.item_type >= r.item_type
        })
    def gt(self, l, r):
        return self.withComparisons(l, r, {
              PyType :     lambda l, r:
                l.py_type != r.py_type and issubclass(l.py_type, r.py_type)
            , ProductType: lambda l, r:
                len(l.types) == len(r.types)
                and ALL((v[0] > v[1] for v in zip(l.types, r.types)))
            , ListType :    lambda l, r:
                l.item_type > r.item_type
        })

    def withComparisons(self, l, r, comparisons):
        if not isinstance(l, Type):
            return self.withComparisons(PyType.get(l), r, comparisons)
        if not isinstance(r, Type):
            return self.withComparisons(l, PyType.get(r), comparisons)

        tl, tr = type(l), type(r)
        if tl != tr:
            return False

        for key, value in comparisons.items():
            if key == tl:
                return value(l, r)
        
        return False

Type.engine = TypeEngine()


# Helpers

def ALL(l):
    """
    Returns True if every element in l is True.
    """
    for v in l:
        if not v:
            return False
    return True

def ANY(l):
    """
    Returns True if at least one element in is True.
    """
    for v in l:
        if v:
            return True
    return False


