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

TODO: 
* A python class could be understand as a constrained on a type in the
spirit of haskell type classes. Thus it could be usefull to allow to express
that a type should be a subclass of multiple other types.
* Something should forbid impossible functions like a -> b. One can't write a
function, that creates any possible b for any possible a.
"""

from functools import reduce
import re

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
    def __call__(self, other):
        return Type.engine.apply(self, other)

    def __hash__(self):
        """
        Hash based on id, since every type object should only be created once.
        """
        return id(self)

    def contains(self, value):
        """
        Check whether the given value is one of the possible values of the type.
        """
        return Type.engine.contains(self, value)

    @staticmethod
    def get(*py_types):
        """
        Factory method for types. Tries to turn a python value into a (possibly 
        nested) Type by using factories of the subclasses.

        Will create TypeVars, PyTypes, ListTypes or ProductTypes.

        Empty arguments will create a TypeVar. One list argument will create a
        ListType, where the items in the list will be used as a product type. 
        One non list argument will yield a PyType. Multiple arguments will create
        a product type.
        """
        if len(py_types) == 0:
            return TypeVar.get()

        if len(py_types) == 1:
            py_type = py_types[0]
            if isinstance(py_type, Type):
                return py_type
            if isinstance(py_type, list):
                return ListType.get(Type.get(*py_type))
            if isinstance(py_type, tuple):
                return ProductType.get(*py_type)

            return PyType.get(py_type)

        return ProductType.get(*py_types)
        

class PyType(Type):
    """
    Represents a python class.
    """
    def __init__(self, py_type):
        if not isinstance(py_type, type):
            raise ValueError("Expected an instance of pythons type class, "
                             "instead got %s." % type(py_type))

        self.py_type = py_type

    cache = {}

    @staticmethod
    def get(py_type):
        if py_type not in PyType.cache:
            PyType.cache[py_type] = PyType(py_type)

        return PyType.cache[py_type]

    def __str__(self):
        s = str(self.py_type)
        return re.match(".*[']([^']+)[']", s).group(1)

class ProductType(Type):
    """
    Represents a product type, that is a tuple of types.
    """
    def __init__(self, *types):
        for t in types:
            if not isinstance(t, Type):
                raise ValueError("Expected list of Type instances as argument.")

        self.types = types

    cache = {}

    @staticmethod
    def get(*types):
        if len(types) == 1:
            return Type.get(types[0])

        def flatten_product_types(types):
            res = []
            for t in types:
                if isinstance(t, ProductType):
                    res += flatten_product_types(t.types)
                else:
                    res.append(Type.get(t))
            return res

        types = tuple(flatten_product_types(types))

        if types not in ProductType.cache:
            ProductType.cache[types] = ProductType(*types)

        return ProductType.cache[types]

    def __str__(self):
        return "(%s)" % reduce(lambda a,b : "%s, %s" % (a,b), self.types)

class ListType(Type):
    """
    Represents a list type with items of one fixed other type.
    """
    def __init__(self, item_type):
        if not isinstance(item_type, Type):
            raise ValueError("Expected instance of Type as argument.")

        self.item_type = item_type

    cache = {}
    
    @staticmethod
    def get(*item_type):
        item_type = Type.get(*item_type)

        if item_type not in ListType.cache:
            ListType.cache[item_type] = ListType(item_type)

        return ListType.cache[item_type]

    def __str__(self):
        return "[%s]" % self.item_type

class ArrowType(Type):
    """
    Represents the type of a transformation from one type to another.
    """
    def __init__(self, l_type, r_type):
        if not isinstance(l_type, Type) or not isinstance(r_type, Type):
            raise ValueError("Expected instances of Type as arguments.")

        # TODO: Something should forbid impossible functions like a -> b.
        # One can't produce an arbitrary b for an arbitrary a
        # Maybe the correct location is in replace...

        self.l_type = l_type
        self.r_type = r_type

    cache = {}

    @staticmethod
    def get(l_type, r_type):
        l_type = Type.get(l_type)
        r_type = Type.get(r_type)

        print((l_type, r_type))

        if (l_type, r_type) not in ArrowType.cache:
            print("is cached")
            ArrowType.cache[(l_type, r_type)] = ArrowType(l_type, r_type)

        print("is not cached")
        return ArrowType.cache[(l_type, r_type)]

    def compose_with(self, other):
        """
        Get the type of the composition of this arrow type with another arrow type.
        """
        if not isinstance(other, ArrowType):
            raise TypeError("Expected arrow type, not '%s'" % other)

        return ArrowType.get(self.l_type, other(self.r_type))

    def __str__(self):
        return "(%s -> %s)" % (self.l_type, self.r_type)

class TypeVar(Type):
    """
    Represents a type that has yet to be inferred.
    """
    def __init__(self):
        pass

    @staticmethod
    def get():
        return TypeVar()

    def __str__(self):
        return "#%s" % (str(id(self))[-3:-1])

class TypeEngine(object):
    """
    Engine that does type checking and inference.
    """
    def lt(self, l, r):
        l,r = self._toType(l,r)
        return self._withComparisons(l, r, {
              PyType :      lambda l, r: 
                l.py_type != r.py_type and issubclass(r.py_type, l.py_type)
            , ProductType : lambda l, r: 
                len(l.types) == len(r.types) 
                and ALL((v[0] < v[1] for v in zip(l.types, r.types)))
            , ListType :    lambda l, r:
                l.item_type < r.item_type
            , ArrowType :   lambda l, r:
                l.l_type > r.l_type or l.r_type < r.r_type
                
        })
    def le(self, l, r):
        l,r = self._toType(l,r)
        return self._withComparisons(l, r, {
              ProductType: lambda l, r:
                len(l.types) == len(r.types)
                and ALL((v[0] <= v[1] for v in zip(l.types, r.types)))
        }, lambda l, r: l == r or l < r)
    def eq(self, l, r):
        l,r = self._toType(l,r)
        return id(l) == id(r)
    def ne(self, l, r):
        l,r = self._toType(l,r)
        return id(l) != id(r)
    def ge(self, l, r):
        return self.le(r, l)
    def gt(self, l, r):
        return self.lt(r, l)

    def _toType(self, l, r):
        return Type.get(l), Type.get(r)

    def _withComparisons(self, l, r, comparisons, default = None):
        tl, tr = type(l), type(r)
        if tl != tr:
            return False

        for key, value in comparisons.items():
            if key == tl:
                return value(l, r)
        
        if default:
            return default(l, r)

        return False

    def apply(self, l, r):
        l,r = self._toType(l,r)
        if not isinstance(l, ArrowType):
            raise ValueError("Can't call non arrow type.")

        if isinstance(l.l_type, TypeVar):
            return self.replace(l.r_type, l.l_type, r)

        if l.l_type > r:
            raise ValueError("Can't use %s as %s." % (r, l.l_type))

        return l.r_type

    def replace(self, where, what, wit):
        t = type(where)

        if where == what:
            return wit

        repl = lambda x: self.replace(x, what, wit)
        if t == ProductType:
            return ProductType.get(*(repl(x) for x in where.types))
        if t == ListType:
            return ListType.get(repl(where.item_type))
        if t == ArrowType:
            return ArrowType.get(repl(where.l_type), repl(where.r_type))

        return where

    def contains(self, _type, value):
        t = type(_type)
        
        if t == PyType:
            return isinstance(value, _type.py_type)
        if t == ProductType:
            if type(value) != tuple:
                return False
            if len(value) != len(_type.types):
                return False
            return ALL((v[0].contains(v[1]) for v in zip(_type.types, value))) 
        if t == ListType:
            return ALL((_type.item_type.contains(v) for v in value))

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


