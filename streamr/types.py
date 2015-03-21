# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

"""
# Type checking

To maintain type safety when writing streaming pipelines the parts of the 
pipelines are typed. On construction of a pipeline it should be possible to 
check, whether two parts of a pipeline fit together.

It is not enough to use the standard isinstance or type functions of python, as
we like to express things like sum or product types or type variables as well.

The different possible types are implemented as subclasses from the type base
class, where the objects are only used as value objects. The processing is done
by an engine, thus the hierarchy of Type is to be considered closed when the
standard engine is used.

Types could be used with the python comparison operators. The meaning of the
comparison operations is a little different from their standard meaning on 
integers e.g..

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
        """
        Type of the application of this type to the other type.
        """
        return Type.engine.apply(self, other)
    def __mod__(self, other):
        """
        Type of composition of this type and the other type.
        """
        return Type.engine.compose(self, other)
    def __mul__(self, other):
        """
        Create a product type from two other types.
        """
        return Type.get(self, other)
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
    def unify(self, other):
        """
        Try to unify this type with the other, that is try to find a smaller type
        that only contains values, that are contained in this and the other type.

        Returns unified type, raises TypeError when unification can't be achieved.
        """
        return Type.engine.unify(self, other)
    def substitute_vars(self, substitutions):
        """
        Replace type variables given in substitutions in this type.
        """
        return Type.engine.substitute_in(self, substitutions)

    @staticmethod
    def infer(value):
        """
        Infer the type of a value.
        """
        return Type.engine.infer(value)

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

            if py_type == ():
                return UnitType.get()

            return PyType.get(py_type)

        return ProductType.get(*py_types)

    def is_variable(self):
        return NotImplementedError()

class UnitType(Type):
    """
    Represents the unit type, that is the type that contains a single value.

    The value is identified with the empty tuple (), which makes it easy to use
    the unit type with a *style parameter list.
    """
    def __init__(self):
        pass

    instance = None

    @staticmethod
    def get():
        if UnitType.instance is None:
            UnitType.instance = UnitType()

        return UnitType.instance

    def __str__(self):
        return "()"

    def is_variable(self):
        return False

unit = UnitType.get()

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

        if (l_type, r_type) not in ArrowType.cache:
            ArrowType.cache[(l_type, r_type)] = ArrowType(l_type, r_type)

        return ArrowType.cache[(l_type, r_type)]

    def type_in(self):
        return self.l_type
    def type_out(self):
        return self.r_type

    def __str__(self):
        return "(%s -> %s)" % (self.l_type, self.r_type)

    def is_variable(self):
        return self.l_type.is_variable() or self.r_type.is_variable() 

class TypeVar(Type):
    """
    Represents a type that has yet to be inferred.
    """
    @staticmethod
    def get():
        return TypeVar()

    def __str__(self):
        return "#%s" % (str(id(self))[-3:-1])

    def is_variable(self):
        return True 

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
        types = tuple(map(Type.get,
                        filter(lambda x : x != unit, 
                            types)))

        if len(types) == 0:
            return UnitType.get()

        if len(types) == 1:
            return Type.get(types[0])

        if types not in ProductType.cache:
            ProductType.cache[types] = ProductType(*types)

        return ProductType.cache[types]

    def __str__(self):
        return "(%s)" % reduce(lambda a,b : "%s, %s" % (a,b), self.types)

    def is_variable(self):
        return ANY(map(lambda x: x.is_variable(), self.types))

    def construct(self, l):
        """
        Construct a tuple according to product by using values from l.
        """
        vals = []
        for t in self.types:
            if isinstance(t, UnitType):
                continue

            if isinstance(t, ProductType):
                vals.append(t.construct(l))
                continue

            val = l.pop(0)
            if not t.contains(val):
                raise TypeError("Expected value of type '%s', got '%s'" % (t,val))
            vals.append(val)
        return tuple(vals)

    def deconstruct(self, p):
        """
        Deconstruct the tuple p into a list.
        """
        if not isinstance(p, tuple):
            return [p]

        ret = []
        for e in p:
            ret += self.deconstruct(e)
        return ret
            
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

    def is_variable(self):
        return self.item_type.is_variable()

class PyType(Type):
    """
    Represents a python class.
    """
    def __init__(self, py_type):
        self.py_type = py_type

    cache = {}

    @staticmethod
    def get(py_type):
        if not isinstance(py_type, type):
            raise ValueError("Expected an instance of pythons type class, "
                             "instead got %s." % type(py_type))

        if py_type not in PyType.cache:
            PyType.cache[py_type] = PyType(py_type)

        return PyType.cache[py_type]

    def __str__(self):
        s = str(self.py_type)
        return re.match(".*[']([^']+)[']", s).group(1)

    def is_variable(self):
        return False

def sequence(arrows, with_substitutions = False):
    return Type.engine.sequence(arrows, with_substitutions)

class TypeEngine(object):
    """
    Engine that does type checking.
    """
    def lt(self, l, r):
        l,r = self._toType(l,r)
        return self._withComparisons(l, r,
            { PyType :      lambda l, r:
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
        return self._withComparisons(l, r, 
            { ProductType: lambda l, r:
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

    def contains(self, _type, value):
        t = type(_type)
        
        if t == UnitType:
            return value == () 
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

    unified_type_cache = {}

    def unify(self, l, r, with_substitutions = False):
        l,r = self._toType(l,r)

        if (l,r) in self.unified_type_cache:
            if with_substitutions:
                return self.unified_type_cache[(l,r)]
            return self.unified_type_cache[(l,r)][0]

        substitutions = {}

        unified = self._unifies(substitutions, l, r)
        substituted = self._do_substitutions(substitutions, unified)
        self.unified_type_cache[(l,r)] = (substituted, substitutions)
        return self.unify(l, r, with_substitutions)

    applied_type_cache = {}

    def apply(self, l, r, with_substitutions = False):
        l,r = self._toType(l,r)

        if not isinstance(l, ArrowType):
            raise TypeError("Can't apply a none arrow type.")

        if (l, r) in self.applied_type_cache:
            if with_substitutions:
                return self.applied_type_cache[(l,r)]
            return self.applied_type_cache[(l,r)][0]

        _, substitutions = self.unify(l.l_type, r, True) 
        r_t = self._do_substitutions(substitutions, l.r_type) 
        self.applied_type_cache[(l,r)] = (r_t, substitutions)
        return self.apply(l, r, with_substitutions)
    
    def compose(self, l, r, with_substitutions = False):
        return self.sequence([l,r], with_substitutions)

    composed_type_cache = {}

    def sequence(self, arrows, with_substitutions = False):
        arrows = list(arrows)
        for i in range(0, len(arrows)):
            arrows[i] = Type.get(arrows[i])
            if not isinstance(arrows[i], ArrowType):
                raise TypeError("Can't compose none arrow types.")

        if tuple(arrows) in self.composed_type_cache:
            if with_substitutions:
                return self.composed_type_cache[tuple(arrows)]
            return self.composed_type_cache[tuple(arrows)][0]

        substitutions = {}
        r_type = arrows[0].r_type
        for a in arrows[1:]:
            self._unifies(substitutions, r_type, a.l_type)
            r_type = a.r_type

        l_t = self._do_substitutions(substitutions, arrows[0].l_type)
        r_t = self._do_substitutions(substitutions, arrows[-1].r_type)
        arr = ArrowType.get(l_t, r_t)

        self.composed_type_cache[tuple(arrows)] = (arr, substitutions)
        return self.sequence(arrows, with_substitutions)

    def substitute_in(self, _type, substitutions):
        return self._do_substitutions(substitutions, _type)

    def infer(self, value):
        if isinstance(value, tuple):
            return Type.get(*[self.infer(v) for v in value])
        if isinstance(value, list):
            if len(value) == 0:
                raise ValueError("Can't infer type of empty list.")
            h = value[0]
            ht = self.infer(h)
            for v in value:
                if not ht.contains(v):
                    raise ValueError("Can't infer type of mixed type list.")
            return Type.get([ht])
        return Type.get(type(value))
            

    @staticmethod
    def _cant_unify(l,r):
        raise TypeError("Can't unify '%s' and '%s'" % (l,r))

    @staticmethod
    def _substitute(subs, v, t):
        if v in subs:
            o = subs[v]
            if isinstance(o, TypeVar):
                subs[o] = t 
                subs[v] = t
            elif o == t:
                pass
            elif t >= o:
                subs[v] = t
            elif not o >= t:
                TypeEngine._cant_unify(o,t) 
            return v

        subs[v] = t
        return v

    @staticmethod
    def _unifies(subs, l,r):
        if l == r:
            return l

        if l >= r:
            return l
        if r >= l:
            return r

        if isinstance(l, TypeVar):
            return TypeEngine._substitute(subs, l, r)
        if isinstance(r, TypeVar):
            return TypeEngine._substitute(subs, r, l)

        if type(l) != type(r):
            TypeEngine._cant_unify(l,r)

        if type(l) == ListType:
            t = TypeEngine._unifies(subs, l.item_type, r.item_type)
            return ListType.get(t)

        if type(l) == ProductType:
            if len(l.types) != len(r.types):
                TypeEngine._cant_unify(l,r)
            m = lambda x: TypeEngine._unifies(subs, *x)
            z = zip(l.types, r.types)
            return ProductType.get(*list(map(m,z)))

        TypeEngine._cant_unify(l,r)

    @staticmethod
    def _do_substitutions(subs, t):
        if t in subs:
            return TypeEngine._do_substitutions(subs, subs[t])

        if type(t) == ListType:
            return ListType.get(TypeEngine._do_substitutions(subs, t.item_type))

        if type(t) == ProductType:
            m = lambda x: TypeEngine._do_substitutions(subs, x)
            return ProductType.get(*list(map(m, t.types)))
        
        return t

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


