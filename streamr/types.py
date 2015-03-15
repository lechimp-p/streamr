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

#    def get_variables(self):
#        """
#        Get all type variables in this type.
#        """
#        return Type.engine.get_variables(self)
#
#    def is_satisfied_by(self, other):
#        """
#        Check whether this type is satisfied by another type by
#        substituting type vars in self with appropriate types to
#        get a type that matches other.
#        """
#        return Type.engine.is_satisfied_by(self, other)

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

class TypeVar(Type):
    """
    Represents a type that has yet to be inferred.
    """
    @staticmethod
    def get():
        return TypeVar()

    def __str__(self):
        return "#%s" % (str(id(self))[-3:-1])

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

def sequence(arrows):
    cur = arrows[0] 
    
    for arr in arrows[1:]:
        cur = cur % arr

    return cur

class TypeEngine(object):
    """
    Engine that does type checking.
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

    def _lt_on_type_var(self, l, r):
        for c in l.constraints:
            # For every constraint in l there needs to be a sufficient
            # constraint in r that guarantees, that the desired class
            # is a superclass of the class required by l.
            has_super = False
            for cr in r.constraints:
                if issubclass(cr, c) and not issubclass(c,cr):
                    has_super = True
                    break
            
            if not has_super:
                return False

        return True
          
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

    type_var_cache = { unit : [] }

    def get_type_vars(self, _type):
        if _type not in type_var_cache:
            t = type(_type)

            if t == ArrowType:
                vs = ( self.get_type_vars(_type.l_type) 
                     + self.get_type_vars(_type.r_type))
            if t == TypeVar:
                vs = [_type]
            if t == ProductType:
                vs = []
                for ty in _type.types:
                    vs = vs + self.get_type_vars(ty)
            if t == ListType:
                vs = self.get_type_vars(t.item_type)

            self.type_var_cache[_type] = vs

        return type_var_cache[_type]

    unified_type_cache = {}

    def unify(self, l, r):
        if (l,r) in self.unified_type_cache:
            return self.unified_type_cache[(l,t)]

        constraints = {} 
        substitutions = {}

        def cant_unify(l,r):
            raise TypeError("Can't unify '%s' and '%s'" % (l,r))

        def substitute(v, t):
            substitutions[v] = t
            return v

        def constraint(v,r):
            v = Type.get()
            for c in l.constraints + r.constraints:
                v = v.constrain(c)


            constraints.append(v)
            return v

        def unifies(l,r):
            if l == r:
                return l

            if isinstance(l, TypeVar):
                if isinstance(r, TypeVar):
                    return constraint(l,r)
                if len(l.constraints) == 0:
                    return substitute(l, r)
                cant_unify(l,r)
            if isinstance(r, TypeVar):
                if len(r.constraints) == 0:
                    return substitute(r, l)
                cant_unify(l,r)

            if type(l) != type(r):
                cant_unify(l,r)

            if type(l) == ListType:
                t = unifies(l.item_type, r.item_type)
                return ListType.get(t)

            if type(l) == ProductType:
                if len(l.types) != len(r.types):
                    cant_unify(l,r)
                return ProductType.get(*list(map(lambda x: unifies(*x), zip(l.types, r.types))))

            cant_unify(l,r)

        def do_substitutions(t):
            if t in substitutions:
                return substitutions[t]

            if type(t) == ListType:
                return ListType.get(do_substitutions(t.item_type))

            if type(t) == ProductType:
                return ProductType.get(*map(do_substitutions, t.types))
            
            return t

        self.unified_type_cache[(l,r)] = do_substitutions(unifies(l,r))
        return self.unified_type_cache[(l,r)]

    def apply(self, l, r, replacements = None):
        if not isinstance(l, ArrowType):
            raise TypeError("Can't apply a none arrow type.")

        return None


#        l,r = self._toType(l,r)
#        if not isinstance(l, ArrowType):
#            raise ValueError("Can't call non arrow type.")
#
#        if not self.is_satisfied_by(l.l_type, r, replacements):
#            raise TypeError("Can't apply '%s' to '%s'" % (l,r))
#
#        return self.replaceMany(l.r_type, replacements)
    
    def compose(self, l, r, replacements = None):
        if not isinstance(l, ArrowType) or not isinstance(r, ArrowType):
            raise TypeError("Can't compose none arrow types.")

        return None
#
#        if replacements is None:
#            replacements = {}
#
#        if not l.r_type.is_variable() and l.r_type != r.l_type:
#            raise TypeError("Can't compose '%s' and '%s'" % (l, r))
#
#        if l.r_type.is_variable():
#            if not self.is_satisfied_by(l.r_type, r.l_type, replacements):
#                raise TypeError("Can't compose '%s' and '%s'" % (l, r))
#            l_type = self.replaceMany(l.l_type, replacements)
#            r_type = r.r_type
#        else:
#            l_type = l.l_type
#            r_type = r.r_type
#
#        return ArrowType.get(l_type, r_type)



#    def replace(self, where, what, wit):
#        return self.replaceMany(where, { what : wit })
#
#    def replaceMany(self, where, replacements):
#        t = type(where)
#
#        if where in replacements:
#            replmnt = replacements[where]
#            while isinstance(replmnt, TypeVar) and replmnt in replacements:
#                replmnt = replacements[where]
#            return replmnt
#
#        repl = lambda x: self.replaceMany(x, replacements)
#        if t == ProductType:
#            return ProductType.get(*(repl(x) for x in where.types))
#        if t == ListType:
#            return ListType.get(repl(where.item_type))
#        if t == ArrowType:
#            return ArrowType.get(repl(where.l_type), repl(where.r_type))
#
#        return where
#
#
#    is_variable_cache = {}
#
#    def is_variable(self, _type):
#        t = type(_type)
#
#        if _type not in TypeEngine.is_variable_cache:
#            if t == TypeVar:
#                is_variable = True
#            elif t == ListType:
#                is_variable = self.is_variable(_type.item_type)
#            elif t == ProductType:
#                is_variable = ANY(self.is_variable(i) for i in _type.types)
#            elif t == ArrowType:
#                is_variable = ( self.is_variable(t.l_type) 
#                                or self.is_variable(t.r_type) )
#            else:
#                is_variable = False 
#
#            TypeEngine.is_variable_cache[_type] = is_variable
#
#        return TypeEngine.is_variable_cache[_type]
#
#    def get_variables(self, _type, vlist = None):
#        if vlist is None:
#            vlist = []
#
#        t = type(_type)
#
#        if t == TypeVar:
#            is_variable = True
#        elif t == PyType:
#            is_variable = False
#        elif t == ListType:
#            is_variable = self.is_variable(_type.item_type)
#        elif t == ProductType:
#            is_variable = ANY(self.is_variable(i) for i in _type.types)
#        elif t == ArrowType:
#            is_variable = ( self.is_variable(t.l_type) 
#                            or self.is_variable(t.r_type) )
#        else:
#            raise TypeError("Can't tell if '%s' is variable" % _type)
#
#        TypeEngine.is_variable_cache[_type] = is_variable
#
#        return TypeEngine.is_variable_cache[_type]
#
#
#
#    def is_satisfied_by(self, _type, other, replacements = None, free = []):
#        # Holds the replacements for type variables
#        if replacements is None:
#            replacements = {}
#
#        def get_replacement(v):
#            if not v in replacements:
#                return None
#            r = replacements[v]
#            if isinstance(r, TypeVar):
#                return get_replacement(r)
#            return r
#
#        def check_replacement(v, l):
#            repl = get_replacement(v)
#            if repl is not None:
#                return repl <= l
#            replacements[v] = l
#            return True
#
#        def go(l,r):
#            if not l.is_variable() and l != r:
#                return False
#
#            if l == r:
#                return True
#
#            tl = type(l)
#            tr = type(r)
#
#            if tl == TypeVar:
#                return check_replacement(l,r) 
#
#            if tl != tr:
#                return False
#
#            if tl == ListType:
#                return go(l.item_type, r.item_type)
#            if tl == PyType:
#                return issubclass(r.py_type, l.py_type)
#            if tl == ProductType:
#                return ALL(go(v[0],v[1]) for v in zip(l.types, r.types))
#            if tl == ArrowType:
#                return go(l.l_type, r.l_type) and go(l.r_type, r.r_type)
#            
#            raise TypeError("Can't check weather '%s' is satisfied." % tl) 
#        
#        return go(_type, other)

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


