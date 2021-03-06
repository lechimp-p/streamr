import sys

if sys.version_info[0] != 3:
    raise RuntimeError("This only works in python3, since in python2 "
                       "unicode-strings and ordinary strings are distinct.")

from streamr.io import read_file, to_json
from streamr.string import search, replace
from streamr import tee, nop, transformation, maps
from streamr.types import Type

import json


# Some custom transformation that looks up values in 
# a dictionary recursively with dot notation and returns
# a list of tuples of the keys and the values.

@transformation((dict, [(str)]), [(str,str)])
def dict_values(inp):
    d, keys = inp
    res = []

    def get(ks, value):
        if len(ks) == 0:
            return value
        return get(ks, value[ks.pop(0)])

    for k in keys:
        res.append((k, get(k.split("."), d)))

    return res


# We run some tests on our way...

from streamr import from_list, to_list

lp = from_list([ ({ "a" : "b", "c" : { "d" : "e" } }, ["a", "c.d"] ) ])
lc = to_list()

sp = lp >> dict_values >> lc

res = sp.run()[0]
print(res)
assert ("a", "b") in res
assert ("c.d", "e") in res


# This is how we get the values we need to replace: we take a string,
# turn it to json, search for the placeholders and join this with
# our custom dict_values transformation.

get_values = tee() >> to_json * search("{{([^}]+)}}") >> dict_values

assert get_values.type_in() == Type.get(str)
assert get_values.type_out() == Type.get([(str, str)])

example = { "a" : "b"
          , "b" : "{{a}}"
          , "c" : { "d" : "e" }
          , "e" : "{{c.d}}" 
          }

lp = from_list([json.dumps(example)])
sp = lp >> get_values >> lc

res = sp.run()[0]
print(res)
assert ("a", "b") in res
assert ("c.d", "e") in res


# We need to expand the keys a to {{a}} to get a map of replacements.

@transformation((str, str), (str, str))
def to_replacement(i):
    return "{{%s}}" % i[0], i[1]

get_replacements = get_values >> maps(to_replacement)

sp = lp >> get_replacements >> lc

res = sp.run()[0]
print(res)
assert ("{{a}}", "b") in res
assert ("{{c.d}}", "e") in res


# And finally the json string replacer. We do not read from a file with it.
# We need a nop to get the types right.

json_string_replacer = tee() >> nop() * get_replacements >> replace >> to_json

sp = lp >> json_string_replacer >> lc

res = sp.run()[0]
print(res)
assert res["a"] == "b"
assert res["b"] == "b"
assert res["c"] == { "d" : "e" }
assert res["e"] == "e"


# Finally, we map a filename to a fully parsed and replace json

json_string_replacer_f = read_file >> json_string_replacer
