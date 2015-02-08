# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from streamr import *

pr1 = ListP(int, [1,2,3,4,5,6])
co1 = ListC(int)
sp = pr1 >> co1

print(sp.run())

pr2 = RepeatP("Hello")
co2 = ListC(str, 10)
sp = pr2 >> co2
print (sp.run())

def AppendWord(word):
    @transformation(str, str)
    def append(inp):
        return "%s %s" % (inp, word)
    return append

sp = pr2 >> AppendWord("you,") >> AppendWord("World!") >> co2
print (sp.run())

sp = pr1 >> chunks(int, 2) >> co1
print (sp.run())

sp = pr1 >> echo(int, 3) >> co1
print (sp.run())
