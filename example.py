# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from streamr import *

pr = ListP(int, [1,2,3,4,5])
co = ListC(int)
sp = pr >> co

print(sp.run())

pr = RepeatP("Hello")
co = ListC(str, 10)
sp = pr >> co
print (sp.run())

def AppendWord(word):
    @statelessPipe(str, str)
    def append(inp):
        return "%s %s" % (inp, word)
    return append

sp = pr >> AppendWord("you,") >> AppendWord("World!") >> co
print (sp.run())
