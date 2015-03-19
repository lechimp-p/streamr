# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from streamr import ListP, ListC, ConstP, pipe, transformation, filter_p

pr1 = ListP([1,2,3,4,5,6])
co1 = ListC()
sp = pr1 >> co1

print(sp.run())

pr2 = ConstP("Hello")
co2 = ListC(max_amount = 10)
sp = pr2 >> co2
print (sp.run())

def AppendWord(word):
    @transformation(str, str)
    def append(inp):
        return "%s %s" % (inp, word)
    return append

def chunks(type_io, length):
    @pipe(type_io, type_io)
    def chunks(await, send):
        send([await() for i in range(0, length)])
    return chunks

def echo(type_io, amount):
    @pipe(type_io, type_io)
    def echo(await, send):
        val = await()
        for i in range(0, amount):
            send(val)
    return echo

sp = pr2 >> AppendWord("you,") >> AppendWord("World!") >> co2
print (sp.run())

sp = pr1 >> chunks(int, 2) >> co1
print (sp.run())

sp = pr1 >> echo(int, 3) >> co1
print (sp.run())

sp = pr1 >> filter_p(int, lambda x: x % 2 == 0) >> co1
print (sp.run())
