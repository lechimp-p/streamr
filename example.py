# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from streamr import from_list, to_list, const, pipe, transformation, pass_if 

pr1 = from_list([1,2,3,4,5,6])
co1 = to_list()
sp = pr1 >> co1

print(sp.run())

pr2 = const("Hello")
co2 = to_list(max_amount = 10)
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
        send([await() for _ in range(0, length)])
    return chunks

def echo(type_io, amount):
    @pipe(type_io, type_io)
    def echo(await, send):
        val = await()
        for _ in range(0, amount):
            send(val)
    return echo

sp = pr2 >> AppendWord("you,") >> AppendWord("World!") >> co2
print (sp.run())

sp = pr1 >> chunks(int, 2) >> co1
print (sp.run())

sp = pr1 >> echo(int, 3) >> co1
print (sp.run())

sp = pr1 >> pass_if(int, lambda x: x % 2 == 0) >> co1
print (sp.run())
