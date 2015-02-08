# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from streamr import *

pr = ListP(int, [1,2,3,4,5])
co = ListC(int)
sp = pr >> co

print(sp.run())
