from streamr import Pipe, MayResume, from_list, to_list
from streamr.types import Type

class Echo(Pipe):
    """
    A pipe that echos values it retreives from upstream for a certain times,
    given as init.
    """
    def __init__(self):
        # We need to pass an init type, an upstream type and a downstream type
        _any = Type.get()
        super(Echo, self).__init__(int, _any, _any)

    def setup(self, params, result):
        return params[0]

    def transform(self, env, await, send):
        val = await()
        for _ in range(0, env):
            send(val)
        return MayResume

echo = lambda: Echo()

sp = from_list([1,2]) >> echo() >> to_list() 
assert sp.run(2) == [1,1,2,2]
