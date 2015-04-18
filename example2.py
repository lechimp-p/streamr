from streamr import Consumer, MayResume, from_list
from streamr.types import Type

class Prints(Consumer):
    """
    A consumer that prints all values it retreives from upstream and returns
    the total amount of values it has seen as a result.
    The value will be inserted in a string given as init parameter.
    """
    def __init__(self):
        # As with the producer, we need to specify the types of the processor.
        # As we do not want to restrict on the upstream type, we use a type
        # variable:
        _any = Type.get()
        # init type, result tyoe and consumed type.
        super(Prints, self).__init__(str, int, _any)

    def setup(self, params, result):
        # We did not see any values recently.
        result(0)
        # We need to capture the string and a mutable amount in our environment.
        return (params[0], [0])
    
    # This method will be called when we should consume a new value from 
    # upstream.
    def consume(self, env, await, result):
        val = await()
        # We saw a new value...
        env[1][0] += 1
        result(env[1][0])
        print(env[0] % val)
        return MayResume

# We bind a lambda here, as we need different prints for different
# input types.
prints = lambda: Prints()

sp = from_list(range(0,10)) >> prints()
assert sp.run("Got value: %s") == 10
