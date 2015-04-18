from streamr import Producer, Stop, MayResume, to_list
import sys

class AllIntsLargerThan(Producer):
    """
    This is a producer that produces all ints starting at an int supplied
    as init parameter. 
    """
    def __init__(self):
        # Specify the init type and the type of the produced values.
        super(AllIntsLargerThan, self).__init__(int, int)

    # A setup method can be implemented for every stream processor. It is
    # called when the processor starts to execute. The return value of the
    # setup will be passed as env to every call to the produce method.
    # Since a stream processor should be composable it should capture changes
    # during processing in an extra environment and not in the state of the
    # object. 
    def setup(self, params, result):
        # * params is a value of init type
        # * result is a function that could be used to set a result for the
        #   processor. As our Producer has no result, we should not use it.

        # Call overwritten setup function to do type checking on the params.
        super(AllIntsLargerThan, self).setup(params, result)
        
        # As our init type is int and we want to produce all integers larger
        # than our init, we use the initial value as mutable environment. 
        return [params[0]]

    # The produce method will be called, when a downstream processor needs more
    # values. It can use the send method, to send values downstream.
    def produce(self, env, send): 
        # Send the next value.
        send(env[0])
        # Set next value to be send.
        env[0] += 1

        if sys.version_info[0] != 3 and env[0] == sys.maxint:
            # No we definetly can't produce any more integers, so we need to
            # signal that we want to stop:
            return Stop

        # We can go one, but we do not need to.
        return MayResume
        
        # We could also return Resume to signal that we need to go on.
        
    # For some processors (e.g. those which use resources) it could be necessary
    # to implement a teardown method. It will be called, after the processor 
    # finished executing, getting the environment previously produce by setup.
    # def teardown(self, env):
    #   pass

# As our processor only needs to exist once, we could as well create an instance
# of it, that could be used anywhere.
all_ints_larger_than = AllIntsLargerThan()

sp = all_ints_larger_than >> to_list(max_amount = 100)
assert sp.run(10) == range(10, 110)
