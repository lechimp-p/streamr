# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

class Producer(object):
    """
    A producer is the source for a stream, that is, it produces new data for
    the downstream.
    """
    pass

class Consumer(object):
    """
    A consumer is the sink for a stream, that is it consumes data from upstream
    without producing new values.
    """
    pass

class Pipe(Producer, Consumer):
    """
    A pipe is an element between a source and a sink, that is it consumes data
    from upstream an produces new data for downstream.
    """ 
    pass

class StreamProcess(object):
    """
    A stream process is a completely defined process between sources and sinks
    with no dangling ends. It could be run.
    """
    pass 

# Objects from the classes need to respect the follwing rules, where abbreaviations
# for the names are used
#
# Pr >> Pr = error
# Co >> Co = error
# Pi >> Pi = Pi
# SP >> any = error
# any >> SP = error
# Pr >> Pi = Pr
# Pi >> Pr = error
# Co >> Pi = error
# Pi >> Co = Co
# Pr >> Co = SP
# Co >> Pr = error

