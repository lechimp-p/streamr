# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

class StreamPart(object):
    """
    Common base class for the parts of a stream processing pipeline.
    """

class Producer(StreamPart):
    """
    A producer is the source for a stream, that is, it produces new data for
    the downstream.
    """
    def type_out(self):
        """
        Get the type of output this producer produces.
        """
        raise NotImplementedError("Producer::type_out: implement me!")

class Consumer(StreamPart):
    """
    A consumer is the sink for a stream, that is it consumes data from upstream
    without producing new values.
    """
    def type_in(self):
        """
        Get the type, this consumer consumes.
        """
        raise NotImplementedError("Consumer::type_in: implement me!")

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
    def run(self):
        """
        Let this stream process run.
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

