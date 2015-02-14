import pytest

from streamr import *

class _TestProducer():
    """
    Checks whether the implementation of the producer fits the definition.
    """

    producer = None # Expected to contain a Producer after setup.

    def testIsInstanceOfProducer(self):
        assert isinstance(self.producer, Producer) 

    
    
