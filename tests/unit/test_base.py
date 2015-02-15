import pytest

from streamr import *

class _TestProducer():
    def testIsInstanceOfProducer(self, producer):
        assert isinstance(producer, Producer) 

    def testTypeOut(self, producer):
        assert isinstance(producer.typeOut(), Type)


class _TestConsumer():
    def testIsInstanceOfConsumer(self, consumer):
        assert ininstance(consumer, Consumer)

    def testTypeIn(self, consumer):
        assert isinstance(consumer.typeIn(), Type)
    
