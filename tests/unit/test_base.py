import pytest

from streamr import *

class _TestProducer():
    def testIsInstanceOfProducer(self, producer):
        assert isinstance(producer, Producer) 

    def testTypeOut(self, producer):
        assert isinstance(producer.typeOut(), Type)

    def testTypeOfProducedValues(self, producer, max_amount):
        env = producer.get_initial_env()
        t = producer.typeOut()
        count = 0
        for var in producer.produce(env):
            assert t.containsValue(var) 

            count += 1
            if count > max_amount:
                return 
        


class _TestConsumer():
    def testIsInstanceOfConsumer(self, consumer):
        assert ininstance(consumer, Consumer)

    def testTypeIn(self, consumer):
        assert isinstance(consumer.typeIn(), Type)
    
