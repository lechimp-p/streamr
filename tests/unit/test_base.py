import pytest

from streamr import *

class _TestProducer():
    def test_isInstanceOfProducer(self, producer):
        assert isinstance(producer, Producer) 

    def test_typeOut(self, producer):
        assert isinstance(producer.typeOut(), Type)

    def test_typeOfProducedValues(self, producer, max_amount):
        env = producer.get_initial_env()
        t = producer.typeOut()
        count = 0
        for var in producer.produce(env):
            assert t.containsValue(var) 

            count += 1
            if count > max_amount:
                return 
        


class _TestConsumer():
    def test_isInstanceOfConsumer(self, consumer):
        assert ininstance(consumer, Consumer)

    def test_typeIn(self, consumer):
        assert isinstance(consumer.typeIn(), Type)

    def test_consumesValuesOfType(self, consumer, test_values):
        env = consumer.get_initial_env()
        t = consumer.typeIn()
        gen = (i for i in test_values)
        def await():
            v = gen.__next__()
            assert t.contains(v)
            return v

        consumer.consume(env, await)
