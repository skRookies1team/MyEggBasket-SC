from confluent_kafka import Consumer, Producer
import json

def create_consumer(topic, group_id, servers):
    conf = {
        "bootstrap.servers": servers,
        "group.id": group_id,
        "auto.offset.reset": "latest",
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer


def create_producer(servers):
    conf = {
        "bootstrap.servers": servers,
    }
    return Producer(conf)
