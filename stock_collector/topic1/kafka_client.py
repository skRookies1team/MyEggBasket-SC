from confluent_kafka import Producer

def create_producer(servers: str) -> Producer:
    conf = {
        "bootstrap.servers": servers,
    }
    return Producer(conf)
