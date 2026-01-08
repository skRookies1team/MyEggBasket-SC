from confluent_kafka import Consumer, Producer, KafkaError
import json


class KafkaConsumerClient:
    def __init__(self, broker, topic, group_id="stock_collector_group"):
        """
        Kafka Consumer 초기화
        """
        conf = {
            'bootstrap.servers': broker,
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([topic])

    def poll(self, timeout=1.0):
        """
        메시지 폴링 (리스트 형태로 반환하여 loop 처리에 용이하게 함)
        """
        msg = self.consumer.poll(timeout)

        if msg is None:
            return []

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka Consumer Error: {msg.error()}")
            return []

        return [msg]

    def close(self):
        self.consumer.close()


# (선택) Producer 필요 시 사용
class KafkaProducerClient:
    def __init__(self, broker):
        self.producer = Producer({'bootstrap.servers': broker})

    def send(self, topic, value):
        if isinstance(value, dict):
            value = json.dumps(value).encode('utf-8')
        self.producer.produce(topic, value=value)
        self.producer.flush()