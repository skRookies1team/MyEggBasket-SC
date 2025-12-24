from kafka_client import create_consumer, create_producer
from subscription_manager import SubscriptionManager
from kis_ws_client import KISWebSocketClient
from config import *
import json

def main():
    consumer = create_consumer(
        topic=TOPIC_SUBSCRIBE,
        group_id=GROUP_ID,
        servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)

    sub_manager = SubscriptionManager()

    def on_tick(tick):
        producer.produce(
            TOPIC_PUBLISH,
            value=json.dumps(tick).encode("utf-8"),
        )
        producer.flush()
        print("tick:", tick)

    kis_ws = KISWebSocketClient(on_tick=on_tick)
    kis_ws.connect()

    print("StockCollector with KIS WS started")

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        event = json.loads(msg.value().decode("utf-8"))
        action = event["action"]
        symbol = event["symbol"]

        if action == "SUBSCRIBE":
            first = sub_manager.subscribe(symbol)
            if first:
                kis_ws.subscribe(symbol)

        elif action == "UNSUBSCRIBE":
            last = sub_manager.unsubscribe(symbol)
            if last:
                kis_ws.unsubscribe(symbol)

if __name__ == "__main__":
    main()
