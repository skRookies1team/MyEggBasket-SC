import json
import time
import os
from dotenv import load_dotenv

from kafka_client import create_producer
from kis_ws_client import KISWebSocketClient
from symbols import SYMBOLS

ENV_PATH = os.path.join(".env")
load_dotenv(ENV_PATH)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_PUBLISH = os.getenv("TOPIC_PUBLISH", "stock-ticks")

if not KAFKA_BOOTSTRAP_SERVERS:
    raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS not set")

def main():
    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)

    def on_tick(tick: dict):
        producer.produce(
            TOPIC_PUBLISH,
            value=json.dumps(tick, ensure_ascii=False).encode("utf-8"),
        )
        producer.poll(0)

    kis_ws = KISWebSocketClient(on_tick)
    kis_ws.connect()

    time.sleep(1)

    for symbol in SYMBOLS:
        kis_ws.subscribe(symbol)
        time.sleep(0.05)

    print(f"✅ StockCollector started ({len(SYMBOLS)} symbols)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 stopping...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
