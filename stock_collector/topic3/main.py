import os
import asyncio
import json
import requests
from datetime import datetime
from kis_ws_client import KisWSClient
from kafka_client import KafkaConsumerClient, KafkaProducerClient
from subscription_manager import SubscriptionManager

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

# 환경변수 로드
KAFKA_BROKER = os.getenv("KAFKA_BROKER", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
# Spring Boot API 주소 (환경에 맞게 수정 필요)
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8081")

# [계정 1] 관리자용 (고정 50개 담당)
APP_KEY_1 = os.getenv("APP_KEY") or os.getenv("KIS_APP_KEY")
APP_SECRET_1 = os.getenv("APP_SECRET") or os.getenv("KIS_APP_SECRET")

# [계정 2] 사용자용 (나머지 동적 구독 담당)
APP_KEY_2 = os.getenv("APP_KEY_2") or os.getenv("KIS_APP_KEY_2") or os.getenv("KIS_APP_KEY2")
APP_SECRET_2 = os.getenv("APP_SECRET_2") or os.getenv("KIS_APP_SECRET_2") or os.getenv("KIS_APP_SECRET2")

if not APP_KEY_1 or not APP_KEY_2:
    print("🚨 경고: 두 개의 계정 키가 모두 필요합니다. .env를 확인해주세요.")
    print(f"   - 계정1: {'OK' if APP_KEY_1 else 'MISSING'}")
    print(f"   - 계정2: {'OK' if APP_KEY_2 else 'MISSING'}")


# -----------------------------------------------------------------------------
# 데이터 처리 핸들러 (공통)
# -----------------------------------------------------------------------------
def handle_tick(data, producer):
    try:
        now = datetime.now()
        # 시간 파싱 (HHmmss -> datetime)
        time_str = data.get('time', now.strftime('%H%M%S'))
        if len(time_str) == 6 and time_str.isdigit():
             dt = now.replace(hour=int(time_str[:2]), minute=int(time_str[2:4]), second=int(time_str[4:6]))
        else:
             dt = now
        timestamp = dt.isoformat()

        payload = None

        # [1] 체결가 데이터 (STOCK_TICK)
        if data['type'] == 'STOCK_TICK':
            payload = {
                "type": "STOCK_TICK",
                "stockCode": data['stockCode'],
                "currentPrice": data['currentPrice'],
                "timestamp": timestamp,
                "changeRate": data['changeRate'],
                "volume": data['volume']
            }
            # 로그 출력 (선택)
            # print(f"⚡️ [Tick] {payload['stockCode']} : {payload['currentPrice']}원")

        # [2] 호가 데이터 (ORDER_BOOK) -> Kafka로 전송
        elif data['type'] == 'ORDER_BOOK':
            payload = {
                "type": "ORDER_BOOK",
                "stockCode": data['stockCode'],
                "timestamp": timestamp,
                "asks": data['asks'],         # 매도 호가 리스트
                "bids": data['bids'],         # 매수 호가 리스트
                "totalAskQty": data['totalAskQty'],
                "totalBidQty": data['totalBidQty']
            }
            # 호가 데이터는 빈도가 높으므로 로그는 생략하거나 필요시 주석 해제
            # print(f"📊 [OrderBook] {payload['stockCode']}")

        # [3] Kafka 전송 (stock-ticks 토픽 공유)
        if payload:
            producer.send("stock-ticks", payload)

    except Exception as e:
        print(f"Error processing data: {e}")


# [추가] API에서 초기 구독 목록 가져오기
def fetch_active_stocks_from_api():
    url = f"{API_BASE_URL}/api/app/subscriptions/active-codes"
    try:
        print(f"📡 Fetching active subscriptions from {url}...")
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            stock_list = response.json()
            print(f"✅ Loaded {len(stock_list)} active stocks from API.")
            return stock_list
        else:
            print(f"⚠️ Failed to load stocks. Status: {response.status_code}, Body: {response.text}")
            return []
    except Exception as e:
        print(f"⚠️ API Connection failed: {e}")
        return []


# -----------------------------------------------------------------------------
# Main Loop
# -----------------------------------------------------------------------------
async def main():
    sub_manager = SubscriptionManager()
    kafka_producer = KafkaProducerClient(broker=KAFKA_BROKER)

    # 공통 콜백
    on_tick_callback = lambda t: handle_tick(t, kafka_producer)

    # 1. [계정 A] 관리자용 클라이언트 생성
    print("🔵 Initialize Admin Client (Account 1)...")
    client_admin = KisWSClient(
        app_key=APP_KEY_1,
        app_secret=APP_SECRET_1,
        mode="VIRTUAL"
    )
    client_admin.on_tick = on_tick_callback

    # 2. [계정 B] 사용자용 클라이언트 생성
    print("🟢 Initialize User Client (Account 2)...")
    client_user = KisWSClient(
        app_key=APP_KEY_2,
        app_secret=APP_SECRET_2,
        mode="VIRTUAL"
    )
    client_user.on_tick = on_tick_callback

    # Kafka 소비자 (구독 명령 수신용)
    kafka_consumer = KafkaConsumerClient(
        broker=KAFKA_BROKER,
        topic="subscription-events",
        group_id="sc-group-dual-v1"
    )

    # 두 클라이언트 모두 연결
    await client_admin.connect()
    await client_user.connect()

    # 3. [계정 A] 고정 종목 50개 구독
    fixed_list = sub_manager.get_fixed_list()
    fixed_set = set(fixed_list)
    if fixed_list:
        print(f"🔒 [Admin] Subscribing fixed list ({len(fixed_list)} stocks)...")
        await client_admin.subscribe_list(fixed_list)

    # 4. [계정 B] API에서 가져온 활성 종목 구독 (초기화)
    active_stocks = fetch_active_stocks_from_api()
    if active_stocks:
        # 매니저에 등록하고 구독할 리스트 받기 (고정 종목 제외됨)
        init_list = sub_manager.init_from_api(active_stocks)
        if init_list:
            print(f"🔓 [User] Subscribing initial list ({len(init_list)} stocks)...")
            await client_user.subscribe_list(init_list)
    else:
        print("🔓 [User] No active subscriptions found or API failed.")

    print("✅ Stock Collector Started (Dual Client Mode)")

    try:
        while True:
            messages = kafka_consumer.poll(timeout=0.1)

            for msg in messages:
                try:
                    val = msg.value().decode('utf-8')
                    data = json.loads(val)

                    stock_code = data.get('stockCode')
                    sub_type = data.get('subType', 'VIEW')

                    if not stock_code: continue

                    # [핵심 로직] 고정 종목에 포함된건지 확인
                    if stock_code in fixed_set:
                        continue

                    # 고정 리스트에 없다면 -> [계정 B] 사용자 클라이언트로 구독
                    needs_update = False
                    if sub_type == 'VIEW':
                        if sub_manager.add_viewing_stock(stock_code):
                            print(f"🆕 [User] New VIEW request: {stock_code}")
                            needs_update = True
                    elif sub_type == 'INTEREST':
                        sub_manager.interest_stocks.add(stock_code)
                        print(f"⭐️ [User] New INTEREST request: {stock_code}")
                        needs_update = sub_manager._refresh_user_account_list()

                    if needs_update:
                        await client_user.subscribe(stock_code)  # Now awaitable

                except Exception as e:
                    print(f"Message Error: {e}")

            await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        print("Shutting down...")
        await client_admin.close()
        await client_user.close()
        kafka_consumer.close()


if __name__ == "__main__":
    asyncio.run(main())