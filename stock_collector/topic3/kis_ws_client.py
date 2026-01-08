import json
import time
import threading
import websocket
import requests
import asyncio
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

if not os.getenv("KIS_APP_KEY"):
    load_dotenv()


class KisWSClient:
    def __init__(self, app_key, app_secret, approval_key=None, mode="REAL"):
        self.app_key = app_key or os.getenv("KIS_APP_KEY")
        self.app_secret = app_secret or os.getenv("KIS_APP_SECRET")
        self.approval_key = approval_key
        self.mode = mode

        if self.mode == "REAL":
            self.rest_base_url = "https://openapi.koreainvestment.com:9443"
            self.ws_url = "ws://ops.koreainvestment.com:21000"
        else:
            self.rest_base_url = "https://openapivts.koreainvestment.com:29443"
            self.ws_url = "ws://ops.koreainvestment.com:21000/tryitout/websocket"

        self.ws = None
        self.connected = False
        self.subscribed: set[str] = set()
        self.on_tick = None

        # [핵심 수정] 메인 스레드의 이벤트 루프를 저장해둠
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = None

    def issue_approval_key(self):
        url = f"{self.rest_base_url}/oauth2/Approval"
        headers = {"content-type": "application/json"}
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }
        try:
            res = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
            if res.status_code == 200:
                self.approval_key = res.json()["approval_key"]
                logger.info(f"🔑 Approval Key 발급 완료 ({self.mode})")
                return True
            else:
                logger.error(f"Approval Key 발급 실패: {res.text}")
                return False
        except Exception as e:
            logger.error(f"Approval Key Error: {e}")
            return False

    async def connect(self):
        # 루프가 변경되었을 수 있으므로 갱신
        self.loop = asyncio.get_running_loop()

        while not self.approval_key:
            if self.issue_approval_key():
                break
            logger.warning("키 발급 실패.. 3초 후 재시도")
            await asyncio.sleep(3)

        self._start_ws_thread()

        for _ in range(100):
            if self.connected:
                logger.info("✅ WebSocket Connected!")
                return
            await asyncio.sleep(0.1)
        logger.error("⚠️ Connection timeout")

    def _start_ws_thread(self):
        def _run():
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self.ws.run_forever(ping_interval=30, ping_timeout=10)

        t = threading.Thread(target=_run, daemon=True)
        t.start()

    async def subscribe_list(self, symbol_list):
        for symbol in symbol_list:
            await self.subscribe(symbol)

    async def close(self):
        if self.ws:
            self.ws.close()
        self.connected = False

    # -----------------------------------------------------------
    # WebSocket Callbacks (별도 스레드에서 실행됨)
    # -----------------------------------------------------------
    def _on_open(self, ws):
        self.connected = True
        logger.info(f"🔌 KIS WebSocket connected ({self.mode})")
        if self.subscribed:
            logger.info(f"Resubscribing {len(self.subscribed)} symbols...")
            for symbol in self.subscribed:
                self._send_subscribe(symbol)
                time.sleep(0.05)

    def _on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        logger.warning(f"⚠️ KIS WebSocket closed ({self.mode})")

        # [핵심 수정] 메인 루프에게 "재연결 함수 좀 실행해줘"라고 안전하게 부탁함
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._attempt_reconnect(), self.loop)

    def _on_error(self, ws, error):
        logger.error(f"KIS WebSocket error: {error}")

    def _on_message(self, ws, message):
        try:
            if message.startswith("{"): return

            parts = message.split("|")
            if len(parts) < 4: return

            tr_id = parts[1]
            data_part = parts[3]
            f = data_part.split("^")

            data = None
            if tr_id == "H0STCNT0":
                data = {
                    "type": "STOCK_TICK",
                    "stockCode": f[0],
                    "time": f[1],
                    "currentPrice": self.to_int(f[2]),
                    "changeRate": self.to_float(f[5]),
                    "volume": self.to_int(f[9])
                }
            elif tr_id == "H0STASP0":
                data = {
                    "type": "ORDER_BOOK",
                    "stockCode": f[0],
                    "time": f[1],
                    "totalAskQty": self.to_int(f[13]),
                    "totalBidQty": self.to_int(f[14]),
                    "asks": [{"price": self.to_int(f[i]), "qty": self.to_int(f[i + 20])} for i in range(3, 13)],
                    "bids": [{"price": self.to_int(f[i]), "qty": self.to_int(f[i + 20])} for i in range(13, 23)]
                }

            if data and self.on_tick:
                self.on_tick(data)

        except Exception as e:
            logger.error(f"Message parse error: {e}")

    # -----------------------------------------------------------
    # Methods
    # -----------------------------------------------------------
    async def subscribe(self, symbol):
        if not symbol: return
        is_new = symbol not in self.subscribed
        self.subscribed.add(symbol)

        if self.connected:
            self._send_subscribe(symbol)
            if is_new:
                logger.info(f"📡 Subscribed: {symbol}")
            await asyncio.sleep(0.05)

    def _send_subscribe(self, symbol):
        if not self.approval_key: return
        self._send_json("H0STCNT0", symbol)
        self._send_json("H0STASP0", symbol)

    def _send_json(self, tr_id, symbol):
        payload = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {"input": {"tr_id": tr_id, "tr_key": symbol}}
        }
        try:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self.ws.send(json.dumps(payload))
        except Exception as e:
            logger.error(f"Send Error: {e}")

    async def _attempt_reconnect(self):
        logger.info("🔄 Reconnecting in 3s...")
        await asyncio.sleep(3)
        if not self.connected:
            self._start_ws_thread()

    @staticmethod
    def to_int(v):
        try:
            return int(float(v))
        except:
            return 0

    @staticmethod
    def to_float(v):
        try:
            return float(v)
        except:
            return 0.0