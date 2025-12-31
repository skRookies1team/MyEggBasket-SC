import json
import time
import threading
import websocket
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class KISWebSocketClient:
    def __init__(self, on_message):
        self.app_key = os.getenv("KIS_APP_KEY")
        self.app_secret = os.getenv("KIS_APP_SECRET")
        self.ws_url = os.getenv("KIS_WS_URL")

        if not self.app_key or not self.app_secret:
            raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET missing")

        self.on_message = on_message
        self.approval_key = None
        self.ws = None
        self.connected = False
        self.subscribed = set()

        # 실전/모의투자에 따른 REST URL 설정
        if "tryitout" in self.ws_url:
            self.rest_base_url = "https://openapivts.koreainvestment.com:29443"
        else:
            self.rest_base_url = "https://openapi.koreainvestment.com:9443"

        # 처리할 TR ID
        self.TR_TICK = "H0STCNT0"
        self.TR_ORDERBOOK = "H0STASP0"

    # -------------------------------
    # Approval Key
    # -------------------------------
    def issue_approval_key(self):
        url = f"{self.rest_base_url}/oauth2/Approval"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }

        # SSL 에러 방지 (verify=False)
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        try:
            res = requests.post(url, json=payload, verify=False)
            res.raise_for_status()
            self.approval_key = res.json()["approval_key"]
            print("🔑 Approval Key issued")
        except Exception as e:
            print(f"❌ Approval Key 발급 실패: {e}")
            raise

    # -------------------------------
    # WebSocket
    # -------------------------------
    def connect(self):
        if not self.approval_key:
            self.issue_approval_key()

        def _run():
            # websocket-client 라이브러리 사용
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_close=self._on_close,
                on_error=self._on_error,
            )
            self.ws.run_forever()

        threading.Thread(target=_run, daemon=True).start()

    def _on_open(self, ws):
        self.connected = True
        print("🔌 KIS WebSocket connected")

        for symbol in self.subscribed:
            self._send_subscribe(symbol, self.TR_TICK)
            self._send_subscribe(symbol, self.TR_ORDERBOOK)

    def _on_close(self, ws, *args):
        self.connected = False
        print("⚠️ WS closed, reconnecting...")
        time.sleep(2)
        self.connect()

    def _on_error(self, ws, error):
        print("WS error:", error)

    # -------------------------------
    # Message Parsing (여기가 핵심!)
    # -------------------------------
    def _on_message(self, ws, message):
        # 1. 핑퐁 메시지나 이상한 데이터 무시
        if message.startswith("{") or "|" not in message:
            return

        parts = message.split("|")
        # TR ID 확인 (H0STCNT0 등)
        if len(parts) < 4:
            return

        tr_id = parts[1]
        fields = parts[3].split("^")
        symbol = fields[0]

        if symbol not in self.subscribed:
            return

        try:
            # =========================
            # 1️⃣ 실시간 체결가
            # =========================
            if tr_id == self.TR_TICK:
                tick = {
                    "type": "STOCK_TICK",
                    "stckShrnIscd": fields[0], # 종목 코드
                    "stckCntgHour": fields[1], # 체결 시간
                    "stckPrpr": int(fields[2]), # 현재가
                    "prdyVrss": int(fields[4]), # 전일 대비
                    "prdyCtrt": float(fields[5]), # 전일 대비율
                    "acmlVol": int(fields[15]), # 누적 거래량
                    "acmlTrPbmn": int(fields[16]), # 누적 거래 대금
                }
                self.on_message(tick)

            # =========================
            # 2️⃣ 실시간 호가
            # =========================
            elif tr_id == self.TR_ORDERBOOK:
                asks = []
                bids = []

                # 매도호가 1~10 / 잔량
                for i in range(10):
                    asks.append({
                        "price": int(float(fields[3 + i])),
                        "qty": int(float(fields[23 + i])),
                    })

                # 매수호가 1~10 / 잔량
                for i in range(10):
                    bids.append({
                        "price": int(float(fields[13 + i])),
                        "qty": int(float(fields[33 + i])),
                    })

                order_book = {
                    "type": "ORDER_BOOK",
                    "code": symbol,
                    "time": fields[1],
                    "asks": asks,
                    "bids": bids,
                    "totalAskQty": int(float(fields[43])),
                    "totalBidQty": int(float(fields[44])),
                }

                self.on_message(order_book)

        except Exception as e:
            print("❌ 실시간 데이터 파싱 오류:", e)
            print("원본 message:", message)

    # -------------------------------
    # Subscribe
    # -------------------------------
    def subscribe(self, symbol: str):
        if symbol in self.subscribed:
            return

        self.subscribed.add(symbol)

        if self.connected:
            self._send_subscribe(symbol, self.TR_TICK)
            self._send_subscribe(symbol, self.TR_ORDERBOOK)

    def _send_subscribe(self, symbol: str, tr_id: str):
        payload = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": symbol,
                }
            },
        }
        self.ws.send(json.dumps(payload))
